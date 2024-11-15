package index

import (
	"bufio"
	"encoding/binary"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
	"soq/feature"
	ownIo "soq/io"
	"time"
)

type RawFeaturesRepository struct {
	baseGridIndex
}

func NewRawFeaturesRepository(tagIndex *TagIndex, cellWidth float64, cellHeight float64, baseFolder string) *RawFeaturesRepository {
	gridIndexWriter := &RawFeaturesRepository{
		baseGridIndex: baseGridIndex{
			TagIndex:   tagIndex,
			CellWidth:  cellWidth,
			CellHeight: cellHeight,
			BaseFolder: baseFolder,
		},
	}
	return gridIndexWriter
}

// WriteOsmToRawEncodedFeatures Reads the input PBF file and converts all OSM objects into temporary raw encoded
// features and writes them to a single cell file. The returned cell map contains all cells that contain nodes.
func (r *RawFeaturesRepository) WriteOsmToRawEncodedFeatures(scannerFactory scannerFactoryFunc) (map[CellIndex]int, *CellExtent, error) {
	sigolo.Info("Start converting OSM data to raw encoded features")
	importStartTime := time.Now()

	err := os.RemoveAll(r.BaseFolder)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Error deleting directory for temporary raw features %s", r.BaseFolder)
	}

	scanner, err := scannerFactory()
	if err != nil {
		return nil, nil, err
	}
	defer scanner.Close()

	firstRelationHasBeenProcessed := false
	firstWayHasBeenProcessed := false

	tempEncodedValues := r.TagIndex.newTempEncodedValueArray()

	nodeWriter, err := r.getFileWriter(feature.OsmObjNode.String())
	if err != nil {
		return nil, nil, err
	}

	wayWriter, err := r.getFileWriter(feature.OsmObjWay.String())
	if err != nil {
		return nil, nil, err
	}

	relationWriter, err := r.getFileWriter(feature.OsmObjRelation.String())
	if err != nil {
		return nil, nil, err
	}

	cellToNodeCount := map[CellIndex]int{}
	var inputDataCellExtent *CellExtent

	sigolo.Debug("Process nodes (1/3)")
	for scanner.Scan() {
		obj := scanner.Object()

		switch osmObj := obj.(type) {
		case *osm.Node:
			cell := r.GetCellIndexForCoordinate(osmObj.Lon, osmObj.Lat)
			if _, ok := cellToNodeCount[cell]; !ok {
				cellToNodeCount[cell] = 1
			} else {
				cellToNodeCount[cell] = cellToNodeCount[cell] + 1
			}

			if inputDataCellExtent == nil {
				inputDataCellExtent = &CellExtent{cell, cell}
			} else {
				newExtent := inputDataCellExtent.Expand(cell)
				inputDataCellExtent = &newExtent
			}

			encodedKeys, encodedValues := r.TagIndex.encodeTags(osmObj.Tags, tempEncodedValues)
			point := osmObj.Point()
			err = r.writeNodeData(osmObj.ID, encodedKeys, encodedValues, &point, nodeWriter)
			sigolo.FatalCheck(err)
		case *osm.Way:
			if !firstWayHasBeenProcessed {
				sigolo.Debug("Start processing ways (2/3)")
				firstWayHasBeenProcessed = true
			}

			encodedKeys, encodedValues := r.TagIndex.encodeTags(osmObj.Tags, tempEncodedValues)
			err = r.writeWayData(osmObj.ID, encodedKeys, encodedValues, osmObj.Nodes, wayWriter)
			sigolo.FatalCheck(err)
		case *osm.Relation:
			if !firstRelationHasBeenProcessed {
				sigolo.Debug("Start processing relations (3/3)")
				firstRelationHasBeenProcessed = true
			}

			var nodeIds []osm.NodeID
			var wayIds []osm.WayID
			var childRelationIds []osm.RelationID

			for _, member := range osmObj.Members {
				switch member.Type {
				case osm.TypeNode:
					nodeId := osm.NodeID(member.Ref)
					nodeIds = append(nodeIds, nodeId)
				case osm.TypeWay:
					wayId := osm.WayID(member.Ref)
					wayIds = append(wayIds, wayId)
				case osm.TypeRelation:
					relId := osm.RelationID(member.Ref)
					childRelationIds = append(childRelationIds, relId)
				}
			}

			encodedKeys, encodedValues := r.TagIndex.encodeTags(osmObj.Tags, tempEncodedValues)
			err = r.writeRelationData(osmObj.ID, encodedKeys, encodedValues, nodeIds, wayIds, childRelationIds, relationWriter)
			sigolo.FatalCheck(err)
		}
	}

	importDuration := time.Since(importStartTime)
	sigolo.Infof("Created raw encoded features from OSM data in %s", importDuration)

	err = nodeWriter.Flush()
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error closing node writer")
	}

	err = wayWriter.Flush()
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error closing way writer")
	}

	err = relationWriter.Flush()
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error closing relation writer")
	}

	return cellToNodeCount, inputDataCellExtent, nil
}

func (r *RawFeaturesRepository) getFileWriter(objectType string) (*bufio.Writer, error) {
	// Not filepath.Join because in this case it's slower than simple concatenation
	cellFolderName := r.BaseFolder
	cellFileName := r.BaseFolder + "/" + objectType + ".rawcell"

	var writer *bufio.Writer
	var err error

	// Cell file not cached
	var file *os.File

	if _, err = os.Stat(cellFileName); err == nil {
		// Cell file does exist -> open it
		sigolo.Tracef("Cell file %s already exist but is not cached, I'll open it", cellFileName)
		file, err = os.OpenFile(cellFileName, os.O_RDWR, 0666)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to open cell file %s", cellFileName)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		// Cell file does NOT exist -> create its folder (if needed) and the file itself

		// Ensure the folder exists
		if _, err = os.Stat(cellFolderName); os.IsNotExist(err) {
			sigolo.Tracef("Cell folder %s doesn't exist, I'll create it", cellFolderName)
			err = os.MkdirAll(cellFolderName, os.ModePerm)
			if err != nil {
				return nil, errors.Wrapf(err, "Unable to create cell folder %s", cellFolderName)
			}
		}

		// Create cell file
		sigolo.Tracef("Cell file %s does not exist, I'll create it", cellFileName)
		file, err = os.Create(cellFileName)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to create new cell file %s", cellFileName)
		}
	} else {
		return nil, errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	writer = bufio.NewWriter(file)

	return writer, nil
}

func (r *RawFeaturesRepository) writeNodeData(id osm.NodeID, keys []byte, values []int, point *orb.Point, f io.Writer) error {
	/*
		Entry format:
		// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes).

		Names: | osmId | lon | lat | num. keys | num. values |   encodedKeys   |   encodedValues   |
		Bytes: |   8   |  4  |  4  |     4     |      4      | <num. keys> / 8 | <num. values> * 3 |

		The encodedKeys is a bit-string (each key 1 bit), that why the division by 8 happens. The stored value is the
		number of bytes in the keys array of the feature (i.e. "len(encodedFeature.GetKeys())"). The encodedValue part, however,
		is an int-array, therefore, we need the multiplication with 4.
	*/

	// The number of key-bins to store is determined by the bin with the highest index that is not empty (i.e. all 0s).
	// If only the first bin contains some 1s (i.e. keys that are set on the feature) and the next 100 bins are empty,
	// then there's no reason to store those empty bins. This reduced the cell-file size for hamburg-latest (45 MB PBF)
	// by a factor of ten!
	numKeys := 0
	for i := 0; i < len(keys); i++ {
		if keys[i] != 0 {
			numKeys = i + 1
		}
	}

	encodedKeyBytes := numKeys           // Is already a byte-array -> no division by 8 needed
	encodedValueBytes := len(values) * 3 // Int array and int = 4 bytes

	headerBytesCount := 8 + 4 + 4 + 4 + 4 // = 24
	byteCount := headerBytesCount
	byteCount += encodedKeyBytes
	byteCount += encodedValueBytes

	data := make([]byte, byteCount)

	binary.LittleEndian.PutUint64(data[0:], uint64(id))
	binary.LittleEndian.PutUint32(data[8:], math.Float32bits(float32(point.Lon())))
	binary.LittleEndian.PutUint32(data[12:], math.Float32bits(float32(point.Lat())))
	binary.LittleEndian.PutUint32(data[16:], uint32(numKeys))
	binary.LittleEndian.PutUint32(data[20:], uint32(len(values)))

	pos := headerBytesCount

	/*
		Write keys
	*/
	copy(data[pos:], keys[0:numKeys])
	pos += numKeys

	/*
		Write values
	*/
	for _, v := range values {
		data[pos] = byte(v)
		data[pos+1] = byte(v >> 8)
		data[pos+2] = byte(v >> 16)
		pos += 3
	}

	_, err := f.Write(data)
	return err
}

func (r *RawFeaturesRepository) writeWayData(id osm.WayID, keys []byte, values []int, nodes osm.WayNodes, f io.Writer) error {
	/*
		Entry format:
		// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes).

		Names: | osmId | num. keys | num. values | num. nodes |   encodedKeys   |   encodedValues   |       nodes       |
		Bytes: |   8   |     4     |      4      |      2     | <num. keys> / 8 | <num. values> * 3 | <num. nodes> * 16 |

		The encodedKeys is a bit-string (each key 1 bit), that why the division by 8 happens. The stored value is the
		number of bytes in the keys array of the feature (i.e. "len(encodedFeature.GetKeys())"). The encodedValue part, however,
		is an int-array, therefore, we need the multiplication with 4.

		The nodes section contains all nodes, not only the ones within this cell. This enables geometric checks, even
		in cases where no way-node is within this cell. The nodes are stores in the following way:
		<id (64-bit)><lon (32-bit)><lat (23-bit)>
	*/
	// The number of key-bins to store is determined by the bin with the highest index that is not empty (i.e. all 0s).
	// If only the first bin contains some 1s (i.e. keys that are set on the feature) and the next 100 bins are empty,
	// then there's no reason to store those empty bins. This reduced the cell-file size for hamburg-latest (45 MB PBF)
	// by a factor of ten!
	numEncodedKeyBytes := 0
	for i := 0; i < len(keys); i++ {
		if keys[i] != 0 {
			numEncodedKeyBytes = i + 1
		}
	}

	numEncodedValueBytes := len(values) * 3 // Int array and int = 4 bytes
	nodeIdBytes := len(nodes) * 16          // Each ID is a 64-bit int + 2*4 bytes for lat/lon

	headerByteCount := 8 + 4 + 4 + 2
	byteCount := headerByteCount
	byteCount += numEncodedKeyBytes
	byteCount += numEncodedValueBytes
	byteCount += nodeIdBytes

	data := make([]byte, byteCount)

	/*
		Write header
	*/
	binary.LittleEndian.PutUint64(data[0:], uint64(id))
	binary.LittleEndian.PutUint32(data[8:], uint32(numEncodedKeyBytes))
	binary.LittleEndian.PutUint32(data[12:], uint32(len(values)))
	binary.LittleEndian.PutUint16(data[16:], uint16(len(nodes)))

	pos := headerByteCount

	/*
		Write keys
	*/
	copy(data[pos:], keys[0:numEncodedKeyBytes])
	pos += numEncodedKeyBytes

	/*
		Write value
	*/
	for _, v := range values {
		data[pos] = byte(v)
		data[pos+1] = byte(v >> 8)
		data[pos+2] = byte(v >> 16)
		pos += 3
	}

	/*
		Write nodes
	*/
	for _, node := range nodes {
		binary.LittleEndian.PutUint64(data[pos:], uint64(node.ID))
		binary.LittleEndian.PutUint32(data[pos+8:], math.Float32bits(float32(node.Lon)))
		binary.LittleEndian.PutUint32(data[pos+12:], math.Float32bits(float32(node.Lat)))
		pos += 16
	}

	_, err := f.Write(data)
	return err
}

func (r *RawFeaturesRepository) writeRelationData(id osm.RelationID, keys []byte, values []int, nodeIds []osm.NodeID, wayIds []osm.WayID, childRelationIds []osm.RelationID, f io.Writer) error {
	/*
		Entry format:
		// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes).

		Names: | osmId | num. keys | num. values | num. nodes | num. ways | num. child rels |   encodedKeys   |   encodedValues   |     node IDs     |     way IDs     |    child rel. IDs     |
		Bytes: |   8   |     4     |      4      |      2     |     2     |        2        | <num. keys> / 8 | <num. values> * 3 | <num. nodes> * 8 | <num. ways> * 8 | <num. child rels> * 8 |

		The encodedKeys is a bit-string (each key 1 bit), that why the division by 8 happens. The stored value is the
		number of bytes in the keys array of the feature (i.e. "len(encodedFeature.GetKeys())"). The encodedValue part, however,
		is an int-array, therefore, we need the multiplication with 4.

		The "bbox" field are 4 32-bit floats for the min-lon, min-lat, max-lon and max-lat values.

		// TODO store real geometry. Including geometry of sub-relations?
	*/
	numKeys := 0
	for i := 0; i < len(keys); i++ {
		if keys[i] != 0 {
			numKeys = i + 1
		}
	}

	encodedKeyBytes := numKeys                        // Is already a byte-array -> no division by 8 needed
	encodedValueBytes := len(values) * 3              // Int array and int = 4 bytes
	nodeIdBytes := len(nodeIds) * 8                   // IDs are all 64-bit integers
	wayIdBytes := len(wayIds) * 8                     // IDs are all 64-bit integers
	childRelationIdBytes := len(childRelationIds) * 8 // IDs are all 64-bit integers

	headerBytesCount := 8 + 4 + 4 + 2 + 2 + 2 // = 22
	byteCount := headerBytesCount
	byteCount += encodedKeyBytes
	byteCount += encodedValueBytes
	byteCount += nodeIdBytes
	byteCount += wayIdBytes
	byteCount += childRelationIdBytes

	data := make([]byte, byteCount)

	binary.LittleEndian.PutUint64(data[0:], uint64(id))
	binary.LittleEndian.PutUint32(data[8:], uint32(numKeys))
	binary.LittleEndian.PutUint32(data[12:], uint32(len(values)))
	binary.LittleEndian.PutUint16(data[16:], uint16(len(nodeIds)))
	binary.LittleEndian.PutUint16(data[18:], uint16(len(wayIds)))
	binary.LittleEndian.PutUint16(data[20:], uint16(len(childRelationIds)))

	pos := headerBytesCount

	/*
		Write keys
	*/
	copy(data[pos:], keys[0:numKeys])
	pos += numKeys

	/*
		Write values
	*/
	for _, v := range values {
		data[pos] = byte(v)
		data[pos+1] = byte(v >> 8)
		data[pos+2] = byte(v >> 16)
		pos += 3
	}

	/*
		Write node-IDs
	*/
	for _, nodeId := range nodeIds {
		binary.LittleEndian.PutUint64(data[pos:], uint64(nodeId))
		pos += 8
	}

	/*
		Write way-IDs
	*/
	for _, wayId := range wayIds {
		binary.LittleEndian.PutUint64(data[pos:], uint64(wayId))
		pos += 8
	}

	/*
		Write child relation-IDs
	*/
	for _, relationId := range childRelationIds {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	_, err := f.Write(data)
	return err
}

func (r *RawFeaturesRepository) ReadFeatures(readFeatureChannel chan feature.EncodedFeature, extent CellExtent) error {
	// TODO Pass the extent to here, so that objects outside of it can be skipped during reading. Maybe create/use wrapper around file to access files by index?
	// TODO Do not read before processing, read on the fly

	cellFileName := r.BaseFolder + "/" + feature.OsmObjNode.String() + ".rawcell"
	cellFile, err := os.OpenFile(cellFileName, os.O_RDONLY, 0666)
	if err != nil {
		return errors.Wrapf(err, "Unable to open temp raw node-feature cell %s", cellFileName)
	}
	cellReader := ownIo.NewIndexReader(cellFile)
	r.readNodesFromCellData(readFeatureChannel, cellReader, extent)
	err = cellFile.Close()
	if err != nil {
		return errors.Wrapf(err, "Unable to close temp raw node-feature cell %s", cellFileName)
	}

	cellFileName = r.BaseFolder + "/" + feature.OsmObjWay.String() + ".rawcell"
	cellFile, err = os.OpenFile(cellFileName, os.O_RDONLY, 0666)
	if err != nil {
		return errors.Wrapf(err, "Unable to open temp raw node-feature cell %s", cellFileName)
	}
	cellReader = ownIo.NewIndexReader(cellFile)
	r.readWaysFromCellData(readFeatureChannel, cellReader, extent)
	err = cellFile.Close()
	if err != nil {
		return errors.Wrapf(err, "Unable to close temp raw node-feature cell %s", cellFileName)
	}

	cellFileName = r.BaseFolder + "/" + feature.OsmObjRelation.String() + ".rawcell"
	cellFile, err = os.OpenFile(cellFileName, os.O_RDONLY, 0666)
	if err != nil {
		return errors.Wrapf(err, "Unable to open temp raw node-feature cell %s", cellFileName)
	}
	cellReader = ownIo.NewIndexReader(cellFile)
	r.readRelationsFromCellData(readFeatureChannel, cellReader)
	err = cellFile.Close()
	if err != nil {
		return errors.Wrapf(err, "Unable to close temp raw node-feature cell %s", cellFileName)
	}

	close(readFeatureChannel)

	return nil
}

func (r *RawFeaturesRepository) readNodesFromCellData(output chan feature.EncodedFeature, reader *ownIo.IndexedReader, extent CellExtent) {
	for pos := int64(0); reader.Has(pos); {
		// See format details (bit position, field sizes, etc.) in function "writeNodeData".

		/*
			Read header fields
		*/
		osmId := reader.Uint64(pos + 0)
		lon := reader.Float32(pos + 8)
		lat := reader.Float32(pos + 12)
		numEncodedKeyBytes := reader.IntFromUint32(pos + 16)
		numValues := reader.IntFromUint32(pos + 20)

		headerBytesCount := 8 + 4 + 4 + 4 + 4 // = 24

		pos += int64(headerBytesCount)

		if !extent.containsLonLat(float64(lon), float64(lat), r.CellWidth, r.CellHeight) {
			pos += int64(numEncodedKeyBytes)
			pos += int64(numValues * 3)
			continue
		}

		/*
			Read keys
		*/
		encodedKeys := make([]byte, numEncodedKeyBytes)
		encodedValues := make([]int, numValues)
		copy(encodedKeys[:], reader.Read(pos, len(encodedKeys)))
		pos += int64(numEncodedKeyBytes)

		/*
			Read values
		*/
		for i := 0; i < numValues; i++ {
			encodedValues[i] = reader.IntFromUint24(pos)
			pos += 3
		}

		/*
			Create encoded feature from raw data
		*/
		encodedFeature := &feature.EncodedNodeFeature{
			AbstractEncodedFeature: feature.AbstractEncodedFeature{
				ID:       osmId,
				Geometry: &orb.Point{float64(lon), float64(lat)},
				Keys:     encodedKeys,
				Values:   encodedValues,
			},
		}

		output <- encodedFeature
	}
}

func (r *RawFeaturesRepository) readWaysFromCellData(output chan feature.EncodedFeature, reader *ownIo.IndexedReader, extent CellExtent) {
	for pos := int64(0); reader.Has(pos); {
		// See format details (bit position, field sizes, etc.) in function "writeWayData".

		/*
			Read header fields
		*/
		osmId := reader.Uint64(pos + 0)
		numEncodedKeyBytes := reader.IntFromUint32(pos + 8)
		numValues := reader.IntFromUint32(pos + 12)
		numNodes := reader.IntFromUint16(pos + 16)

		headerBytesCount := 8 + 4 + 4 + 2

		pos += int64(headerBytesCount)

		/*
			Read keys
		*/
		encodedKeys := make([]byte, numEncodedKeyBytes)
		encodedValues := make([]int, numValues)
		copy(encodedKeys[:], reader.Read(pos, len(encodedKeys)))
		pos += int64(numEncodedKeyBytes)

		/*
			Read values
		*/
		for i := 0; i < numValues; i++ {
			encodedValues[i] = reader.IntFromUint24(pos)
			pos += 3
		}

		/*
			Read node-IDs
		*/
		nodes := make([]osm.WayNode, numNodes)
		extentContainsWay := false
		for i := 0; i < numNodes; i++ {
			id := osm.NodeID(reader.Int64(pos))
			lon := float64(reader.Float32(pos + 8))
			lat := float64(reader.Float32(pos + 12))

			nodes[i] = osm.WayNode{
				ID:  id,
				Lon: lon,
				Lat: lat,
			}
			pos += 16

			extentContainsWay = extentContainsWay || extent.containsLonLat(lon, lat, r.CellWidth, r.CellHeight)
		}

		if !extentContainsWay {
			continue
		}

		/*
			Create encoded feature from raw data
		*/
		lineString := make(orb.LineString, len(nodes))
		for i, node := range nodes {
			lineString[i] = orb.Point{node.Lon, node.Lat}
		}

		encodedFeature := &feature.EncodedWayFeature{
			AbstractEncodedFeature: feature.AbstractEncodedFeature{
				ID:       osmId,
				Keys:     encodedKeys,
				Values:   encodedValues,
				Geometry: &lineString,
			},
			Nodes: nodes,
		}

		output <- encodedFeature
	}
}

func (r *RawFeaturesRepository) readRelationsFromCellData(output chan feature.EncodedFeature, reader *ownIo.IndexedReader) {
	for pos := int64(0); reader.Has(pos); {
		// See format details (bit position, field sizes, etc.) in function "writeRelationData".

		/*
			Read header fields
		*/
		osmId := reader.Uint64(pos + 0)
		numEncodedKeyBytes := reader.IntFromUint32(pos + 8)
		numValues := reader.IntFromUint32(pos + 12)
		numNodeIds := reader.IntFromUint16(pos + 16)
		numWayIds := reader.IntFromUint16(pos + 18)
		numChildRelationIds := reader.IntFromUint16(pos + 20)

		headerBytesCount := 8 + 4 + 4 + 2 + 2 + 2 // = 22

		pos += int64(headerBytesCount)

		/*
			Read keys
		*/
		encodedKeys := make([]byte, numEncodedKeyBytes)
		encodedValues := make([]int, numValues)
		copy(encodedKeys[:], reader.Read(pos, len(encodedKeys)))
		pos += int64(numEncodedKeyBytes)

		/*
			Read values
		*/
		for i := 0; i < numValues; i++ {
			encodedValues[i] = reader.IntFromUint24(pos)
			pos += 3
		}

		/*
			Read node-IDs
		*/
		nodeIds := make([]osm.NodeID, numNodeIds)
		for i := 0; i < numNodeIds; i++ {
			nodeIds[i] = osm.NodeID(reader.Int64(pos))
			pos += 8
		}

		/*
			Read way-IDs
		*/
		wayIds := make([]osm.WayID, numWayIds)
		for i := 0; i < numWayIds; i++ {
			wayIds[i] = osm.WayID(reader.Int64(pos))
			pos += 8
		}

		/*
			Read child relation-IDs
		*/
		childRelationIds := make([]osm.RelationID, numChildRelationIds)
		for i := 0; i < numChildRelationIds; i++ {
			childRelationIds[i] = osm.RelationID(reader.Int64(pos))
			pos += 8
		}

		/*
			Create encoded feature from raw data
		*/
		encodedFeature := &feature.EncodedRelationFeature{
			AbstractEncodedFeature: feature.AbstractEncodedFeature{
				ID:     osmId,
				Keys:   encodedKeys,
				Values: encodedValues,
			},
			NodeIds:          nodeIds,
			WayIds:           wayIds,
			ChildRelationIds: childRelationIds,
		}

		output <- encodedFeature
	}
}

package importing

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
	"soq/common"
	"soq/feature"
	"soq/index"
	ownIo "soq/io"
	ownOsm "soq/osm"
)

type TemporaryFeatureImporter struct {
	repository             *TemporaryFeatureRepository
	tagIndex               *index.TagIndex
	tagIndexTempValueArray []int
	nodeWriter             map[common.CellExtent]*bufio.Writer
	wayWriter              map[common.CellExtent]*bufio.Writer
	relationWriter         *bufio.Writer
	cellExtents            []common.CellExtent
	cellWidth              float64
	cellHeight             float64
}

func NewTemporaryFeatureImporter(repository *TemporaryFeatureRepository, tagIndex *index.TagIndex, cellExtents []common.CellExtent, cellWidth float64, cellHeight float64) *TemporaryFeatureImporter {
	return &TemporaryFeatureImporter{
		repository:             repository,
		tagIndex:               tagIndex,
		tagIndexTempValueArray: tagIndex.NewTempEncodedValueArray(),
		nodeWriter:             map[common.CellExtent]*bufio.Writer{},
		wayWriter:              map[common.CellExtent]*bufio.Writer{},
		cellExtents:            cellExtents,
		cellWidth:              cellWidth,
		cellHeight:             cellHeight,
	}
}

func (i *TemporaryFeatureImporter) Name() string {
	return "TemporaryFeatureImporter"
}

func (i *TemporaryFeatureImporter) Init() error {
	err := i.repository.Clear()
	if err != nil {
		return err
	}

	for _, cellExtent := range i.cellExtents {
		nodeWriter, err := getFileWriterForExtent(i.repository.BaseFolder, ownOsm.OsmObjNode.String(), cellExtent)
		if err != nil {
			return err
		}
		i.nodeWriter[cellExtent] = nodeWriter

		wayWriter, err := getFileWriterForExtent(i.repository.BaseFolder, ownOsm.OsmObjWay.String(), cellExtent)
		if err != nil {
			return err
		}
		i.wayWriter[cellExtent] = wayWriter
	}

	cellFileName := fmt.Sprintf("%s/%s.tmpcell", i.repository.BaseFolder, ownOsm.OsmObjRelation.String())
	relationWriter, err := getFileWriter(i.repository.BaseFolder, cellFileName)
	if err != nil {
		return err
	}
	i.relationWriter = relationWriter

	return nil
}

func (i *TemporaryFeatureImporter) HandleNode(node *osm.Node) error {
	var writer *bufio.Writer
	for _, cellExtent := range i.cellExtents {
		if cellExtent.ContainsLonLat(node.Lon, node.Lat, i.cellWidth, i.cellHeight) {
			writer = i.nodeWriter[cellExtent]
			break
		}
	}
	if writer == nil {
		return errors.Errorf("Could not find cell extent and writer for node %d", node.ID)
	}

	encodedKeys, encodedValues := i.tagIndex.EncodeTags(node.Tags, i.tagIndexTempValueArray)
	point := node.Point()
	return i.repository.writeNodeData(node.ID, encodedKeys, encodedValues, &point, writer)
}

func (i *TemporaryFeatureImporter) HandleWay(way *osm.Way) error {
	encodedKeys, encodedValues := i.tagIndex.EncodeTags(way.Tags, i.tagIndexTempValueArray)
	processedExtents := map[common.CellExtent]common.CellExtent{}

	for _, node := range way.Nodes {
		for _, cellExtent := range i.cellExtents {
			_, extentHasBeenProcessed := processedExtents[cellExtent]
			if !extentHasBeenProcessed && cellExtent.ContainsLonLat(node.Lon, node.Lat, i.cellWidth, i.cellHeight) {
				writer := i.wayWriter[cellExtent]
				err := i.repository.writeWayData(way.ID, encodedKeys, encodedValues, way.Nodes, writer)
				if err != nil {
					return err
				}
				processedExtents[cellExtent] = cellExtent
				break
			}
		}
	}

	return nil
}

func (i *TemporaryFeatureImporter) HandleRelation(relation *osm.Relation) error {
	var nodeIds []osm.NodeID
	var wayIds []osm.WayID
	var childRelationIds []osm.RelationID

	for _, member := range relation.Members {
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

	encodedKeys, encodedValues := i.tagIndex.EncodeTags(relation.Tags, i.tagIndexTempValueArray)
	return i.repository.writeRelationData(relation.ID, encodedKeys, encodedValues, nodeIds, wayIds, childRelationIds, i.relationWriter)
}

func (i *TemporaryFeatureImporter) Done() error {
	for _, nodeWriter := range i.nodeWriter {
		err := nodeWriter.Flush()
		if err != nil {
			return errors.Wrap(err, "Error closing node writer")
		}
	}

	for _, wayWriter := range i.wayWriter {
		err := wayWriter.Flush()
		if err != nil {
			return errors.Wrap(err, "Error closing way writer")
		}
	}

	err := i.relationWriter.Flush()
	if err != nil {
		return errors.Wrap(err, "Error closing relation writer")
	}

	return nil
}

type TemporaryFeatureRepository struct {
	index.BaseGridIndex
}

func NewTemporaryFeatureRepository(cellWidth float64, cellHeight float64, baseFolder string) *TemporaryFeatureRepository {
	gridIndexWriter := &TemporaryFeatureRepository{
		BaseGridIndex: index.BaseGridIndex{
			CellWidth:  cellWidth,
			CellHeight: cellHeight,
			BaseFolder: baseFolder,
		},
	}
	return gridIndexWriter
}

// Clear removes all files belonging to this repository.
func (r *TemporaryFeatureRepository) Clear() error {
	err := os.RemoveAll(r.BaseFolder)
	if err != nil {
		return errors.Wrapf(err, "Error deleting directory for temporary features %s", r.BaseFolder)
	}

	return nil
}

func (r *TemporaryFeatureRepository) writeNodeData(id osm.NodeID, keys []byte, values []int, point *orb.Point, f io.Writer) error {
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

func (r *TemporaryFeatureRepository) writeWayData(id osm.WayID, keys []byte, values []int, nodes osm.WayNodes, f io.Writer) error {
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

func (r *TemporaryFeatureRepository) writeRelationData(id osm.RelationID, keys []byte, values []int, nodeIds []osm.NodeID, wayIds []osm.WayID, childRelationIds []osm.RelationID, f io.Writer) error {
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

// TODO Create own tmp feature object that is only a wrapper for []byte. This makes deserialization faster. Of course such object should contain methods to obtain necessary data (ID, geometry, ...).
func (r *TemporaryFeatureRepository) ReadFeatures(readFeatureChannel chan feature.EncodedFeature, extent common.CellExtent) error {
	cellFile, err := getFileForExtent(r.BaseFolder, ownOsm.OsmObjNode.String(), extent)
	if err != nil {
		return errors.Wrapf(err, "Unable to open tmp node-feature cell %s", cellFile.Name())
	}
	cellReader := ownIo.NewIndexReader(cellFile)
	r.readNodesFromCellData(readFeatureChannel, cellReader, extent)
	err = cellFile.Close()
	if err != nil {
		return errors.Wrapf(err, "Unable to close tmp node-feature cell %s", cellFile.Name())
	}

	cellFile, err = getFileForExtent(r.BaseFolder, ownOsm.OsmObjWay.String(), extent)
	if err != nil {
		return errors.Wrapf(err, "Unable to open tmp way-feature cell %s", cellFile.Name())
	}
	cellReader = ownIo.NewIndexReader(cellFile)
	r.readWaysFromCellData(readFeatureChannel, cellReader, extent)
	err = cellFile.Close()
	if err != nil {
		return errors.Wrapf(err, "Unable to close tmp way-feature cell %s", cellFile.Name())
	}

	cellFileName := fmt.Sprintf("%s/%s.tmpcell", r.BaseFolder, ownOsm.OsmObjRelation.String())
	cellFile, err = os.OpenFile(cellFileName, os.O_RDONLY, 0666)
	if err != nil {
		return errors.Wrapf(err, "Unable to open tmp relation-feature cell %s", cellFile.Name())
	}
	cellReader = ownIo.NewIndexReader(cellFile)
	r.readRelationsFromCellData(readFeatureChannel, cellReader)
	err = cellFile.Close()
	if err != nil {
		return errors.Wrapf(err, "Unable to close tmp relation-feature cell %s", cellFile.Name())
	}

	close(readFeatureChannel)

	return nil
}

func (r *TemporaryFeatureRepository) readNodesFromCellData(output chan feature.EncodedFeature, reader *ownIo.IndexedReader, extent common.CellExtent) {
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

		if !extent.ContainsLonLat(float64(lon), float64(lat), r.CellWidth, r.CellHeight) {
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

func (r *TemporaryFeatureRepository) readWaysFromCellData(output chan feature.EncodedFeature, reader *ownIo.IndexedReader, extent common.CellExtent) {
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

			extentContainsWay = extentContainsWay || extent.ContainsLonLat(lon, lat, r.CellWidth, r.CellHeight)
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

func (r *TemporaryFeatureRepository) readRelationsFromCellData(output chan feature.EncodedFeature, reader *ownIo.IndexedReader) {
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

func getFileForExtent(cellFolderName string, filename string, cellExtent common.CellExtent) (*os.File, error) {
	cellFileName := getFilenameForExtent(cellFolderName, filename, cellExtent)
	return os.OpenFile(cellFileName, os.O_RDONLY, 0666)
}

func getFileWriterForExtent(cellFolderName string, filename string, cellExtent common.CellExtent) (*bufio.Writer, error) {
	cellFileName := getFilenameForExtent(cellFolderName, filename, cellExtent)
	return getFileWriter(cellFolderName, cellFileName)
}

func getFilenameForExtent(cellFolderName string, filename string, cellExtent common.CellExtent) string {
	return fmt.Sprintf("%s/%s_%d-%d_%d-%d.tmpcell", cellFolderName, filename, cellExtent.LowerLeftCell().X(), cellExtent.LowerLeftCell().Y(), cellExtent.UpperRightCell().X(), cellExtent.UpperRightCell().Y())
}

func getFileWriter(cellFolderName string, cellFileName string) (*bufio.Writer, error) {
	file, err := openFile(cellFolderName, cellFileName)
	if err != nil {
		return nil, err
	}

	return bufio.NewWriter(file), nil
}

func openFile(cellFolderName string, cellFileName string) (*os.File, error) {
	var file *os.File

	if _, err := os.Stat(cellFileName); err == nil {
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

	return file, nil
}

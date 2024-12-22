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

// Slice that contains the data of the feature that should be written to disk. These slice is reused to reduce garbage
// collection. This is a simple solutions and cannot safely be used for concurrent writes!
var data = make([]byte, 1000)

func ensureDataSliceSize(byteCount int) {
	for cap(data) < byteCount {
		newSize := cap(data) * 2
		sigolo.Debugf("Resize data slice from %d to %d", cap(data), newSize)
		data = make([]byte, newSize)
	}
}

type TemporaryFeatureImporter struct {
	repository             *TemporaryFeatureRepository
	tagIndex               *index.TagIndex
	tagIndexTempValueArray []int
	nodeWriter             map[common.CellExtent]*bufio.Writer
	nodeFiles              map[common.CellExtent]*os.File
	wayWriter              map[common.CellExtent]*bufio.Writer
	wayFiles               map[common.CellExtent]*os.File
	relationWriter         *bufio.Writer
	relationFile           *os.File
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
		nodeFiles:              map[common.CellExtent]*os.File{},
		wayWriter:              map[common.CellExtent]*bufio.Writer{},
		wayFiles:               map[common.CellExtent]*os.File{},
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
		file, nodeWriter, err := getFileWriterForExtent(i.repository.BaseFolder, ownOsm.OsmObjNode.String(), cellExtent)
		if err != nil {
			return err
		}
		i.nodeWriter[cellExtent] = nodeWriter
		i.nodeFiles[cellExtent] = file

		file, wayWriter, err := getFileWriterForExtent(i.repository.BaseFolder, ownOsm.OsmObjWay.String(), cellExtent)
		if err != nil {
			return err
		}
		i.wayWriter[cellExtent] = wayWriter
		i.wayFiles[cellExtent] = file
	}

	cellFileName := fmt.Sprintf("%s/%s.tmpcell", i.repository.BaseFolder, ownOsm.OsmObjRelation.String())
	file, relationWriter, err := getFileWriter(i.repository.BaseFolder, cellFileName)
	if err != nil {
		return err
	}
	i.relationWriter = relationWriter
	i.relationFile = file

	return nil
}

func (i *TemporaryFeatureImporter) HandleNode(node *osm.Node) error {
	var writer io.Writer
	for _, cellExtent := range i.cellExtents {
		if cellExtent.ContainsLonLat(node.Lon, node.Lat, i.cellWidth, i.cellHeight) {
			writer = i.nodeWriter[cellExtent]
			break
		}
	}
	if writer == nil {
		return errors.Errorf("Could not find cell extent and writer for node %d", node.ID)
	}

	encodedKeys, encodedValues := i.tagIndex.EncodeTags(node.Tags)
	point := node.Point()
	return i.repository.writeNodeData(node.ID, encodedKeys, encodedValues, &point, writer)
}

func (i *TemporaryFeatureImporter) HandleWay(way *osm.Way) error {
	encodedKeys, encodedValues := i.tagIndex.EncodeTags(way.Tags)
	data := i.repository.getWayData(way.ID, encodedKeys, encodedValues, way.Nodes)

	for _, cellExtent := range i.cellExtents {
		for _, node := range way.Nodes {
			if cellExtent.ContainsLonLat(node.Lon, node.Lat, i.cellWidth, i.cellHeight) {
				writer := i.wayWriter[cellExtent]
				_, err := writer.Write(data)
				if err != nil {
					return err
				}
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

	encodedKeys, encodedValues := i.tagIndex.EncodeTags(relation.Tags)
	return i.repository.writeRelationData(relation.ID, encodedKeys, encodedValues, nodeIds, wayIds, childRelationIds, i.relationWriter)
}

func (i *TemporaryFeatureImporter) Done() error {
	var err error
	for _, writer := range i.nodeWriter {
		err = writer.Flush()
		if err != nil {
			return errors.Wrapf(err, "Error flushing node writer")
		}
	}
	for _, file := range i.nodeFiles {
		err = file.Close()
		if err != nil {
			return errors.Wrapf(err, "Error closing node writer %s", file.Name())
		}
	}

	for _, writer := range i.wayWriter {
		err = writer.Flush()
		if err != nil {
			return errors.Wrapf(err, "Error flushing way writer")
		}
	}
	for _, file := range i.wayFiles {
		err = file.Close()
		if err != nil {
			return errors.Wrapf(err, "Error closing way file %s", file.Name())
		}
	}

	err = i.relationWriter.Flush()
	if err != nil {
		return errors.Wrapf(err, "Error closing relation writer")
	}
	err = i.relationFile.Close()
	if err != nil {
		return errors.Wrapf(err, "Error closing relation writer %s", i.relationFile.Name())
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

func (r *TemporaryFeatureRepository) writeNodeData(id osm.NodeID, keys []int, values []int, point *orb.Point, f io.Writer) error {
	/*
		Entry format:

		Names: | osmId | lon | lat | num. tags |          encodedTags          |
		Bytes: |   8   |  4  |  4  |     2     | key (32 bit) | value (32 bit) |

		Tags are stored as a list of "num. tags" many key-value-pairs.
	*/

	if len(keys) != len(values) {
		return errors.Errorf("Number of keys and values for node %d different: keys %d, values %d", id, len(keys), len(values))
	}
	numberOfTags := len(keys)

	// The number of key-bins to store is determined by the bin with the highest index that is not empty (i.e. all 0s).
	// If only the first bin contains some 1s (i.e. keys that are set on the feature) and the next 100 bins are empty,
	// then there's no reason to store those empty bins. This reduced the cell-file size for hamburg-latest (45 MB PBF)
	// by a factor of ten!
	headerBytesCount := 8 + 4 + 4 + 2 // = 18
	byteCount := headerBytesCount
	byteCount += len(keys) * 4
	byteCount += len(values) * 4

	ensureDataSliceSize(byteCount)

	binary.LittleEndian.PutUint64(data[0:], uint64(id))
	binary.LittleEndian.PutUint32(data[8:], math.Float32bits(float32(point.Lon())))
	binary.LittleEndian.PutUint32(data[12:], math.Float32bits(float32(point.Lat())))
	binary.LittleEndian.PutUint16(data[16:], uint16(numberOfTags))

	pos := headerBytesCount

	/*
		Write tags
	*/
	for i := 0; i < numberOfTags; i++ {
		binary.LittleEndian.PutUint32(data[pos:], uint32(keys[i]))
		pos += 4
		binary.LittleEndian.PutUint32(data[pos:], uint32(values[i]))
		pos += 4
	}

	_, err := f.Write(data[0:byteCount])
	return err
}

func (r *TemporaryFeatureRepository) getWayData(id osm.WayID, keys []int, values []int, nodes osm.WayNodes) []byte {
	/*
		Entry format:

		Names: | osmId | num. keys | num. tags |          encodedTags          |       nodes       |
		Bytes: |   8   |     2     |      2    | key (32 bit) | value (32 bit) | <num. nodes> * 16 |

		Tags are stored as a list of "num. tags" many key-value-pairs.

		The nodes section contains all nodes, not only the ones within this cell. This enables geometric checks, even
		in cases where no way-node is within this cell. The nodes are stores in the following way:
		<id (64-bit)><lon (32-bit)><lat (23-bit)>
	*/

	if len(keys) != len(values) {
		return nil
	}
	numberOfTags := len(keys)

	nodeIdBytes := len(nodes) * 16 // Each ID is a 64-bit int + 2*4 bytes for lat/lon

	headerByteCount := 8 + 2 + 2 // = 12
	byteCount := headerByteCount
	byteCount += numberOfTags * 4
	byteCount += numberOfTags * 4
	byteCount += nodeIdBytes

	ensureDataSliceSize(byteCount)

	/*
		Write header
	*/
	binary.LittleEndian.PutUint64(data[0:], uint64(id))
	binary.LittleEndian.PutUint16(data[8:], uint16(numberOfTags))
	binary.LittleEndian.PutUint16(data[10:], uint16(len(nodes)))

	pos := headerByteCount

	/*
		Write tags
	*/
	for i := 0; i < numberOfTags; i++ {
		binary.LittleEndian.PutUint32(data[pos:], uint32(keys[i]))
		pos += 4
		binary.LittleEndian.PutUint32(data[pos:], uint32(values[i]))
		pos += 4
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

	return data[0:byteCount]
}

func (r *TemporaryFeatureRepository) writeRelationData(id osm.RelationID, keys []int, values []int, nodeIds []osm.NodeID, wayIds []osm.WayID, childRelationIds []osm.RelationID, f io.Writer) error {
	/*
		Entry format:

		Names: | osmId | num. keys | num. tags |  num. ways | num. child rels |          encodedTags          |     node IDs      |     way IDs     |    child rel. IDs     |
		Bytes: |   8   |     2     |      2    |      2     |        2        | key (32 bit) | value (32 bit) |  <num. nodes> * 8 | <num. ways> * 8 | <num. child rels> * 8 |

		Tags are stored as a list of "num. tags" many key-value-pairs.

		The "bbox" field are 4 32-bit floats for the min-lon, min-lat, max-lon and max-lat values.

		// TODO store real geometry. Including geometry of sub-relations?
	*/
	if len(keys) != len(values) {
		return errors.Errorf("Number of keys and values for node %d different: keys %d, values %d", id, len(keys), len(values))
	}
	numberOfTags := len(keys)

	nodeIdBytes := len(nodeIds) * 8                   // IDs are all 64-bit integers
	wayIdBytes := len(wayIds) * 8                     // IDs are all 64-bit integers
	childRelationIdBytes := len(childRelationIds) * 8 // IDs are all 64-bit integers

	headerBytesCount := 8 + 2 + 2 + 2 + 2 + 2
	byteCount := headerBytesCount
	byteCount += numberOfTags * 4
	byteCount += numberOfTags * 4
	byteCount += nodeIdBytes
	byteCount += wayIdBytes
	byteCount += childRelationIdBytes

	ensureDataSliceSize(byteCount)

	binary.LittleEndian.PutUint64(data[0:], uint64(id))
	binary.LittleEndian.PutUint16(data[8:], uint16(numberOfTags))
	binary.LittleEndian.PutUint16(data[10:], uint16(len(nodeIds)))
	binary.LittleEndian.PutUint16(data[12:], uint16(len(wayIds)))
	binary.LittleEndian.PutUint16(data[14:], uint16(len(childRelationIds)))

	pos := headerBytesCount

	/*
		Write tags
	*/
	for i := 0; i < numberOfTags; i++ {
		binary.LittleEndian.PutUint32(data[pos:], uint32(keys[i]))
		pos += 4
		binary.LittleEndian.PutUint32(data[pos:], uint32(values[i]))
		pos += 4
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

	_, err := f.Write(data[0:byteCount])
	return err
}

// TODO Create own tmp feature object that is only a wrapper for []byte. This makes deserialization faster. Of course such object should contain methods to obtain necessary data (ID, geometry, ...).
func (r *TemporaryFeatureRepository) ReadFeatures(readFeatureChannel chan feature.Feature, extent common.CellExtent) error {
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
	cellFile, err = os.OpenFile(cellFileName, os.O_RDONLY, 0644)
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

func (r *TemporaryFeatureRepository) readNodesFromCellData(output chan feature.Feature, reader *ownIo.IndexedReader, extent common.CellExtent) {
	for pos := int64(0); reader.Has(pos); {
		// See format details (bit position, field sizes, etc.) in function "writeNodeData".

		/*
			Read header fields
		*/
		osmId := reader.Uint64(pos + 0)
		lon := reader.Float32(pos + 8)
		lat := reader.Float32(pos + 12)
		numberOfTags := reader.IntFromUint16(pos + 16)

		headerBytesCount := 8 + 4 + 4 + 2 // = 18

		pos += int64(headerBytesCount)

		if !extent.ContainsLonLat(float64(lon), float64(lat), r.CellWidth, r.CellHeight) {
			pos += int64(numberOfTags * 4) // keys
			pos += int64(numberOfTags * 4) // values
			continue
		}

		/*
			Read tags
		*/
		encodedKeys := make([]int, numberOfTags)
		encodedValues := make([]int, numberOfTags)

		for i := 0; i < numberOfTags; i++ {
			encodedKeys[i] = reader.IntFromUint32(pos)
			pos += 4
			encodedValues[i] = reader.IntFromUint32(pos)
			pos += 4
		}

		/*
			Create encoded feature from raw data
		*/
		encodedFeature := &index.EncodedNodeFeature{
			AbstractEncodedFeature: index.AbstractEncodedFeature{
				ID:       osmId,
				Geometry: &orb.Point{float64(lon), float64(lat)},
				Keys:     encodedKeys,
				Values:   encodedValues,
			},
		}

		output <- encodedFeature
	}
}

func (r *TemporaryFeatureRepository) readWaysFromCellData(output chan feature.Feature, reader *ownIo.IndexedReader, extent common.CellExtent) {
	for pos := int64(0); reader.Has(pos); {
		// See format details (bit position, field sizes, etc.) in function "writeWayData".

		/*
			Read header fields
		*/
		osmId := reader.Uint64(pos + 0)
		numberOfTags := reader.IntFromUint16(pos + 8)
		numNodes := reader.IntFromUint16(pos + 10)

		headerBytesCount := 8 + 2 + 2 // = 12

		pos += int64(headerBytesCount)

		/*
			Read tags
		*/
		encodedKeys := make([]int, numberOfTags)
		encodedValues := make([]int, numberOfTags)

		for i := 0; i < numberOfTags; i++ {
			encodedKeys[i] = reader.IntFromUint32(pos)
			pos += 4
			encodedValues[i] = reader.IntFromUint32(pos)
			pos += 4
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

		encodedFeature := &index.EncodedWayFeature{
			AbstractEncodedFeature: index.AbstractEncodedFeature{
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

func (r *TemporaryFeatureRepository) readRelationsFromCellData(output chan feature.Feature, reader *ownIo.IndexedReader) {
	for pos := int64(0); reader.Has(pos); {
		// See format details (bit position, field sizes, etc.) in function "writeRelationData".

		/*
			Read header fields
		*/
		osmId := reader.Uint64(pos + 0)
		numberOfTags := reader.IntFromUint16(pos + 8)
		numNodeIds := reader.IntFromUint16(pos + 10)
		numWayIds := reader.IntFromUint16(pos + 12)
		numChildRelationIds := reader.IntFromUint16(pos + 14)

		headerBytesCount := 8 + 2 + 2 + 2 + 2 // = 16

		pos += int64(headerBytesCount)

		/*
			Read tags
		*/
		encodedKeys := make([]int, numberOfTags)
		encodedValues := make([]int, numberOfTags)

		for i := 0; i < numberOfTags; i++ {
			encodedKeys[i] = reader.IntFromUint32(pos)
			pos += 4
			encodedValues[i] = reader.IntFromUint32(pos)
			pos += 4
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
		encodedFeature := &index.EncodedRelationFeature{
			AbstractEncodedFeature: index.AbstractEncodedFeature{
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
	return os.OpenFile(cellFileName, os.O_RDONLY, 0644)
}

func getFileWriterForExtent(cellFolderName string, filename string, cellExtent common.CellExtent) (*os.File, *bufio.Writer, error) {
	cellFileName := getFilenameForExtent(cellFolderName, filename, cellExtent)
	return getFileWriter(cellFolderName, cellFileName)
}

func getFilenameForExtent(cellFolderName string, filename string, cellExtent common.CellExtent) string {
	return fmt.Sprintf("%s/%s_%d-%d_%d-%d.tmpcell", cellFolderName, filename, cellExtent.LowerLeftCell().X(), cellExtent.LowerLeftCell().Y(), cellExtent.UpperRightCell().X(), cellExtent.UpperRightCell().Y())
}

func getFileWriter(cellFolderName string, cellFileName string) (*os.File, *bufio.Writer, error) {
	file, err := openFile(cellFolderName, cellFileName, os.O_WRONLY)
	if err != nil {
		return nil, nil, err
	}

	return file, bufio.NewWriter(file), nil
}

func openFile(cellFolderName string, cellFileName string, fileFlag int) (*os.File, error) {
	var file *os.File

	if _, err := os.Stat(cellFileName); err == nil {
		// Cell file does exist -> open it
		sigolo.Tracef("Cell file %s already exist but is not cached, I'll open it", cellFileName)
		file, err = os.OpenFile(cellFileName, fileFlag, 0644)
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

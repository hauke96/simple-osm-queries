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
	"reflect"
	"soq/common"
	"soq/feature"
	ownOsm "soq/osm"
	"strconv"
	"sync"
	"time"
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

type GridIndexWriter struct {
	BaseGridIndex

	cacheFileWriterFiles     map[io.Writer]*os.File
	cacheFileWriters         map[int64]*[3]*bufio.Writer // Key is a aggregation of the cells x and y coordinate. The array index is based on the object type. Value be a pointer to not create unnecessary files.
	cacheFileMutexes         map[io.Writer]*sync.Mutex
	cacheFileMutex           *sync.Mutex
	cacheRawEncodedNodes     map[common.CellIndex][]feature.NodeFeature
	cacheRawEncodedWays      map[common.CellIndex][]feature.WayFeature
	cacheRawEncodedRelations map[common.CellIndex][]feature.RelationFeature

	// During writing, some of the half-written data must be read again. This requires some functionality of the
	// GridIndexReader during importing data and writing a new index.
	gridIndexReader *GridIndexReader
}

func ImportTempFeatures(tempRawFeatureChannel chan feature.Feature, baseFolder string, cellWidth float64, cellHeight float64, cellExtent common.CellExtent) error {
	gridIndexWriter := NewGridIndexWriter(cellWidth, cellHeight, baseFolder)

	sigolo.Debug("Read OSM data and write them as raw encoded features")

	err := gridIndexWriter.WriteOsmToRawEncodedFeatures(tempRawFeatureChannel, cellExtent)
	if err != nil {
		return err
	}

	gridIndexWriter.addAdditionalIdsToObjectsInCells(cellExtent.GetCellIndices())

	return nil
}

func NewGridIndexWriter(cellWidth float64, cellHeight float64, baseFolder string) *GridIndexWriter {
	baseGridIndex := BaseGridIndex{
		CellWidth:  cellWidth,
		CellHeight: cellHeight,
		BaseFolder: baseFolder,
	}
	gridIndexWriter := &GridIndexWriter{
		BaseGridIndex:            baseGridIndex,
		cacheFileWriterFiles:     map[io.Writer]*os.File{},
		cacheFileWriters:         map[int64]*[3]*bufio.Writer{},
		cacheFileMutexes:         map[io.Writer]*sync.Mutex{},
		cacheFileMutex:           &sync.Mutex{},
		cacheRawEncodedNodes:     map[common.CellIndex][]feature.NodeFeature{},
		cacheRawEncodedWays:      map[common.CellIndex][]feature.WayFeature{},
		cacheRawEncodedRelations: map[common.CellIndex][]feature.RelationFeature{},
		gridIndexReader: &GridIndexReader{
			BaseGridIndex:        baseGridIndex,
			checkFeatureValidity: false,
		},
	}
	return gridIndexWriter
}

// WriteOsmToRawEncodedFeatures Reads the input feature channel and converts all OSM objects into raw encoded features and
// writes them into their respective cells. The returned cell map contains all cells that contain nodes.
func (g *GridIndexWriter) WriteOsmToRawEncodedFeatures(tempRawFeatureChannel chan feature.Feature, cellExtent common.CellExtent) error {
	sigolo.Debug("Start converting OSM data to raw encoded features")
	importStartTime := time.Now()

	// We assume relations, like all other object types, to be sorted in a way that when a relation with child relations
	// appears, all child relation members have been visited before. Therefore, this map then contains all bounds of the
	// child relation members. Cyclic relation structures (when A is child of B but B also of A) are not supported.
	relationToBound := map[osm.RelationID]*orb.Bound{}

	firstWayHasBeenProcessed := false
	firstRelationHasBeenProcessed := false

	wayToCellsMap := map[osm.WayID][]common.CellIndex{}
	relationToCellsMap := map[osm.RelationID][]common.CellIndex{}

	nodeToPoint := map[osm.NodeID]*orb.Point{}
	wayToBound := map[osm.WayID]*orb.Bound{}

	sigolo.Debug("Process nodes (1/3)")
	for obj := range tempRawFeatureChannel {
		switch rawFeature := obj.(type) {
		case feature.NodeFeature:
			cell := g.GetCellIndexForCoordinate(rawFeature.GetLon(), rawFeature.GetLat())

			id := osm.NodeID(rawFeature.GetID())
			nodeToPoint[id] = rawFeature.GetGeometry().(*orb.Point)

			err := g.writeOsmObjectToCellCache(cell, rawFeature)
			sigolo.FatalCheck(err)
		case feature.WayFeature:
			if !firstWayHasBeenProcessed {
				sigolo.Debug("Start processing ways (2/3)")
				firstWayHasBeenProcessed = true
			}

			wayCells := map[common.CellIndex]common.CellIndex{}
			for _, node := range rawFeature.GetNodes() {
				cell := g.GetCellIndexForCoordinate(node.Lon, node.Lat)
				wayCells[cell] = cell
			}

			id := osm.WayID(rawFeature.GetID())
			wayToCellsMap[id] = make([]common.CellIndex, len(wayCells))
			j := 0
			for _, cell := range wayCells {
				wayToCellsMap[id][j] = cell
				j++
			}

			bbox := rawFeature.GetGeometry().Bound()
			wayToBound[id] = &bbox

			for _, cell := range wayCells {
				err := g.writeOsmObjectToCellCache(cell, rawFeature)
				sigolo.FatalCheck(err)
			}
		case feature.RelationFeature:
			if !firstRelationHasBeenProcessed {
				sigolo.Debug("Start processing relations (3/3)")
				firstRelationHasBeenProcessed = true
			}

			extentContainsRelation := false
			for _, nodeId := range rawFeature.GetNodeIds() {
				nodePoint := nodeToPoint[nodeId]
				if nodePoint != nil {
					cell := g.GetCellIndexForCoordinate(nodePoint.X(), nodePoint.Y())
					extentContainsRelation = extentContainsRelation || cellExtent.Contains(cell)
					if extentContainsRelation {
						break
					}
				}
			}
			if !extentContainsRelation {
				for _, wayId := range rawFeature.GetWayIds() {
					extentContainsRelation = extentContainsRelation || cellExtent.ContainsAny(wayToCellsMap[wayId])
					if extentContainsRelation {
						break
					}
				}
			}
			if !extentContainsRelation {
				for _, relationId := range rawFeature.GetChildRelationIds() {
					extentContainsRelation = extentContainsRelation || cellExtent.ContainsAny(relationToCellsMap[relationId])
					if extentContainsRelation {
						break
					}
				}
			}
			if !extentContainsRelation {
				continue
			}

			relCells := map[common.CellIndex]common.CellIndex{}

			var bbox *orb.Bound

			for _, nodeId := range rawFeature.GetNodeIds() {
				point, ok := nodeToPoint[nodeId]
				if ok {
					cell := g.GetCellIndexForCoordinate(point.X(), point.Y())
					relCells[cell] = cell
					if bbox == nil {
						nodeBound := point.Bound()
						bbox = &nodeBound
					} else {
						b := bbox.Union(point.Bound())
						bbox = &b
					}
				}
			}
			for _, wayId := range rawFeature.GetWayIds() {
				cells := wayToCellsMap[wayId]
				for _, cell := range cells {
					relCells[cell] = cell
				}
				if bound, ok := wayToBound[wayId]; ok {
					if bbox == nil {
						bbox = bound
					} else {
						b := bbox.Union(*bound)
						bbox = &b
					}
				}
			}
			for _, relationId := range rawFeature.GetChildRelationIds() {
				cells := relationToCellsMap[relationId]
				for _, cell := range cells {
					relCells[cell] = cell
				}
				if bound, ok := relationToBound[relationId]; ok {
					if bbox == nil {
						bbox = bound
					} else {
						b := bbox.Union(*bound)
						bbox = &b
					}
				}
			}

			id := osm.RelationID(rawFeature.GetID())
			relationToBound[id] = bbox

			if bbox == nil {
				sigolo.Warnf("No BBOX for relation %d could be determined. This relation will be skipped.", id)
				continue
			}

			// TODO This polygon is not accurate. Not only because it's a bbox but also because the relation might stretch over multiple sub-extents which are not covered here. This bbox would roughlty stretch only over one sub-extent.
			rawFeature.SetGeometry(bbox.ToPolygon())

			for _, cell := range relCells {
				err := g.writeOsmObjectToCellCache(cell, rawFeature)
				sigolo.FatalCheck(err)
			}
		}
	}

	importDuration := time.Since(importStartTime)
	sigolo.Debugf("Created raw encoded features from OSM data in %s", importDuration)

	return nil
}

func (g *GridIndexWriter) addAdditionalIdsToObjectsInCells(cells []common.CellIndex) {
	numberOfCells := len(cells)
	sigolo.Debugf("Start adding way and relation IDs to raw encoded nodes in %d cells", numberOfCells)

	importStartTime := time.Now()

	nodeToRelations := make(map[uint64][]osm.RelationID)
	waysToRelations := make(map[uint64][]osm.RelationID)
	relationsToParentRelations := make(map[uint64][]osm.RelationID)

	for _, cell := range cells {
		sigolo.Tracef("[Cell %v] Collect relationships between nodes, ways and relations", cell)

		for _, relation := range g.cacheRawEncodedRelations[cell] {
			for _, nodeId := range relation.GetNodeIds() {
				nodeToRelations[uint64(nodeId)] = append(nodeToRelations[uint64(nodeId)], osm.RelationID(relation.GetID()))
			}
			for _, wayId := range relation.GetWayIds() {
				waysToRelations[uint64(wayId)] = append(nodeToRelations[uint64(wayId)], osm.RelationID(relation.GetID()))
			}
			for _, relId := range relation.GetChildRelationIds() {
				relationsToParentRelations[uint64(relId)] = append(nodeToRelations[uint64(relId)], osm.RelationID(relation.GetID()))
			}
		}
	}

	for _, cell := range cells {
		sigolo.Tracef("[Cell %v] Adding additional IDs and writing encoded features to disk", cell)

		err := g.addAdditionalIdsToObjectsOfType(ownOsm.OsmObjNode, nodeToRelations, cell)
		if err != nil {
			sigolo.Errorf("Error adding additional IDs to nodes: %+v", err)
			// TODO return error
		}

		err = g.addAdditionalIdsToObjectsOfType(ownOsm.OsmObjWay, waysToRelations, cell)
		if err != nil {
			sigolo.Errorf("Error adding additional IDs to ways: %+v", err)
			// TODO return error
		}

		err = g.addAdditionalIdsToObjectsOfType(ownOsm.OsmObjRelation, relationsToParentRelations, cell)
		if err != nil {
			sigolo.Errorf("Error adding additional IDs to relations: %+v", err)
			// TODO return error
		}

		cellPositionKey := g.getMapKeyForCell(cell.X(), cell.Y())
		if writers, ok := g.cacheFileWriters[cellPositionKey]; ok {
			for writerIndex, writer := range writers {
				if writer == nil {
					continue
				}

				file := g.cacheFileWriterFiles[writer]

				err = writer.Flush()
				if err != nil {
					sigolo.Errorf("Error flushing buffered writer %d for file %s", writerIndex, file.Name())
					// TODO return error
				}

				err = file.Close()
				if err != nil {
					sigolo.Errorf("Error closing file %s", file.Name())
					// TODO return error
				}

				delete(g.cacheFileWriterFiles, writer)
			}
			delete(g.cacheFileWriters, cellPositionKey)
		}
	}

	importDuration := time.Since(importStartTime)
	sigolo.Debugf("Done adding way IDs to raw encoded nodes in %s", importDuration)
}

// addAdditionalIdsToObjectsOfType adds the reverse IDs to the given object type. For example nodes themselves do not
// contain IDs to the ways they belong to. So this function adds this information to the given object type. In case of
// relations, this is done using the given object to relation map. This map maps an ID of the given object type to the
// relations this object is part of.
func (g *GridIndexWriter) addAdditionalIdsToObjectsOfType(objectType ownOsm.OsmObjectType, objectTypeToRelationMapping map[uint64][]osm.RelationID, cell common.CellIndex) error {
	var err error

	//cellFolderName := path.Join(g.BaseFolder, objectType.String(), strconv.Itoa(cell.X()))
	//cellFileName := path.Join(cellFolderName, strconv.Itoa(cell.Y())+".cell")

	//if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
	//	if objectType == ownOsm.OsmObjNode {
	//		// We got all the cells in which nodes are. When this cell doesn't exist, then something went really wrong.
	//		util.LogFatalBug("Cell file %s for nodes could not be found in the list of node-cells. This is a critical error and should not have happened", cellFileName)
	//	}
	//	// We found a cell only containing nodes. This might happen sometimes and is ok.
	//	sigolo.Debugf("Cell file %s for OSM object type %v does not exist, since it might only contain nodes", cellFileName, objectType)
	//	return nil
	//}
	//sigolo.FatalCheck(errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName))

	//sigolo.Tracef("[Cell %v] Read cell file %s", cell, cellFileName)
	//data, err := os.ReadFile(cellFileName)
	//sigolo.FatalCheck(errors.Wrapf(err, "Unable to read %v-cell x=%d, y=%d", objectType, cell.X(), cell.Y()))

	//sigolo.Tracef("[Cell %v] Read %v objects from cell and write them including additional IDs", cell, objectType)
	//readFeatureChannel := make(chan []feature.Feature)
	//var finishWaitGroup sync.WaitGroup
	//finishWaitGroup.Add(1)

	// TODO Use the data-bytes directly without deserialization first. This probably saves a lot of time.
	//go func() {
	//	for encFeatures := range readFeatureChannel {
	//		for _, encFeature := range encFeatures {
	//			if encFeature == nil {
	//				continue
	//			}

	switch objectType {
	case ownOsm.OsmObjNode:
		nodeToWays := map[uint64][]osm.WayID{}
		ways := g.cacheRawEncodedWays[cell]
		for _, way := range ways {
			for _, nodeId := range way.GetNodes().NodeIDs() {
				nodeToWays[uint64(nodeId)] = append(nodeToWays[uint64(nodeId)], osm.WayID(way.GetID()))
			}
		}

		for _, encFeature := range g.cacheRawEncodedNodes[cell] {
			if wayIds, ok := nodeToWays[encFeature.GetID()]; ok {
				encFeature.SetWayIds(wayIds)
			}
			if relationIds, ok := objectTypeToRelationMapping[encFeature.GetID()]; ok {
				encFeature.SetRelationIds(relationIds)
			}

			err = g.writeOsmObjectToCell(cell.X(), cell.Y(), encFeature)
			sigolo.FatalCheck(err)
		}
		delete(g.cacheRawEncodedNodes, cell)
	case ownOsm.OsmObjWay:
		for _, encFeature := range g.cacheRawEncodedWays[cell] {
			if relationIds, ok := objectTypeToRelationMapping[encFeature.GetID()]; ok {
				encFeature.SetRelationIds(relationIds)
			}

			err = g.writeOsmObjectToCell(cell.X(), cell.Y(), encFeature)
			sigolo.FatalCheck(err)
		}
		delete(g.cacheRawEncodedWays, cell)
	case ownOsm.OsmObjRelation:
		for _, encFeature := range g.cacheRawEncodedRelations[cell] {
			if relationIds, ok := objectTypeToRelationMapping[encFeature.GetID()]; ok {
				encFeature.SetParentRelationIds(relationIds)
			}

			err = g.writeOsmObjectToCell(cell.X(), cell.Y(), encFeature)
			sigolo.FatalCheck(err)
		}
		delete(g.cacheRawEncodedRelations, cell)
	default:
		return errors.Errorf("Unsupported object type %v to add IDs to", objectType)
	}

	return nil
}

func (g *GridIndexWriter) writeOsmObjectToCell(cellX int, cellY int, encodedFeature feature.Feature) error {
	switch featureObj := encodedFeature.(type) {
	case feature.NodeFeature:
		f, err := g.getCellFile(cellX, cellY, ownOsm.OsmObjNode)
		if err != nil {
			return err
		}
		err = g.writeNodeData(featureObj, f)
		if err != nil {
			return errors.Wrapf(err, "Unable to write node %d to cell x=%d, y=%d", encodedFeature.GetID(), cellX, cellY)
		}
		return nil
	case feature.WayFeature:
		f, err := g.getCellFile(cellX, cellY, ownOsm.OsmObjWay)
		if err != nil {
			return err
		}
		err = g.writeWayData(featureObj, f)
		if err != nil {
			return errors.Wrapf(err, "Unable to write way %d to cell x=%d, y=%d", encodedFeature.GetID(), cellX, cellY)
		}
		return nil
	case feature.RelationFeature:
		f, err := g.getCellFile(cellX, cellY, ownOsm.OsmObjRelation)
		if err != nil {
			return err
		}
		err = g.writeRelationData(featureObj, f)
		if err != nil {
			return errors.Wrapf(err, "Unable to write way %d to cell x=%d, y=%d", encodedFeature.GetID(), cellX, cellY)
		}
		return nil
	}

	return nil
}

func (g *GridIndexWriter) writeOsmObjectToCellCache(cell common.CellIndex, encodedFeature feature.Feature) error {
	switch featureObj := encodedFeature.(type) {
	case feature.NodeFeature:
		g.cacheRawEncodedNodes[cell] = append(g.cacheRawEncodedNodes[cell], featureObj)
	case feature.WayFeature:
		g.cacheRawEncodedWays[cell] = append(g.cacheRawEncodedWays[cell], featureObj)
	case feature.RelationFeature:
		g.cacheRawEncodedRelations[cell] = append(g.cacheRawEncodedRelations[cell], featureObj)
	}

	return nil
}

func (g *GridIndexWriter) getCellFile(cellX int, cellY int, objectType ownOsm.OsmObjectType) (io.Writer, error) {
	g.cacheFileMutex.Lock()

	cellPositionKey := g.getMapKeyForCell(cellX, cellY)
	writers, hasWriterForCell := g.cacheFileWriters[cellPositionKey]
	writersIndex := g.getWriterIndex(objectType)
	if hasWriterForCell {
		writer := writers[writersIndex]
		if writer != nil {
			// We have a writer for this cell and this type of object -> return it
			g.cacheFileMutex.Unlock()
			return writer, nil
		}
	}

	// Not filepath.Join because in this case it's slower than simple concatenation
	cellFolderName := g.BaseFolder + "/" + objectType.String() + "/" + strconv.Itoa(cellX)
	cellFileName := cellFolderName + "/" + strconv.Itoa(cellY) + ".cell"

	// Cell file not hasWriterForCell
	var file *os.File

	if _, err := os.Stat(cellFileName); err == nil {
		// Cell file does exist -> open it
		sigolo.Tracef("Cell file %s already exist but is not cached, I'll open it", cellFileName)
		file, err = os.OpenFile(cellFileName, os.O_WRONLY, 0644)
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
				return nil, errors.Wrapf(err, "Unable to create cell folder %s for cellY=%d", cellFolderName, cellY)
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

	if !hasWriterForCell {
		g.cacheFileWriters[cellPositionKey] = &[3]*bufio.Writer{}
	}

	writer := bufio.NewWriter(file)
	g.cacheFileWriters[cellPositionKey][writersIndex] = writer
	g.cacheFileWriterFiles[writer] = file
	g.cacheFileMutexes[writer] = &sync.Mutex{}

	g.cacheFileMutex.Unlock()

	return writer, nil
}

func (g *GridIndexWriter) getMapKeyForCell(cellX int, cellY int) int64 {
	return int64(cellX)<<32 | int64(cellY)
}

func (g *GridIndexWriter) getWriterIndex(objectType ownOsm.OsmObjectType) int {
	writersIndex := -1
	if objectType == ownOsm.OsmObjNode {
		writersIndex = 0
	} else if objectType == ownOsm.OsmObjWay {
		writersIndex = 1
	} else {
		writersIndex = 2
	}
	return writersIndex
}

func (g *GridIndexWriter) writeNodeData(encodedFeature feature.NodeFeature, f io.Writer) error {
	/*
		Entry format:

		Names: | osmId | lon | lat | num. tags | num. ways | num. rels |          encodedTags          |     way IDs     |   relation IDs  |
		Bytes: |   8   |  4  |  4  |     2     |     2     |     2     | key (32 bit) | value (32 bit) | <num. ways> * 8 | <num. rels> * 8 |

		Tags are stored as a list of "num. tags" many key-value-pairs.
	*/

	keys := encodedFeature.GetKeys()
	values := encodedFeature.GetValues()
	if len(keys) != len(values) {
		return errors.Errorf("Number of keys and values for node %d different: keys %d, values %d", encodedFeature.GetID(), len(keys), len(values))
	}
	numberOfTags := len(keys)

	wayIdBytes := len(encodedFeature.GetWayIds()) * 8           // IDs are all 64-bit integers
	relationIdBytes := len(encodedFeature.GetRelationIds()) * 8 // IDs are all 64-bit integers

	headerBytesCount := 8 + 4 + 4 + 2 + 2 + 2 // = 20
	byteCount := headerBytesCount
	byteCount += numberOfTags * 4
	byteCount += numberOfTags * 4
	byteCount += wayIdBytes
	byteCount += relationIdBytes

	ensureDataSliceSize(byteCount)

	binary.LittleEndian.PutUint64(data[0:], encodedFeature.GetID())
	binary.LittleEndian.PutUint32(data[8:], math.Float32bits(float32(encodedFeature.GetLon())))
	binary.LittleEndian.PutUint32(data[12:], math.Float32bits(float32(encodedFeature.GetLat())))
	binary.LittleEndian.PutUint16(data[16:], uint16(numberOfTags))
	binary.LittleEndian.PutUint16(data[18:], uint16(len(encodedFeature.GetWayIds())))
	binary.LittleEndian.PutUint16(data[20:], uint16(len(encodedFeature.GetRelationIds())))

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
		Write way-IDs
	*/
	for _, wayId := range encodedFeature.GetWayIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(wayId))
		pos += 8
	}

	/*
		Write relation-IDs
	*/
	for _, relationId := range encodedFeature.GetRelationIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	return g.writeData(encodedFeature, data[0:byteCount], f)
}

func (g *GridIndexWriter) writeWayData(encodedFeature feature.WayFeature, f io.Writer) error {
	/*
		Entry format:

		Names: | osmId | num. keys | num. values | num. rels |          encodedTags          |       nodes       |       rels      |
		Bytes: |   8   |     2     |      2      |     2     | key (32 bit) | value (32 bit) | <num. nodes> * 16 | <num. rels> * 8 |

		Tags are stored as a list of "num. tags" many key-value-pairs.

		The nodes section contains all nodes, not only the ones within this cell. This enables geometric checks, even
		in cases where no way-node is within this cell. The nodes are stores in the following way:
		<id (64-bit)><lon (32-bit)><lat (23-bit)>
	*/

	keys := encodedFeature.GetKeys()
	values := encodedFeature.GetValues()
	if len(keys) != len(values) {
		return errors.Errorf("Number of keys and values for node %d different: keys %d, values %d", encodedFeature.GetID(), len(keys), len(values))
	}
	numberOfTags := len(keys)

	nodeIdBytes := len(encodedFeature.GetNodes()) * 16          // Each ID is a 64-bit int + 2*4 bytes for lat/lon
	relationIdBytes := len(encodedFeature.GetRelationIds()) * 8 // Each ID is a 64-bit int

	headerByteCount := 8 + 2 + 2 + 2 // = 14
	byteCount := headerByteCount
	byteCount += numberOfTags * 4
	byteCount += numberOfTags * 4
	byteCount += nodeIdBytes
	byteCount += relationIdBytes

	ensureDataSliceSize(byteCount)

	/*
		Write header
	*/
	binary.LittleEndian.PutUint64(data[0:], encodedFeature.GetID())
	binary.LittleEndian.PutUint16(data[8:], uint16(numberOfTags))
	binary.LittleEndian.PutUint16(data[10:], uint16(len(encodedFeature.GetNodes())))
	binary.LittleEndian.PutUint16(data[12:], uint16(len(encodedFeature.GetRelationIds())))

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
	for _, node := range encodedFeature.GetNodes() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(node.ID))
		binary.LittleEndian.PutUint32(data[pos+8:], math.Float32bits(float32(node.Lon)))
		binary.LittleEndian.PutUint32(data[pos+12:], math.Float32bits(float32(node.Lat)))
		pos += 16
	}

	/*
		Write relation-IDs
	*/
	for _, relationId := range encodedFeature.GetRelationIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	return g.writeData(encodedFeature, data[0:byteCount], f)
}

func (g *GridIndexWriter) writeRelationData(encodedFeature feature.RelationFeature, f io.Writer) error {
	/*
		Entry format:

		Names: | osmId | bbox | num. keys | num. nodes | num. ways | num. child rels | num. parent rels |          encodedTags          |     node IDs     |     way IDs     |    child rel. IDs     |    parent rel. IDs     |
		Bytes: |   8   |  16  |     2     |      2     |     2     |        2        |         2        | key (32 bit) | value (32 bit) | <num. nodes> * 8 | <num. ways> * 8 | <num. child rels> * 8 | <num. parent rels> * 8 |

		Tags are stored as a list of "num. tags" many key-value-pairs.

		The "bbox" field are 4 32-bit floats for the min-lon, min-lat, max-lon and max-lat values.

		// TODO store real geometry. Including geometry of sub-relations?
	*/

	keys := encodedFeature.GetKeys()
	values := encodedFeature.GetValues()
	if len(keys) != len(values) {
		return errors.Errorf("Number of keys and values for node %d different: keys %d, values %d", encodedFeature.GetID(), len(keys), len(values))
	}
	numberOfTags := len(keys)

	nodeIdBytes := len(encodedFeature.GetNodeIds()) * 8                     // IDs are all 64-bit integers
	wayIdBytes := len(encodedFeature.GetWayIds()) * 8                       // IDs are all 64-bit integers
	childRelationIdBytes := len(encodedFeature.GetChildRelationIds()) * 8   // IDs are all 64-bit integers
	parentRelationIdBytes := len(encodedFeature.GetParentRelationIds()) * 8 // IDs are all 64-bit integers

	headerBytesCount := 8 + 16 + 2 + 2 + 2 + 2 + 2 // = 34
	byteCount := headerBytesCount
	byteCount += numberOfTags * 4
	byteCount += numberOfTags * 4
	byteCount += nodeIdBytes
	byteCount += wayIdBytes
	byteCount += childRelationIdBytes
	byteCount += parentRelationIdBytes

	ensureDataSliceSize(byteCount)

	bbox := encodedFeature.GetGeometry().Bound()

	binary.LittleEndian.PutUint64(data[0:], encodedFeature.GetID())
	binary.LittleEndian.PutUint32(data[8:], math.Float32bits(float32(bbox.Min.Lon())))
	binary.LittleEndian.PutUint32(data[12:], math.Float32bits(float32(bbox.Min.Lat())))
	binary.LittleEndian.PutUint32(data[16:], math.Float32bits(float32(bbox.Max.Lon())))
	binary.LittleEndian.PutUint32(data[20:], math.Float32bits(float32(bbox.Max.Lat())))
	binary.LittleEndian.PutUint16(data[24:], uint16(numberOfTags))
	binary.LittleEndian.PutUint16(data[16:], uint16(len(encodedFeature.GetNodeIds())))
	binary.LittleEndian.PutUint16(data[28:], uint16(len(encodedFeature.GetWayIds())))
	binary.LittleEndian.PutUint16(data[30:], uint16(len(encodedFeature.GetChildRelationIds())))
	binary.LittleEndian.PutUint16(data[32:], uint16(len(encodedFeature.GetParentRelationIds())))

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
	for _, nodeId := range encodedFeature.GetNodeIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(nodeId))
		pos += 8
	}

	/*
		Write way-IDs
	*/
	for _, wayId := range encodedFeature.GetWayIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(wayId))
		pos += 8
	}

	/*
		Write child relation-IDs
	*/
	for _, relationId := range encodedFeature.GetChildRelationIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	/*
		Write parent relation-IDs
	*/
	for _, relationId := range encodedFeature.GetParentRelationIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	return g.writeData(encodedFeature, data[0:byteCount], f)
}

func (g *GridIndexWriter) writeData(encodedFeature feature.Feature, data []byte, f io.Writer) error {
	g.cacheFileMutex.Lock()
	m := g.cacheFileMutexes[f]
	g.cacheFileMutex.Unlock()
	m.Lock()
	_, err := f.Write(data)
	m.Unlock()
	if err != nil {
		return errors.Wrapf(err, "Unable to write %s %d to cell file", reflect.TypeOf(encodedFeature).Name(), encodedFeature.GetID())
	}
	return nil
}

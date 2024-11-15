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
	"soq/feature"
	"strconv"
	"sync"
	"time"
)

type GridIndexWriter struct {
	baseGridIndex

	cacheFileHandles            map[int64]*[3]*os.File      // Key is a aggregation of the cells x and y coordinate. The array index is based on the object type.
	cacheFileWriters            map[int64]*[3]*bufio.Writer // Key is a aggregation of the cells x and y coordinate The array index is based on the object type.
	cacheFileMutexes            map[io.Writer]*sync.Mutex
	cacheFileMutex              *sync.Mutex
	cacheRawEncodedNodes        map[CellIndex][]*feature.EncodedNodeFeature
	cacheRawEncodedWays         map[CellIndex][]*feature.EncodedWayFeature
	cacheRawEncodedRelations    map[CellIndex][]*feature.EncodedRelationFeature
	cacheRawEncodedFeatureMutex *sync.Mutex

	// During writing, some of the half-written data must be read again. This requires some functionality of the
	// GridIndexReader during importing data and writing a new index.
	gridIndexReader *GridIndexReader
}

type scannerFactoryFunc func() (osm.Scanner, error)

func ImportDataFile(tagIndex *TagIndex, tempRawFeatureChannel chan feature.EncodedFeature, baseFolder string, cellWidth float64, cellHeight float64, cellExtent CellExtent) error {
	gridIndexWriter := NewGridIndexWriter(tagIndex, cellWidth, cellHeight, baseFolder)

	sigolo.Debug("Read OSM data and write them as raw encoded features")

	nodeCells, err := gridIndexWriter.WriteOsmToRawEncodedFeatures(tempRawFeatureChannel, cellExtent)
	if err != nil {
		return err
	}

	gridIndexWriter.addAdditionalIdsToObjectsInCells(nodeCells)

	return nil
}

func NewGridIndexWriter(tagIndex *TagIndex, cellWidth float64, cellHeight float64, baseFolder string) *GridIndexWriter {
	baseGridIndex := baseGridIndex{
		TagIndex:   tagIndex,
		CellWidth:  cellWidth,
		CellHeight: cellHeight,
		BaseFolder: baseFolder,
	}
	gridIndexWriter := &GridIndexWriter{
		baseGridIndex:               baseGridIndex,
		cacheFileHandles:            map[int64]*[3]*os.File{},
		cacheFileWriters:            map[int64]*[3]*bufio.Writer{},
		cacheFileMutexes:            map[io.Writer]*sync.Mutex{},
		cacheFileMutex:              &sync.Mutex{},
		cacheRawEncodedNodes:        map[CellIndex][]*feature.EncodedNodeFeature{},
		cacheRawEncodedWays:         map[CellIndex][]*feature.EncodedWayFeature{},
		cacheRawEncodedRelations:    map[CellIndex][]*feature.EncodedRelationFeature{},
		cacheRawEncodedFeatureMutex: &sync.Mutex{},
		gridIndexReader: &GridIndexReader{
			baseGridIndex:        baseGridIndex,
			checkFeatureValidity: false,
		},
	}
	return gridIndexWriter
}

// WriteOsmToRawEncodedFeatures Reads the input feature channel and converts all OSM objects into raw encoded features and
// writes them into their respective cells. The returned cell map contains all cells that contain nodes.
func (g *GridIndexWriter) WriteOsmToRawEncodedFeatures(tempRawFeatureChannel chan feature.EncodedFeature, cellExtent CellExtent) (map[CellIndex]CellIndex, error) {
	sigolo.Info("Start converting OSM data to raw encoded features")
	importStartTime := time.Now()

	nodeCells := map[CellIndex]CellIndex{}

	// We assume relations, like all other object types, to be sorted in a way that when a relation with child relations
	// appears, all child relation members have been visited before. Therefore, this map then contains all bounds of the
	// child relation members. Cyclic relation structures (when A is child of B but B also of A) are not supported.
	relationToBound := map[osm.RelationID]*orb.Bound{}

	firstWayHasBeenProcessed := false
	firstRelationHasBeenProcessed := false

	nodeToCellMap := map[osm.NodeID]CellIndex{}
	wayToCellsMap := map[osm.WayID][]CellIndex{}
	relationToCellsMap := map[osm.RelationID][]CellIndex{}

	nodeToBound := map[osm.NodeID]*orb.Bound{}

	wayToBound := map[osm.WayID]*orb.Bound{}

	sigolo.Debug("Process nodes (1/3)")
	for obj := range tempRawFeatureChannel {
		switch rawFeature := obj.(type) {
		case *feature.EncodedNodeFeature:
			cell := g.GetCellIndexForCoordinate(rawFeature.GetLon(), rawFeature.GetLat())
			// TODO Not needed anymore for nodes and ways? Because reading the data already filters it.
			if !cellExtent.contains(cell) {
				continue
			}

			id := osm.NodeID(rawFeature.GetID())
			nodeToCellMap[id] = cell

			bbox := rawFeature.GetGeometry().Bound()
			nodeToBound[id] = &bbox

			err := g.writeOsmObjectToCellCache(cell, rawFeature)
			sigolo.FatalCheck(err)

			nodeCells[cell] = cell
		case *feature.EncodedWayFeature:
			if !firstWayHasBeenProcessed {
				sigolo.Debug("Start processing ways (2/3)")
				firstWayHasBeenProcessed = true
			}

			extentContainsWay := false
			for _, node := range rawFeature.Nodes {
				extentContainsWay = extentContainsWay || cellExtent.contains(nodeToCellMap[node.ID])
				if extentContainsWay {
					break
				}
			}
			if !extentContainsWay {
				continue
			}

			wayCells := map[CellIndex]CellIndex{}
			for _, nodeId := range rawFeature.Nodes.NodeIDs() {
				cell := nodeToCellMap[nodeId]
				wayCells[cell] = cell
			}

			id := osm.WayID(rawFeature.GetID())
			wayToCellsMap[id] = make([]CellIndex, len(wayCells))
			j := 0
			for _, cell := range wayCells {
				wayToCellsMap[id][j] = cell
				j++
			}

			bbox := rawFeature.GetGeometry().Bound()
			wayToBound[id] = &bbox

			savedCells := map[CellIndex]bool{}
			for _, node := range rawFeature.Nodes {
				cell := g.GetCellIndexForCoordinate(node.Lon, node.Lat)

				if _, cellAlreadySaved := savedCells[cell]; !cellAlreadySaved && cellExtent.contains(cell) {
					err := g.writeOsmObjectToCellCache(cell, rawFeature)
					sigolo.FatalCheck(err)
					savedCells[cell] = true
				}
			}
		case *feature.EncodedRelationFeature:
			if !firstRelationHasBeenProcessed {
				sigolo.Debug("Start processing relations (3/3)")
				firstRelationHasBeenProcessed = true
			}

			extentContainsRelation := false
			for _, nodeId := range rawFeature.NodeIds {
				extentContainsRelation = extentContainsRelation || cellExtent.contains(nodeToCellMap[nodeId])
				if extentContainsRelation {
					break
				}
			}
			if !extentContainsRelation {
				for _, wayId := range rawFeature.WayIds {
					extentContainsRelation = extentContainsRelation || cellExtent.containsAny(wayToCellsMap[wayId])
					if extentContainsRelation {
						break
					}
				}
			}
			if !extentContainsRelation {
				for _, relationId := range rawFeature.ChildRelationIds {
					extentContainsRelation = extentContainsRelation || cellExtent.containsAny(relationToCellsMap[relationId])
					if extentContainsRelation {
						break
					}
				}
			}
			if !extentContainsRelation {
				continue
			}

			relCells := map[CellIndex]CellIndex{}

			var bbox *orb.Bound

			for _, nodeId := range rawFeature.NodeIds {
				cell := nodeToCellMap[nodeId]
				relCells[cell] = cell
				if bound, ok := nodeToBound[nodeId]; ok {
					if bbox == nil {
						bbox = bound
					} else {
						b := bbox.Union(*bound)
						bbox = &b
					}
				}
			}
			for _, wayId := range rawFeature.WayIds {
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
			for _, relationId := range rawFeature.ChildRelationIds {
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

			rawFeature.Geometry = bbox.ToPolygon()

			for _, cell := range relCells {
				err := g.writeOsmObjectToCellCache(cell, rawFeature)
				sigolo.FatalCheck(err)
			}
		}
	}

	importDuration := time.Since(importStartTime)
	sigolo.Infof("Created raw encoded features from OSM data in %s", importDuration)

	g.closeOpenFileHandles()

	return nodeCells, nil
}

func (g *GridIndexWriter) closeOpenFileHandles() {
	var err error
	g.cacheFileMutex.Lock()
	sigolo.Debugf("Close remaining open file handles")
	for mapKey, fileHandles := range g.cacheFileHandles {
		for writerIndex, writer := range g.cacheFileWriters[mapKey] {
			if writer != nil {
				err = writer.Flush()
				sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file writer for grid-index store %s", fileHandles[writerIndex].Name()))
			}
		}

		for _, fileHandle := range fileHandles {
			if fileHandle != nil {
				err = fileHandle.Close()
				sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for grid-index store %s", fileHandle.Name()))
			}
		}
	}
	g.cacheFileHandles = map[int64]*[3]*os.File{}
	g.cacheFileWriters = map[int64]*[3]*bufio.Writer{}
	g.cacheFileMutexes = map[io.Writer]*sync.Mutex{}
	g.cacheFileMutex.Unlock()
}

func (g *GridIndexWriter) addAdditionalIdsToObjectsInCells(cells map[CellIndex]CellIndex) {
	numberOfCells := len(cells)
	sigolo.Infof("Start adding way and relation IDs to raw encoded nodes in %d cells", numberOfCells)

	importStartTime := time.Now()

	currentCell := 1

	numThreads := 1 // TODO would otherwise cause "concurrent map read and map write" error for maybe objectTypeToRelationMapping in addAdditionalIdsToObjectsOfType
	cellQueue := make(chan CellIndex, numThreads*2)
	cellSync := &sync.WaitGroup{}
	for i := 0; i < numThreads; i++ {
		cellSync.Add(1)
		go g.addAdditionalIdsToObjectsInCell(cellQueue, cellSync)
	}

	for cell, _ := range cells {
		sigolo.Tracef("Add cell %v to queue (%d/%d)", cell, currentCell, numberOfCells)
		currentCell++

		cellQueue <- cell
	}

	close(cellQueue)
	cellSync.Wait()

	importDuration := time.Since(importStartTime)
	sigolo.Infof("Done adding way IDs to raw encoded nodes in %s", importDuration)

	g.closeOpenFileHandles()
}

func (g *GridIndexWriter) addAdditionalIdsToObjectsInCell(cellChannel chan CellIndex, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	for cell := range cellChannel {
		sigolo.Tracef("[Cell %v] Collect relationships between nodes, ways and relations", cell)

		//nodeToRelations, waysToRelations, relationsToParentRelations, err := g.gridIndexReader.readObjectsToRelationMappingFromCellData(cell.X(), cell.Y())
		//if err != nil {
		//	sigolo.Errorf("Error reading objects to relations mapping: %+v", err)
		//	// TODO return error
		//}

		nodeToRelations := make(map[uint64][]osm.RelationID)
		waysToRelations := make(map[uint64][]osm.RelationID)
		relationsToParentRelations := make(map[uint64][]osm.RelationID)

		for _, relation := range g.cacheRawEncodedRelations[cell] {
			for _, nodeId := range relation.NodeIds {
				nodeToRelations[uint64(nodeId)] = append(nodeToRelations[uint64(nodeId)], osm.RelationID(relation.ID))
			}
			for _, wayId := range relation.WayIds {
				waysToRelations[uint64(wayId)] = append(nodeToRelations[uint64(wayId)], osm.RelationID(relation.ID))
			}
			for _, relId := range relation.ChildRelationIds {
				relationsToParentRelations[uint64(relId)] = append(nodeToRelations[uint64(relId)], osm.RelationID(relation.ID))
			}
		}

		err := g.addAdditionalIdsToObjectsOfType(feature.OsmObjNode, nodeToRelations, cell)
		if err != nil {
			sigolo.Errorf("Error adding additional IDs to nodes: %+v", err)
			// TODO return error
		}

		err = g.addAdditionalIdsToObjectsOfType(feature.OsmObjWay, waysToRelations, cell)
		if err != nil {
			sigolo.Errorf("Error adding additional IDs to ways: %+v", err)
			// TODO return error
		}

		err = g.addAdditionalIdsToObjectsOfType(feature.OsmObjRelation, relationsToParentRelations, cell)
		if err != nil {
			sigolo.Errorf("Error adding additional IDs to relations: %+v", err)
			// TODO return error
		}
	}
}

// addAdditionalIdsToObjectsOfType adds the reverse IDs to the given object type. For example nodes themselves do not
// contain IDs to the ways they belong to. So this function adds this information to the given object type. In case of
// relations, this is done using the given object to relation map. This map maps an ID of the given object type to the
// relations this object is part of.
func (g *GridIndexWriter) addAdditionalIdsToObjectsOfType(objectType feature.OsmObjectType, objectTypeToRelationMapping map[uint64][]osm.RelationID, cell CellIndex) error {
	var err error

	nodeToWays := map[uint64][]osm.WayID{}
	for _, way := range g.cacheRawEncodedWays[cell] {
		for _, nodeId := range way.Nodes.NodeIDs() {
			nodeToWays[uint64(nodeId)] = append(nodeToWays[uint64(nodeId)], osm.WayID(way.ID))
		}
	}

	//cellFolderName := path.Join(g.BaseFolder, objectType.String(), strconv.Itoa(cell.X()))
	//cellFileName := path.Join(cellFolderName, strconv.Itoa(cell.Y())+".cell")

	//if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
	//	if objectType == feature.OsmObjNode {
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
	//readFeatureChannel := make(chan []feature.EncodedFeature)
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
	case feature.OsmObjNode:
		for _, encFeature := range g.cacheRawEncodedNodes[cell] {
			if wayIds, ok := nodeToWays[encFeature.GetID()]; ok {
				encFeature.WayIds = wayIds
			}
			if relationIds, ok := objectTypeToRelationMapping[encFeature.GetID()]; ok {
				encFeature.RelationIds = relationIds
			}

			err = g.writeOsmObjectToCell(cell.X(), cell.Y(), encFeature)
			sigolo.FatalCheck(err)
		}
		delete(g.cacheRawEncodedNodes, cell)
	case feature.OsmObjWay:
		for _, encFeature := range g.cacheRawEncodedWays[cell] {
			if relationIds, ok := objectTypeToRelationMapping[encFeature.GetID()]; ok {
				encFeature.RelationIds = relationIds
			}

			err = g.writeOsmObjectToCell(cell.X(), cell.Y(), encFeature)
			sigolo.FatalCheck(err)
		}
		delete(g.cacheRawEncodedWays, cell)
	case feature.OsmObjRelation:
		for _, encFeature := range g.cacheRawEncodedRelations[cell] {
			if relationIds, ok := objectTypeToRelationMapping[encFeature.GetID()]; ok {
				encFeature.ParentRelationIds = relationIds
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

func (g *GridIndexWriter) writeOsmObjectToCell(cellX int, cellY int, encodedFeature feature.EncodedFeature) error {
	switch featureObj := encodedFeature.(type) {
	case *feature.EncodedNodeFeature:
		f, err := g.getCellFile(cellX, cellY, feature.OsmObjNode)
		if err != nil {
			return err
		}
		err = g.writeNodeData(featureObj, f)
		if err != nil {
			return errors.Wrapf(err, "Unable to write node %d to cell x=%d, y=%d", encodedFeature.GetID(), cellX, cellY)
		}
		return nil
	case *feature.EncodedWayFeature:
		f, err := g.getCellFile(cellX, cellY, feature.OsmObjWay)
		if err != nil {
			return err
		}
		err = g.writeWayData(featureObj, f)
		if err != nil {
			return errors.Wrapf(err, "Unable to write way %d to cell x=%d, y=%d", encodedFeature.GetID(), cellX, cellY)
		}
		return nil
	case *feature.EncodedRelationFeature:
		f, err := g.getCellFile(cellX, cellY, feature.OsmObjRelation)
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

func (g *GridIndexWriter) writeOsmObjectToCellCache(cell CellIndex, encodedFeature feature.EncodedFeature) error {
	g.cacheRawEncodedFeatureMutex.Lock()
	switch featureObj := encodedFeature.(type) {
	case *feature.EncodedNodeFeature:
		g.cacheRawEncodedNodes[cell] = append(g.cacheRawEncodedNodes[cell], featureObj)
	case *feature.EncodedWayFeature:
		g.cacheRawEncodedWays[cell] = append(g.cacheRawEncodedWays[cell], featureObj)
	case *feature.EncodedRelationFeature:
		g.cacheRawEncodedRelations[cell] = append(g.cacheRawEncodedRelations[cell], featureObj)
	}
	g.cacheRawEncodedFeatureMutex.Unlock()

	return nil
}

func (g *GridIndexWriter) getCellFile(cellX int, cellY int, objectType feature.OsmObjectType) (io.Writer, error) {
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
		g.cacheFileHandles[cellPositionKey] = &[3]*os.File{}
		writers = g.cacheFileWriters[cellPositionKey]
	}

	writer := bufio.NewWriter(file)
	writers[writersIndex] = writer

	openFileHandleForThisCell := g.cacheFileHandles[cellPositionKey]
	openFileHandleForThisCell[writersIndex] = file

	g.cacheFileMutexes[writer] = &sync.Mutex{}

	g.cacheFileMutex.Unlock()

	return writer, nil
}

func (g *GridIndexWriter) getMapKeyForCell(cellX int, cellY int) int64 {
	return int64(cellX)<<32 | int64(cellY)
}

func (g *GridIndexWriter) getWriterIndex(objectType feature.OsmObjectType) int {
	writersIndex := -1
	if objectType == feature.OsmObjNode {
		writersIndex = 0
	} else if objectType == feature.OsmObjWay {
		writersIndex = 1
	} else {
		writersIndex = 2
	}
	return writersIndex
}

func (g *GridIndexWriter) writeNodeData(encodedFeature *feature.EncodedNodeFeature, f io.Writer) error {
	/*
		Entry format:
		// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes).

		Names: | osmId | lon | lat | num. keys | num. values | num. ways | num. rels |   encodedKeys   |   encodedValues   |     way IDs     |   relation IDs  |
		Bytes: |   8   |  4  |  4  |     4     |      4      |     2     |     2     | <num. keys> / 8 | <num. values> * 3 | <num. ways> * 8 | <num. rels> * 8 |

		The encodedKeys is a bit-string (each key 1 bit), that why the division by 8 happens. The stored value is the
		number of bytes in the keys array of the feature (i.e. "len(encodedFeature.GetKeys())"). The encodedValue part, however,
		is an int-array, therefore, we need the multiplication with 4.
	*/

	// The number of key-bins to store is determined by the bin with the highest index that is not empty (i.e. all 0s).
	// If only the first bin contains some 1s (i.e. keys that are set on the feature) and the next 100 bins are empty,
	// then there's no reason to store those empty bins. This reduced the cell-file size for hamburg-latest (45 MB PBF)
	// by a factor of ten!
	numKeys := 0
	for i := 0; i < len(encodedFeature.GetKeys()); i++ {
		if encodedFeature.GetKeys()[i] != 0 {
			numKeys = i + 1
		}
	}

	encodedKeyBytes := numKeys                               // Is already a byte-array -> no division by 8 needed
	encodedValueBytes := len(encodedFeature.GetValues()) * 3 // Int array and int = 4 bytes
	wayIdBytes := len(encodedFeature.WayIds) * 8             // IDs are all 64-bit integers
	relationIdBytes := len(encodedFeature.RelationIds) * 8   // IDs are all 64-bit integers

	headerBytesCount := 8 + 4 + 4 + 4 + 4 + 2 + 2 // = 28
	byteCount := headerBytesCount
	byteCount += encodedKeyBytes
	byteCount += encodedValueBytes
	byteCount += wayIdBytes
	byteCount += relationIdBytes

	data := make([]byte, byteCount)

	point := encodedFeature.Geometry.(*orb.Point)

	binary.LittleEndian.PutUint64(data[0:], encodedFeature.ID)
	binary.LittleEndian.PutUint32(data[8:], math.Float32bits(float32(point.Lon())))
	binary.LittleEndian.PutUint32(data[12:], math.Float32bits(float32(point.Lat())))
	binary.LittleEndian.PutUint32(data[16:], uint32(numKeys))
	binary.LittleEndian.PutUint32(data[20:], uint32(len(encodedFeature.GetValues())))
	binary.LittleEndian.PutUint16(data[24:], uint16(len(encodedFeature.WayIds)))
	binary.LittleEndian.PutUint16(data[26:], uint16(len(encodedFeature.RelationIds)))

	pos := headerBytesCount

	/*
		Write keys
	*/
	copy(data[pos:], encodedFeature.GetKeys()[0:numKeys])
	pos += numKeys

	/*
		Write values
	*/
	for _, v := range encodedFeature.GetValues() {
		data[pos] = byte(v)
		data[pos+1] = byte(v >> 8)
		data[pos+2] = byte(v >> 16)
		pos += 3
	}

	/*
		Write way-IDs
	*/
	for _, wayId := range encodedFeature.WayIds {
		binary.LittleEndian.PutUint64(data[pos:], uint64(wayId))
		pos += 8
	}

	/*
		Write relation-IDs
	*/
	for _, relationId := range encodedFeature.RelationIds {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	return g.writeData(encodedFeature, data, f)
}

func (g *GridIndexWriter) writeWayData(encodedFeature *feature.EncodedWayFeature, f io.Writer) error {
	/*
		Entry format:
		// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes).

		Names: | osmId | num. keys | num. values | num. nodes | num. rels |   encodedKeys   |   encodedValues   |       nodes       |       rels      |
		Bytes: |   8   |     4     |      4      |      2     |     2     | <num. keys> / 8 | <num. values> * 3 | <num. nodes> * 16 | <num. rels> * 8 |

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
	for i := 0; i < len(encodedFeature.GetKeys()); i++ {
		if encodedFeature.GetKeys()[i] != 0 {
			numEncodedKeyBytes = i + 1
		}
	}

	numEncodedValueBytes := len(encodedFeature.GetValues()) * 3 // Int array and int = 4 bytes
	nodeIdBytes := len(encodedFeature.Nodes) * 16               // Each ID is a 64-bit int + 2*4 bytes for lat/lon
	relationIdBytes := len(encodedFeature.RelationIds) * 8      // Each ID is a 64-bit int

	headerByteCount := 8 + 4 + 4 + 2 + 2
	byteCount := headerByteCount
	byteCount += numEncodedKeyBytes
	byteCount += numEncodedValueBytes
	byteCount += nodeIdBytes
	byteCount += relationIdBytes

	data := make([]byte, byteCount)

	/*
		Write header
	*/
	binary.LittleEndian.PutUint64(data[0:], encodedFeature.ID)
	binary.LittleEndian.PutUint32(data[8:], uint32(numEncodedKeyBytes))
	binary.LittleEndian.PutUint32(data[12:], uint32(len(encodedFeature.GetValues())))
	binary.LittleEndian.PutUint16(data[16:], uint16(len(encodedFeature.Nodes)))
	binary.LittleEndian.PutUint16(data[18:], uint16(len(encodedFeature.RelationIds)))

	pos := headerByteCount

	/*
		Write keys
	*/
	copy(data[pos:], encodedFeature.GetKeys()[0:numEncodedKeyBytes])
	pos += numEncodedKeyBytes

	/*
		Write value
	*/
	for _, v := range encodedFeature.GetValues() {
		data[pos] = byte(v)
		data[pos+1] = byte(v >> 8)
		data[pos+2] = byte(v >> 16)
		pos += 3
	}

	/*
		Write nodes
	*/
	for _, node := range encodedFeature.Nodes {
		binary.LittleEndian.PutUint64(data[pos:], uint64(node.ID))
		binary.LittleEndian.PutUint32(data[pos+8:], math.Float32bits(float32(node.Lon)))
		binary.LittleEndian.PutUint32(data[pos+12:], math.Float32bits(float32(node.Lat)))
		pos += 16
	}

	/*
		Write relation-IDs
	*/
	for _, relationId := range encodedFeature.RelationIds {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	return g.writeData(encodedFeature, data, f)
}

func (g *GridIndexWriter) writeRelationData(encodedFeature *feature.EncodedRelationFeature, f io.Writer) error {
	/*
		Entry format:
		// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes).

		Names: | osmId | bbox | num. keys | num. values | num. nodes | num. ways | num. child rels | num. parent rels |   encodedKeys   |   encodedValues   |     node IDs     |     way IDs     |    child rel. IDs     |    parent rel. IDs     |
		Bytes: |   8   |  16  |     4     |      4      |      2     |     2     |        2        |         2        | <num. keys> / 8 | <num. values> * 3 | <num. nodes> * 8 | <num. ways> * 8 | <num. child rels> * 8 | <num. parent rels> * 8 |

		The encodedKeys is a bit-string (each key 1 bit), that why the division by 8 happens. The stored value is the
		number of bytes in the keys array of the feature (i.e. "len(encodedFeature.GetKeys())"). The encodedValue part, however,
		is an int-array, therefore, we need the multiplication with 4.

		The "bbox" field are 4 32-bit floats for the min-lon, min-lat, max-lon and max-lat values.

		// TODO store real geometry. Including geometry of sub-relations?
	*/
	numKeys := 0
	for i := 0; i < len(encodedFeature.GetKeys()); i++ {
		if encodedFeature.GetKeys()[i] != 0 {
			numKeys = i + 1
		}
	}

	encodedKeyBytes := numKeys                                         // Is already a byte-array -> no division by 8 needed
	encodedValueBytes := len(encodedFeature.GetValues()) * 3           // Int array and int = 4 bytes
	nodeIdBytes := len(encodedFeature.NodeIds) * 8                     // IDs are all 64-bit integers
	wayIdBytes := len(encodedFeature.WayIds) * 8                       // IDs are all 64-bit integers
	childRelationIdBytes := len(encodedFeature.ChildRelationIds) * 8   // IDs are all 64-bit integers
	parentRelationIdBytes := len(encodedFeature.ParentRelationIds) * 8 // IDs are all 64-bit integers

	headerBytesCount := 8 + 16 + 4 + 4 + 2 + 2 + 2 + 2 // = 40
	byteCount := headerBytesCount
	byteCount += encodedKeyBytes
	byteCount += encodedValueBytes
	byteCount += nodeIdBytes
	byteCount += wayIdBytes
	byteCount += childRelationIdBytes
	byteCount += parentRelationIdBytes

	data := make([]byte, byteCount)

	bbox := encodedFeature.Geometry.Bound()

	binary.LittleEndian.PutUint64(data[0:], encodedFeature.ID)
	binary.LittleEndian.PutUint32(data[8:], math.Float32bits(float32(bbox.Min.Lon())))
	binary.LittleEndian.PutUint32(data[12:], math.Float32bits(float32(bbox.Min.Lat())))
	binary.LittleEndian.PutUint32(data[16:], math.Float32bits(float32(bbox.Max.Lon())))
	binary.LittleEndian.PutUint32(data[20:], math.Float32bits(float32(bbox.Max.Lat())))
	binary.LittleEndian.PutUint32(data[24:], uint32(numKeys))
	binary.LittleEndian.PutUint32(data[28:], uint32(len(encodedFeature.GetValues())))
	binary.LittleEndian.PutUint16(data[32:], uint16(len(encodedFeature.NodeIds)))
	binary.LittleEndian.PutUint16(data[34:], uint16(len(encodedFeature.WayIds)))
	binary.LittleEndian.PutUint16(data[36:], uint16(len(encodedFeature.ChildRelationIds)))
	binary.LittleEndian.PutUint16(data[38:], uint16(len(encodedFeature.ParentRelationIds)))

	pos := headerBytesCount

	/*
		Write keys
	*/
	copy(data[pos:], encodedFeature.GetKeys()[0:numKeys])
	pos += numKeys

	/*
		Write values
	*/
	for _, v := range encodedFeature.GetValues() {
		data[pos] = byte(v)
		data[pos+1] = byte(v >> 8)
		data[pos+2] = byte(v >> 16)
		pos += 3
	}

	/*
		Write node-IDs
	*/
	for _, nodeId := range encodedFeature.NodeIds {
		binary.LittleEndian.PutUint64(data[pos:], uint64(nodeId))
		pos += 8
	}

	/*
		Write way-IDs
	*/
	for _, wayId := range encodedFeature.WayIds {
		binary.LittleEndian.PutUint64(data[pos:], uint64(wayId))
		pos += 8
	}

	/*
		Write child relation-IDs
	*/
	for _, relationId := range encodedFeature.ChildRelationIds {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	/*
		Write parent relation-IDs
	*/
	for _, relationId := range encodedFeature.ParentRelationIds {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	return g.writeData(encodedFeature, data, f)
}

func (g *GridIndexWriter) writeData(encodedFeature feature.EncodedFeature, data []byte, f io.Writer) error {
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

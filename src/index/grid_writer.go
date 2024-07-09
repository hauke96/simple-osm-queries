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
	"path"
	"reflect"
	"soq/feature"
	"soq/util"
	"strconv"
	"sync"
	"time"
)

func (g *GridIndex) createAndWriteRawWayFeature(osmWayChannel chan *osm.Way, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	tempEncodedValues := g.TagIndex.newTempEncodedValueArray()
	var emptyRelationIds []osm.RelationID

	for osmObj := range osmWayChannel {
		wayFeature, err := g.toEncodedWayFeature(osmObj, emptyRelationIds, tempEncodedValues)
		sigolo.FatalCheck(err)

		wayFeatureData := g.getWayData(wayFeature)

		savedCells := map[CellIndex]bool{}
		for _, node := range osmObj.Nodes {
			cell := g.GetCellIndexForCoordinate(node.Lon, node.Lat)

			if _, ok := savedCells[cell]; !ok {
				f, err := g.getCellFile(cell.X(), cell.Y(), feature.OsmObjWay.String())
				sigolo.FatalCheck(err)

				err = g.writeData(wayFeature, wayFeatureData, f)
				sigolo.FatalCheck(err)

				savedCells[cell] = true
			}
		}
	}
}

func (g *GridIndex) addAdditionalIdsToObjectsInCells(cells map[CellIndex]CellIndex) {
	sigolo.Info("Start adding way and relation IDs to raw encoded nodes")
	importStartTime := time.Now()

	currentCell := 1
	numberOfCells := len(cells)

	numThreads := 10
	cellQueue := make(chan CellIndex, numThreads*2)
	cellSync := &sync.WaitGroup{}
	for i := 0; i < numThreads; i++ {
		cellSync.Add(1)
		go g.addAdditionalIdsToObjectsInCell(cellQueue, cellSync)
	}

	for cell, _ := range cells {
		sigolo.Debugf("Add cell %v to queue (%d/%d)", cell, currentCell, numberOfCells)
		currentCell++

		cellQueue <- cell
	}

	close(cellQueue)
	cellSync.Wait()

	importDuration := time.Since(importStartTime)
	sigolo.Infof("Done adding way IDs to raw encoded nodes in %s", importDuration)
}

func (g *GridIndex) addAdditionalIdsToObjectsInCell(cellChannel chan CellIndex, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	for cell := range cellChannel {
		sigolo.Tracef("[Cell %v] Collect relationships between nodes, ways and relations", cell)

		err := g.addAdditionalIdsToObjectsOfType(feature.OsmObjNode, cell)
		if err != nil {
			sigolo.Errorf("Error adding additional IDs to nodes: %+v", err)
			// TODO return error
		}

		err = g.addAdditionalIdsToObjectsOfType(feature.OsmObjWay, cell)
		if err != nil {
			sigolo.Errorf("Error adding additional IDs to ways: %+v", err)
			// TODO return error
		}

		err = g.addAdditionalIdsToObjectsOfType(feature.OsmObjRelation, cell)
		if err != nil {
			sigolo.Errorf("Error adding additional IDs to relations: %+v", err)
			// TODO return error
		}
	}
}

// addAdditionalIdsToObjectsOfType reads the features for the given cell and object type and also the IDs of the objects
// that should be connected. Example: If the object type is "way" then the way-to-relation-mapping is determined from
// the relation cell to add the relation IDs to all ways in this cell.
func (g *GridIndex) addAdditionalIdsToObjectsOfType(objectType feature.OsmObjectType, cell CellIndex) error {
	var err error
	var nodeToWays map[uint64][]osm.WayID
	var nodeToRelations map[uint64][]osm.RelationID
	var wayToRelations map[uint64][]osm.RelationID
	var relationToParentRelations map[uint64][]osm.RelationID

	switch objectType {
	case feature.OsmObjNode:
		nodeToWays, err = g.readNodeToWayMappingFromCellData(cell.X(), cell.Y())
		nodeToRelations, err = g.readNodeToRelationMappingFromCellData(cell.X(), cell.Y())
	case feature.OsmObjWay:
		wayToRelations, err = g.readWayToRelationMappingFromCellData(cell.X(), cell.Y())
	case feature.OsmObjRelation:
		relationToParentRelations, err = g.readRelationToParentRelationMappingFromCellData(cell.X(), cell.Y())
	default:
		return errors.Errorf("Unsupported object type %v to add IDs to", objectType)
	}

	cellFolderName := path.Join(g.BaseFolder, objectType.String(), strconv.Itoa(cell.X()))
	cellFileName := path.Join(cellFolderName, strconv.Itoa(cell.Y())+".cell")

	if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
		if objectType == feature.OsmObjNode {
			// We got all the cells in which nodes are. When this cell doesn't exist, then something went really wrong.
			util.LogFatalBug("Cell file %s for nodes could not be found in the list of node-cells. This is a critical error and should not have happened", cellFileName)
		}
		// We found a cell only containing nodes. This might happen sometimes and is ok.
		sigolo.Debugf("Cell file %s for OSM object type %v does not exist, since it might only contain nodes", cellFileName, objectType)
		return nil
	}
	sigolo.FatalCheck(errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName))

	sigolo.Tracef("[Cell %v] Read cell file %s", cell, cellFileName)
	data, err := os.ReadFile(cellFileName)
	sigolo.FatalCheck(errors.Wrapf(err, "Unable to read %v-cell x=%d, y=%d", objectType, cell.X(), cell.Y()))

	sigolo.Tracef("[Cell %v] Read %v objects from cell and write them including additional IDs", cell, objectType)
	readFeatureChannel := make(chan []feature.EncodedFeature)
	var finishWaitGroup sync.WaitGroup
	finishWaitGroup.Add(1)

	go func() {
		for encFeatures := range readFeatureChannel {
			for _, encFeature := range encFeatures {
				if encFeature == nil {
					continue
				}

				switch objectType {
				case feature.OsmObjNode:
					if wayIds, ok := nodeToWays[encFeature.GetID()]; ok {
						encFeature.(*feature.EncodedNodeFeature).WayIds = wayIds
					}
					if relationIds, ok := nodeToRelations[encFeature.GetID()]; ok {
						encFeature.(*feature.EncodedNodeFeature).RelationIds = relationIds
					}
				case feature.OsmObjWay:
					if relationIds, ok := wayToRelations[encFeature.GetID()]; ok {
						encFeature.(*feature.EncodedWayFeature).RelationIds = relationIds
					}
				case feature.OsmObjRelation:
					if relationIds, ok := relationToParentRelations[encFeature.GetID()]; ok {
						encFeature.(*feature.EncodedRelationFeature).ParentRelationIDs = relationIds
					}
				default:
					sigolo.Fatalf("Unsupported object type %v to add IDs to", objectType)
					return
				}

				err = g.writeOsmObjectToCell(cell.X(), cell.Y(), encFeature)
				sigolo.FatalCheck(err)
			}
		}
		finishWaitGroup.Done()
	}()

	switch objectType {
	case feature.OsmObjNode:
		g.readNodesFromCellData(readFeatureChannel, data)
	case feature.OsmObjWay:
		g.readWaysFromCellData(readFeatureChannel, data)
	case feature.OsmObjRelation:
		g.readRelationsFromCellData(readFeatureChannel, data)
	default:
		return errors.Errorf("Unsupported object type %v to read data for", objectType)
	}

	close(readFeatureChannel)
	finishWaitGroup.Wait()

	return nil
}

func (g *GridIndex) writeOsmObjectToCell(cellX int, cellY int, encodedFeature feature.EncodedFeature) error {
	sigolo.Tracef("Write OSM object to cell x=%d, y=%d, obj=%#v", cellX, cellY, encodedFeature.GetID())

	switch featureObj := encodedFeature.(type) {
	case *feature.EncodedNodeFeature:
		f, err := g.getCellFile(cellX, cellY, feature.OsmObjNode.String())
		if err != nil {
			return err
		}
		err = g.writeNodeData(featureObj, f)
		if err != nil {
			return errors.Wrapf(err, "Unable to write node %d to cell x=%d, y=%d", encodedFeature.GetID(), cellX, cellY)
		}
		return nil
	case *feature.EncodedWayFeature:
		f, err := g.getCellFile(cellX, cellY, feature.OsmObjWay.String())
		if err != nil {
			return err
		}
		err = g.writeWayData(featureObj, f)
		if err != nil {
			return errors.Wrapf(err, "Unable to write way %d to cell x=%d, y=%d", encodedFeature.GetID(), cellX, cellY)
		}
		return nil
	case *feature.EncodedRelationFeature:
		f, err := g.getCellFile(cellX, cellY, feature.OsmObjRelation.String())
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

func (g *GridIndex) getCellFile(cellX int, cellY int, objectType string) (io.Writer, error) {
	// Not filepath.Join because in this case it's slower than simple concatenation
	cellFolderName := g.BaseFolder + "/" + objectType + "/" + strconv.Itoa(cellX)
	cellFileName := cellFolderName + "/" + strconv.Itoa(cellY) + ".cell"

	var writer *bufio.Writer
	var cached bool
	var err error

	g.cacheFileMutex.Lock()

	writer, cached = g.cacheFileWriters[cellFileName]
	if cached {
		g.cacheFileMutex.Unlock()
		sigolo.Tracef("Cell file %s already exist and cached", cellFileName)
		return writer, nil
	}

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

	writer = bufio.NewWriter(file) // 4MiB
	g.cacheFileWriters[cellFileName] = writer
	g.cacheFileHandles[cellFileName] = file
	g.cacheFileMutexes[writer] = &sync.Mutex{}

	g.cacheFileMutex.Unlock()

	return writer, nil
}

func (g *GridIndex) writeNodeData(encodedFeature *feature.EncodedNodeFeature, f io.Writer) error {
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

	sigolo.Tracef("Write feature %d pos=%#v, byteCount=%d, numKeys=%d, numValues=%d, numWays=%d, numRels=%d", encodedFeature.ID, point, byteCount, numKeys, len(encodedFeature.GetValues()), len(encodedFeature.WayIds), len(encodedFeature.RelationIds))

	return g.writeData(encodedFeature, data, f)
}

func (g *GridIndex) writeWayData(encodedFeature *feature.EncodedWayFeature, f io.Writer) error {
	data := g.getWayData(encodedFeature)

	sigolo.Tracef("Write feature %d byteCount=%d, numValues=%d, numNodeIds=%d", encodedFeature.ID, len(data), len(encodedFeature.GetValues()), len(encodedFeature.Nodes))

	return g.writeData(encodedFeature, data, f)
}

func (g *GridIndex) writeData(encodedFeature feature.EncodedFeature, data []byte, f io.Writer) error {
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

func (g *GridIndex) getWayData(encodedFeature *feature.EncodedWayFeature) []byte {
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

	return data
}

func (g *GridIndex) writeRelationData(encodedFeature *feature.EncodedRelationFeature, f io.Writer) error {
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
	nodeIdBytes := len(encodedFeature.NodeIDs) * 8                     // IDs are all 64-bit integers
	wayIdBytes := len(encodedFeature.WayIDs) * 8                       // IDs are all 64-bit integers
	childRelationIdBytes := len(encodedFeature.ChildRelationIDs) * 8   // IDs are all 64-bit integers
	parentRelationIdBytes := len(encodedFeature.ParentRelationIDs) * 8 // IDs are all 64-bit integers

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
	binary.LittleEndian.PutUint16(data[32:], uint16(len(encodedFeature.NodeIDs)))
	binary.LittleEndian.PutUint16(data[34:], uint16(len(encodedFeature.WayIDs)))
	binary.LittleEndian.PutUint16(data[36:], uint16(len(encodedFeature.ChildRelationIDs)))
	binary.LittleEndian.PutUint16(data[38:], uint16(len(encodedFeature.ParentRelationIDs)))

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
	for _, nodeId := range encodedFeature.NodeIDs {
		binary.LittleEndian.PutUint64(data[pos:], uint64(nodeId))
		pos += 8
	}

	/*
		Write way-IDs
	*/
	for _, wayId := range encodedFeature.WayIDs {
		binary.LittleEndian.PutUint64(data[pos:], uint64(wayId))
		pos += 8
	}

	/*
		Write child relation-IDs
	*/
	for _, relationId := range encodedFeature.ChildRelationIDs {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	/*
		Write parent relation-IDs
	*/
	for _, relationId := range encodedFeature.ParentRelationIDs {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	sigolo.Tracef("Write feature %d bbox=%#v, byteCount=%d, numKeys=%d, numValues=%d, numNodes=%d, numWays=%d, numChildRels=%d, numParentRels=%d", encodedFeature.ID, bbox, byteCount, numKeys, len(encodedFeature.GetValues()), len(encodedFeature.NodeIDs), len(encodedFeature.WayIDs), len(encodedFeature.ChildRelationIDs), len(encodedFeature.ParentRelationIDs))

	return g.writeData(encodedFeature, data, f)
}

func (g *GridIndex) toEncodedNodeFeature(obj *osm.Node, wayIds []osm.WayID, relationIds []osm.RelationID, tempEncodedValues []int) (*feature.EncodedNodeFeature, error) {
	var geometry orb.Geometry

	point := obj.Point()
	geometry = &point

	encodedKeys, encodedValues := g.TagIndex.encodeTags(obj.Tags, tempEncodedValues)

	abstractEncodedFeature := feature.AbstractEncodedFeature{
		ID:       uint64(obj.ID),
		Geometry: geometry,
		Keys:     encodedKeys,
		Values:   encodedValues,
	}

	return &feature.EncodedNodeFeature{
		AbstractEncodedFeature: abstractEncodedFeature,
		WayIds:                 wayIds,
		RelationIds:            relationIds,
	}, nil
}

func (g *GridIndex) toEncodedWayFeature(obj *osm.Way, relationIds []osm.RelationID, tempEncodedValues []int) (*feature.EncodedWayFeature, error) {
	var geometry orb.Geometry
	var osmId uint64

	lineString := obj.LineString()
	geometry = &lineString
	osmId = uint64(obj.ID)

	encodedKeys, encodedValues := g.TagIndex.encodeTags(obj.Tags, tempEncodedValues)

	abstractEncodedFeature := feature.AbstractEncodedFeature{
		ID:       osmId,
		Geometry: geometry,
		Keys:     encodedKeys,
		Values:   encodedValues,
	}

	return &feature.EncodedWayFeature{
		AbstractEncodedFeature: abstractEncodedFeature,
		Nodes:                  obj.Nodes,
		RelationIds:            relationIds,
	}, nil
}

func (g *GridIndex) toEncodedRelationFeature(obj *osm.Relation, bbox *orb.Bound, nodeIds []osm.NodeID, wayIds []osm.WayID, childRelationIds []osm.RelationID, tempEncodedValues []int) (*feature.EncodedRelationFeature, error) {
	// This is probably temporary until the real geometry collection is stored
	geometry := bbox.ToPolygon()
	osmId := uint64(obj.ID)

	encodedKeys, encodedValues := g.TagIndex.encodeTags(obj.Tags, tempEncodedValues)

	abstractEncodedFeature := feature.AbstractEncodedFeature{
		ID:       osmId,
		Geometry: geometry,
		Keys:     encodedKeys,
		Values:   encodedValues,
	}

	return &feature.EncodedRelationFeature{
		AbstractEncodedFeature: abstractEncodedFeature,
		NodeIDs:                nodeIds,
		WayIDs:                 wayIds,
		ChildRelationIDs:       childRelationIds,
		ParentRelationIDs:      []osm.RelationID{},
	}, nil
}

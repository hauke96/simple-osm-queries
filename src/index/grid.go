package index

import (
	"bufio"
	"context"
	"encoding/binary"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
	"github.com/paulmach/osm/osmxml"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
	"path"
	"soq/feature"
	"strconv"
	"strings"
	"sync"
	"time"
)

const GridIndexFolder = "grid-index"

type GridIndex struct {
	TagIndex             *TagIndex
	CellWidth            float64
	CellHeight           float64
	BaseFolder           string
	cacheFileHandles     map[string]*os.File
	cacheFileWriters     map[string]*bufio.Writer
	cacheFileMutex       *sync.Mutex
	checkFeatureValidity bool
	nodeToPositionMap    map[osm.NodeID]orb.Point
	nodeToWayMap         map[osm.NodeID]osm.Ways
	featureCache         map[string][]feature.EncodedFeature // Filename to feature within it
	featureCacheMutex    *sync.Mutex
}

type CellIndex [2]int

func (c CellIndex) X() int { return c[0] }

func (c CellIndex) Y() int { return c[1] }

func LoadGridIndex(indexBaseFolder string, cellWidth float64, cellHeight float64, checkFeatureValidity bool, tagIndex *TagIndex) *GridIndex {
	return &GridIndex{
		TagIndex:             tagIndex,
		CellWidth:            cellWidth,
		CellHeight:           cellHeight,
		BaseFolder:           path.Join(indexBaseFolder, GridIndexFolder),
		checkFeatureValidity: checkFeatureValidity,
		featureCache:         map[string][]feature.EncodedFeature{},
		featureCacheMutex:    &sync.Mutex{},
		cacheFileMutex:       &sync.Mutex{},
	}
}

func (g *GridIndex) Import(inputFile string) error {
	err := os.RemoveAll(g.BaseFolder)
	if err != nil {
		return errors.Wrapf(err, "Unable to remove grid-index base folder %s", g.BaseFolder)
	}

	g.cacheFileHandles = map[string]*os.File{}
	g.cacheFileWriters = map[string]*bufio.Writer{}
	//g.nodeToPositionMap = map[osm.NodeID]orb.Point{}
	g.nodeToWayMap = map[osm.NodeID]osm.Ways{}
	g.featureCache = map[string][]feature.EncodedFeature{}
	g.featureCacheMutex = &sync.Mutex{}
	g.cacheFileMutex = &sync.Mutex{}

	time.Sleep(10 * time.Second)

	//amountOfObjects := 0
	//cellDataMap := map[CellIndex]osm.Objects{}

	sigolo.Infof("Start processing geometries from input file %s", inputFile)
	importStartTime := time.Now()

	cells := map[CellIndex]CellIndex{}

	sigolo.Debug("Read OSM data and write them as raw encoded features")

	err = g.convertOsmToRawEncodedFeatures(inputFile, cells)
	if err != nil {
		return err
	}
	//g.nodeToPositionMap = map[osm.NodeID]orb.Point{}

	//runtime.GC()

	g.cacheFileMutex.Lock()
	sigolo.Debugf("Close remaining open file handles")
	for filename, file := range g.cacheFileHandles {
		if file != nil {
			sigolo.Tracef("Close cell file %s", file.Name())

			writer := g.cacheFileWriters[filename]
			err = writer.Flush()
			sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file writer for grid-index store %s", file.Name()))

			err = file.Close()
			sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for grid-index store %s", file.Name()))
		} else {
			sigolo.Warnf("No cell file %s to close, there's probably an error previously when opening/creating it", filename)
		}
	}
	g.cacheFileHandles = map[string]*os.File{}
	g.cacheFileWriters = map[string]*bufio.Writer{}
	g.cacheFileMutex.Unlock()

	//runtime.GC()

	g.addWayIdsToNodesInCells(cells)

	g.cacheFileMutex.Lock()
	sigolo.Debugf("Close remaining open file handles")
	for filename, file := range g.cacheFileHandles {
		if file != nil {
			sigolo.Tracef("Close cell file %s", file.Name())

			writer := g.cacheFileWriters[filename]
			err = writer.Flush()
			sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file writer for grid-index store %s", file.Name()))

			err = file.Close()
			sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for grid-index store %s", file.Name()))
		} else {
			sigolo.Warnf("No cell file %s to close, there's probably an error previously when opening/creating it", filename)
		}
	}
	g.cacheFileHandles = map[string]*os.File{}
	g.cacheFileWriters = map[string]*bufio.Writer{}
	g.cacheFileMutex.Unlock()

	//for scanner.Scan() {
	//	obj := scanner.Object()
	//
	//	if obj.ObjectID().Type() != osm.TypeNode && obj.ObjectID().Type() != osm.TypeWay {
	//		// TODO Add relation support
	//		continue
	//	}
	//
	//	switch osmObj := obj.(type) {
	//	case *osm.Node:
	//		cell := g.GetCellIndexForCoordinate(osmObj.Lon, osmObj.Lat)
	//
	//		if _, hasDataInCell := cellDataMap[cell]; !hasDataInCell {
	//			cellDataMap[cell] = osm.Objects{osmObj}
	//		} else {
	//			cellDataMap[cell] = append(cellDataMap[cell], osmObj)
	//		}
	//
	//		g.nodeToPositionMap[osmObj.ID] = orb.Point{osmObj.Lon, osmObj.Lat}
	//		amountOfObjects++
	//	case *osm.Way:
	//		for i, node := range osmObj.Nodes {
	//			node.Lon = g.nodeToPositionMap[node.ID][0]
	//			node.Lat = g.nodeToPositionMap[node.ID][1]
	//			osmObj.Nodes[i] = node
	//
	//			cell := g.GetCellIndexForCoordinate(node.Lon, node.Lat)
	//			if _, hasDataInCell := cellDataMap[cell]; !hasDataInCell {
	//				cellDataMap[cell] = osm.Objects{osmObj}
	//			} else {
	//				cellDataMap[cell] = append(cellDataMap[cell], osmObj)
	//			}
	//
	//			// Add way to node mapping if not already exists (ways to contain duplicate nodes, i.e. polygons,
	//			// some roundabout, etc.).
	//			_, found := g.nodeToWayMap[node.ID]
	//			if !found {
	//				g.nodeToWayMap[node.ID] = osm.Ways{}
	//			}
	//			if !util.Contains(g.nodeToWayMap[node.ID], osmObj) {
	//				g.nodeToWayMap[node.ID] = append(g.nodeToWayMap[node.ID], osmObj)
	//			}
	//		}
	//		amountOfObjects++
	//	}
	//	// TODO	 Implement relation handling
	//	//case *osm.Relation:
	//
	//}
	//sigolo.Infof("Read %d objects in %d cells", amountOfObjects, len(cellDataMap))
	//
	//sigolo.Debugf("Write OSM objects")
	//for cell, objects := range cellDataMap {
	//	for _, obj := range objects {
	//		encodedFeature, err := g.toEncodedFeature(obj)
	//		if err != nil {
	//			return err
	//		}
	//
	//		err = g.writeOsmObjectToCell(cell[0], cell[1], encodedFeature)
	//		if err != nil {
	//			return err
	//		}
	//	}
	//}
	//
	//sigolo.Debugf("Close remaining open file handles")
	//for filename, file := range g.cacheFileHandles {
	//	if file != nil {
	//		sigolo.Tracef("Close cell file %s", file.Name())
	//
	//		writer := g.cacheFileWriters[filename]
	//		err = writer.Flush()
	//		sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file writer for grid-index store %s", file.Name()))
	//
	//		err = file.Close()
	//		sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for grid-index store %s", file.Name()))
	//	} else {
	//		sigolo.Warnf("No cell file %s to close, there's probably an error previously when opening/creating it", filename)
	//	}
	//}

	importDuration := time.Since(importStartTime)
	sigolo.Infof("Created OSM object index from OSM data in %s", importDuration)

	return nil
}

func (g *GridIndex) addWayIdsToNodesInCells(cells map[CellIndex]CellIndex) {
	sigolo.Debugf("Add way IDs to nodes")

	for cell, _ := range cells {
		sigolo.Debugf("Process cell %v", cell)

		sigolo.Debug("Collect node-way-relationship")
		nodeToWays, err := g.readNodeToWayMappingFromCellData(cell.X(), cell.Y())

		// TODO Collect relation IDs as well

		//g.featureCache = map[string][]feature.EncodedFeature{}
		//runtime.GC()

		cellFolderName := path.Join(g.BaseFolder, feature.OsmObjNode.String(), strconv.Itoa(cell.X()))
		cellFileName := path.Join(cellFolderName, strconv.Itoa(cell.Y())+".cell")

		if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
			sigolo.Tracef("Cell file %s does not exist, I'll return an empty feature list", cellFileName)
			//return nil, nil
			return
		} else if err != nil {
			//return nil, errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
			return
		}

		sigolo.Tracef("Read cell file %s", cellFileName)
		data, err := os.ReadFile(cellFileName)
		if err != nil {
			//return nil, errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, objectType)
			return
		}

		readFeatureChannel := make(chan []feature.EncodedFeature)
		var finishWaitGroup sync.WaitGroup
		finishWaitGroup.Add(1)
		go func() {
			for nodes := range readFeatureChannel {
				for _, node := range nodes {
					if node == nil {
						continue
					}

					if wayIds, ok := nodeToWays[node.GetID()]; ok {
						node.(*feature.EncodedNodeFeature).WayIds = wayIds
					}

					err = g.writeOsmObjectToCell(cell.X(), cell.Y(), node)
					sigolo.FatalCheck(err)
				}
				//runtime.GC()
			}
			finishWaitGroup.Done()
		}()

		g.readNodesFromCellData(readFeatureChannel, data)

		close(readFeatureChannel)

		finishWaitGroup.Wait()

		// Add way information to nodes
		//sigolo.Debugf("Add way IDs for nodes in cell %v", cell)
		//nodes, err := g.readFeaturesFromCellFile(cell.X(), cell.Y(), feature.OsmObjNode.String())
		//sigolo.FatalCheck(err)
		//for _, node := range nodes {
		//	if node == nil {
		//		continue
		//	}
		//
		//	if wayIds, ok := nodeToWays[node.GetID()]; ok {
		//		node.(*feature.EncodedNodeFeature).WayIds = wayIds
		//	}
		//
		//	err = g.writeOsmObjectToCell(cell.X(), cell.Y(), node)
		//	sigolo.FatalCheck(err)
		//}
		//
		//g.featureCache = map[string][]feature.EncodedFeature{}
		//runtime.GC()
	}
}

func (g *GridIndex) convertOsmToRawEncodedFeatures(inputFile string, cells map[CellIndex]CellIndex) error {
	file, scanner, err := g.getScanner(inputFile)
	if err != nil {
		return err
	}

	defer file.Close()
	defer scanner.Close()

	var emptyWayIds []osm.WayID
	//nodeToPositionMap := map[osm.NodeID][2]float32{}

	for scanner.Scan() {
		obj := scanner.Object()

		switch osmObj := obj.(type) {
		case *osm.Node:
			cell := g.GetCellIndexForCoordinate(osmObj.Lon, osmObj.Lat)

			nodeFeature, err := g.toEncodedNodeFeature(osmObj, emptyWayIds)
			sigolo.FatalCheck(err)
			err = g.writeOsmObjectToCell(cell.X(), cell.Y(), nodeFeature)
			sigolo.FatalCheck(err)

			cells[cell] = cell
			//nodeToPositionMap[osmObj.ID] = [2]float32{float32(osmObj.Lon), float32(osmObj.Lat)}
		case *osm.Way:
			//for i, node := range osmObj.Nodes {
			//	node.Lon = float64(nodeToPositionMap[node.ID][0])
			//	node.Lat = float64(nodeToPositionMap[node.ID][1])
			//	osmObj.Nodes[i] = node
			//}

			wayFeature, err := g.toEncodedWayFeature(osmObj)
			sigolo.FatalCheck(err)

			for _, node := range osmObj.Nodes {
				cell := g.GetCellIndexForCoordinate(node.Lon, node.Lat)

				err = g.writeOsmObjectToCell(cell.X(), cell.Y(), wayFeature)
				sigolo.FatalCheck(err)
			}
		}
		// TODO	 Implement relation handling
		//case *osm.Relation:
	}
	//sigolo.Debugf("Stored %d entries in nodeToPositionMap", len(nodeToPositionMap))
	return nil
}

func (g *GridIndex) getScanner(inputFile string) (*os.File, osm.Scanner, error) {
	if !strings.HasSuffix(inputFile, ".osm") && !strings.HasSuffix(inputFile, ".pbf") {
		return nil, nil, errors.Errorf("Input file %s must be an .osm or .pbf file", inputFile)
	}

	f, err := os.Open(inputFile)
	sigolo.FatalCheck(err)

	var scanner osm.Scanner
	if strings.HasSuffix(inputFile, ".osm") {
		scanner = osmxml.New(context.Background(), f)
	} else if strings.HasSuffix(inputFile, ".pbf") {
		scanner = osmpbf.New(context.Background(), f, 1)
	}
	return f, scanner, err
}

func (g *GridIndex) writeOsmObjectToCell(cellX int, cellY int, encodedFeature feature.EncodedFeature) error {
	var f io.Writer
	var err error

	sigolo.Tracef("Write OSM object to cell x=%d, y=%d, obj=%#v", cellX, cellY, encodedFeature.GetID())

	switch featureObj := encodedFeature.(type) {
	case *feature.EncodedNodeFeature:
		f, err = g.getCellFile(cellX, cellY, "node")
		if err != nil {
			return err
		}
		err = g.writeNodeData(featureObj, f)
		if err != nil {
			return errors.Wrapf(err, "Unable to write node %d to cell x=%d, y=%d", encodedFeature.GetID(), cellX, cellY)
		}
		return nil
	case *feature.EncodedWayFeature:
		f, err = g.getCellFile(cellX, cellY, "way")
		if err != nil {
			return err
		}
		err = g.writeWayData(featureObj, f)
		if err != nil {
			return errors.Wrapf(err, "Unable to write way %d to cell x=%d, y=%d", encodedFeature.GetID(), cellX, cellY)
		}
		return nil
	}
	// TODO	 Implement relation handling: Store nodes (ID with lat+lon) and ways (ID with first cell index, because the way appears in all cells it covers, so just store the first one)

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
	g.cacheFileMutex.Unlock()
	if cached {
		sigolo.Tracef("Cell file %s already exist and cached", cellFileName)
		return writer, nil
	}

	// Cell file not cached
	var file *os.File

	if _, err = os.Stat(cellFileName); err == nil {
		// Cell file does exist -> open it
		sigolo.Debugf("Cell file %s already exist but is not cached, I'll open it", cellFileName)
		file, err = os.OpenFile(cellFileName, os.O_RDWR, 0666)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to open cell file %s", cellFileName)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		// Cell file does NOT exist -> create its folder (if needed) and the file itself

		// Ensure the folder exists
		if _, err = os.Stat(cellFolderName); os.IsNotExist(err) {
			sigolo.Debugf("Cell folder %s doesn't exist, I'll create it", cellFolderName)
			err = os.MkdirAll(cellFolderName, os.ModePerm)
			if err != nil {
				return nil, errors.Wrapf(err, "Unable to create cell folder %s for cellY=%d", cellFolderName, cellY)
			}
		}

		// Create cell file
		sigolo.Debugf("Cell file %s does not exist, I'll create it", cellFileName)
		file, err = os.Create(cellFileName)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to create new cell file %s", cellFileName)
		}
	} else {
		return nil, errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	g.cacheFileMutex.Lock()
	g.cacheFileHandles[cellFileName] = file

	writer = bufio.NewWriter(file)
	g.cacheFileWriters[cellFileName] = writer
	g.cacheFileMutex.Unlock()

	return writer, nil
}

func (g *GridIndex) writeNodeData(encodedFeature *feature.EncodedNodeFeature, f io.Writer) error {
	/*
		Entry format:
		// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes).
		// TODO Store nodes with ways (only IDs to relations, because it can be fetched from the cell of the node) and relations (only IDs to relations, because it can be fetched from the cell of the node)

		Names: | osmId | lon | lat | num. keys | num. values | num. ways |   encodedKeys   |   encodedValues   |       ways      |
		Bytes: |   8   |  4  |  4  |     4     |      4      |     2     | <num. keys> / 8 | <num. values> * 3 | <num. ways> * 8 |

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
	wayIdBytes := len(encodedFeature.WayIds) * 8             // Int array and int = 4 bytes

	headerBytesCount := 8 + 4 + 4 + 4 + 4 + 2 // = 26
	byteCount := headerBytesCount
	byteCount += encodedKeyBytes
	byteCount += encodedValueBytes
	byteCount += wayIdBytes

	data := make([]byte, byteCount)

	point := encodedFeature.Geometry.(*orb.Point)

	binary.LittleEndian.PutUint64(data[0:], encodedFeature.ID)
	binary.LittleEndian.PutUint32(data[8:], math.Float32bits(float32(point.Lon())))
	binary.LittleEndian.PutUint32(data[12:], math.Float32bits(float32(point.Lat())))
	binary.LittleEndian.PutUint32(data[16:], uint32(numKeys))
	binary.LittleEndian.PutUint32(data[20:], uint32(len(encodedFeature.GetValues())))
	binary.LittleEndian.PutUint16(data[24:], uint16(len(encodedFeature.WayIds)))

	copy(data[headerBytesCount:], encodedFeature.GetKeys()[0:numKeys])
	for i, v := range encodedFeature.GetValues() {
		b := data[headerBytesCount+encodedKeyBytes+i*3:]
		b[0] = byte(v)
		b[1] = byte(v >> 8)
		b[2] = byte(v >> 16)
	}

	wayIdsStartIndex := headerBytesCount + encodedKeyBytes + encodedValueBytes
	for i, wayId := range encodedFeature.WayIds {
		binary.LittleEndian.PutUint64(data[wayIdsStartIndex+i*8:], uint64(wayId))
	}

	sigolo.Tracef("Write feature %d pos=%#v, byteCount=%d, numKeys=%d, numValues=%d, numWays=%d", encodedFeature.ID, point, byteCount, numKeys, len(encodedFeature.GetValues()), len(encodedFeature.WayIds))

	_, err := f.Write(data)
	if err != nil {
		return errors.Wrapf(err, "Unable to write node %d to cell file", encodedFeature.ID)
	}
	return nil
}

func (g *GridIndex) writeWayData(encodedFeature *feature.EncodedWayFeature, f io.Writer) error {
	/*
		Entry format:
		// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes).
		// TODO Store relations: only IDs to relations, because its data can be fetched from the cell of the way

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
	numKeys := 0
	for i := 0; i < len(encodedFeature.GetKeys()); i++ {
		if encodedFeature.GetKeys()[i] != 0 {
			numKeys = i + 1
		}
	}

	encodedKeyBytes := numKeys                               // Is already a byte-array -> no division by 8 needed
	encodedValueBytes := len(encodedFeature.GetValues()) * 3 // Int array and int = 4 bytes
	nodeIdBytes := len(encodedFeature.Nodes) * 16            // Each ID is a 64-bit int

	headerByteCount := 8 + 4 + 4 + 2
	byteCount := headerByteCount
	byteCount += encodedKeyBytes
	byteCount += encodedValueBytes
	byteCount += nodeIdBytes

	data := make([]byte, byteCount)

	/*
		Write header
	*/
	binary.LittleEndian.PutUint64(data[0:], encodedFeature.ID)
	binary.LittleEndian.PutUint32(data[8:], uint32(numKeys))
	binary.LittleEndian.PutUint32(data[12:], uint32(len(encodedFeature.GetValues())))
	binary.LittleEndian.PutUint16(data[16:], uint16(len(encodedFeature.Nodes)))

	/*
		Write keys and values
	*/
	copy(data[headerByteCount:], encodedFeature.GetKeys()[0:numKeys])
	for i, v := range encodedFeature.GetValues() {
		b := data[headerByteCount+encodedKeyBytes+i*3:]
		b[0] = byte(v)
		b[1] = byte(v >> 8)
		b[2] = byte(v >> 16)
	}

	/*
		Write nodes
	*/
	nodeIdStartIndex := headerByteCount + encodedKeyBytes + len(encodedFeature.GetValues())*3
	for i, node := range encodedFeature.Nodes {
		binary.LittleEndian.PutUint64(data[nodeIdStartIndex+i*16:], uint64(node.ID))
		binary.LittleEndian.PutUint32(data[nodeIdStartIndex+i*16+8:], math.Float32bits(float32(node.Lon)))
		binary.LittleEndian.PutUint32(data[nodeIdStartIndex+i*16+12:], math.Float32bits(float32(node.Lat)))
	}

	sigolo.Tracef("Write feature %d byteCount=%d, numKeys=%d, numValues=%d, numNodeIds=%d", encodedFeature.ID, byteCount, numKeys, len(encodedFeature.GetValues()), len(encodedFeature.Nodes))

	_, err := f.Write(data)
	if err != nil {
		return errors.Wrapf(err, "Unable to write node %d to cell file", encodedFeature.ID)
	}
	return nil
}

// GetCellIndexForCoordinate returns the cell index (i.e. position) for the given coordinate.
func (g *GridIndex) GetCellIndexForCoordinate(x float64, y float64) CellIndex {
	return CellIndex{int(x / g.CellWidth), int(y / g.CellHeight)}
}

func (g *GridIndex) toEncodedFeature(obj osm.Object) (feature.EncodedFeature, error) {
	switch osmObj := obj.(type) {
	case *osm.Node:
		var wayIds []osm.WayID
		for _, way := range g.nodeToWayMap[osmObj.ID] {
			wayIds = append(wayIds, way.ID)
		}

		return g.toEncodedNodeFeature(osmObj, wayIds)
	case *osm.Way:
		return g.toEncodedWayFeature(osmObj)
	}
	// TODO Implement relation handling
	//case *osm.Relation:

	return nil, errors.Errorf("Converting OSM object of type '%s' not supported", obj.ObjectID().Type())
}

func (g *GridIndex) toEncodedNodeFeature(obj *osm.Node, wayIds []osm.WayID) (*feature.EncodedNodeFeature, error) {
	var geometry orb.Geometry

	point := obj.Point()
	geometry = &point

	encodedKeys, encodedValues := g.TagIndex.encodeTags(obj.Tags)

	abstractEncodedFeature := feature.AbstractEncodedFeature{
		ID:       uint64(obj.ID),
		Geometry: geometry,
		Keys:     encodedKeys,
		Values:   encodedValues,
	}

	return &feature.EncodedNodeFeature{
		AbstractEncodedFeature: abstractEncodedFeature,
		WayIds:                 wayIds,
	}, nil
}

func (g *GridIndex) toEncodedWayFeature(obj *osm.Way) (*feature.EncodedWayFeature, error) {
	var tags osm.Tags
	var geometry orb.Geometry
	var osmId uint64

	tags = obj.Tags
	lineString := obj.LineString()
	geometry = &lineString
	osmId = uint64(obj.ID)

	encodedKeys, encodedValues := g.TagIndex.encodeTags(tags)

	abstractEncodedFeature := feature.AbstractEncodedFeature{
		ID:       osmId,
		Geometry: geometry,
		Keys:     encodedKeys,
		Values:   encodedValues,
	}

	return &feature.EncodedWayFeature{
		AbstractEncodedFeature: abstractEncodedFeature,
		Nodes:                  obj.Nodes,
	}, nil
}

func (g *GridIndex) Get(bbox *orb.Bound, objectType string) (chan *GetFeaturesResult, error) {
	sigolo.Debugf("Get feature from bbox=%#v", bbox)
	minCell := g.GetCellIndexForCoordinate(bbox.Min.Lon(), bbox.Min.Lat())
	maxCell := g.GetCellIndexForCoordinate(bbox.Max.Lon(), bbox.Max.Lat())

	resultChannel := make(chan *GetFeaturesResult, 10)

	numThreads := 3
	var wg sync.WaitGroup
	wg.Add(numThreads)
	go func() {
		// Group the cells into columns of equal size so that each goroutine below can handle on column.
		cellColumns := maxCell.X() - minCell.X() + 1 // min and max are inclusive, therefore +1
		threadColumns := cellColumns / numThreads

		for i := 0; i < numThreads; i++ {
			minColX := minCell.X() + i*threadColumns
			maxColX := minCell.X() + (i+1)*threadColumns
			if i != 0 {
				// To prevent overlapping columns
				minColX++
			}
			if i == numThreads-1 {
				// Last column: Make sure it goes til the requested end
				maxColX = maxCell.X()
			}

			go g.getFeaturesForCellsWithBbox(resultChannel, &wg, bbox, minColX, maxColX, minCell.Y(), maxCell.Y(), objectType)
		}
	}()

	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	return resultChannel, nil // Remove error from return, since it doesn't make any sense here
}

func (g *GridIndex) GetNodes(nodes osm.WayNodes) (chan *GetFeaturesResult, error) {
	cells := map[CellIndex][]uint64{}            // just a lookup table to quickly see if a cell has already been collected
	innerCellBounds := map[CellIndex]orb.Bound{} // just a lookup table to quickly see if a cell has already been collected
	for _, node := range nodes {
		cell := g.GetCellIndexForCoordinate(node.Lon, node.Lat)
		if _, ok := cells[cell]; !ok {
			// New cell -> Create it and add first node ID
			cells[cell] = []uint64{uint64(node.ID)}
			innerCellBounds[cell] = node.Point().Bound()
		} else {
			// Cell has been seen before -> just add the new node ID
			cells[cell] = append(cells[cell], uint64(node.ID))
			innerCellBounds[cell] = innerCellBounds[cell].Union(node.Point().Bound())
		}
	}

	var resultChannel = make(chan *GetFeaturesResult, 10)

	if len(cells) == 0 {
		close(resultChannel)
		return resultChannel, nil
	}

	go func() {
		for cell, nodeIds := range cells {
			innerCellBound := innerCellBounds[cell]
			outputBuffer := []feature.EncodedFeature{}

			unfilteredFeatures, err := g.readFeaturesFromCellFile(cell[0], cell[1], feature.OsmObjNode.String())
			sigolo.FatalCheck(err)

			for i := 0; i < len(unfilteredFeatures); i++ {
				encodedFeature := unfilteredFeatures[i]
				if encodedFeature != nil {
					// TODO Getting the bound, geometry and filtering takes quite long. Try to optimize this (maybe with a "encodedFeature.IsWithin()" func?)
					featureBound := encodedFeature.GetGeometry().Bound()
					notWithinCellContent := (innerCellBound.Max[0] < featureBound.Min[0]) ||
						(innerCellBound.Min[0] > featureBound.Max[0]) ||
						(innerCellBound.Max[1] < featureBound.Min[1]) ||
						(innerCellBound.Min[1] > featureBound.Max[1])
					if notWithinCellContent {
						continue
					}

					for j := 0; j < len(nodeIds); j++ {
						if encodedFeature.GetID() == nodeIds[j] {
							outputBuffer = append(outputBuffer, encodedFeature)
							break
						}
					}
				}
			}

			resultChannel <- &GetFeaturesResult{
				Cell:     cell,
				Features: outputBuffer,
			}
		}
		close(resultChannel)
	}()

	return resultChannel, nil
}

func (g *GridIndex) GetFeaturesForCells(cells []CellIndex, objectType string) chan *GetFeaturesResult {
	resultChannel := make(chan *GetFeaturesResult)

	go func() {
		for _, cell := range cells {
			featuresInCell := &GetFeaturesResult{
				Cell:     cell,
				Features: []feature.EncodedFeature{},
			}

			encodedFeatures, err := g.readFeaturesFromCellFile(cell[0], cell[1], objectType)
			sigolo.FatalCheck(err)
			featuresInCell.Features = encodedFeatures

			resultChannel <- featuresInCell
		}
		close(resultChannel)
	}()

	return resultChannel
}

func (g *GridIndex) getFeaturesForCellsWithBbox(output chan *GetFeaturesResult, wg *sync.WaitGroup, bbox *orb.Bound, minCellX int, maxCellX int, minCellY int, maxCellY int, objectType string) {
	sigolo.Tracef("Get feature for cell column from=%d, to=%d", minCellX, maxCellX)
	for cellX := minCellX; cellX <= maxCellX; cellX++ {
		for cellY := minCellY; cellY <= maxCellY; cellY++ {
			featuresInBbox := &GetFeaturesResult{
				Cell:     CellIndex{cellX, cellY},
				Features: []feature.EncodedFeature{},
			}

			encodedFeatures, err := g.readFeaturesFromCellFile(cellX, cellY, objectType)
			sigolo.FatalCheck(err)

			for i := 0; i < len(encodedFeatures); i++ {
				if encodedFeatures[i] != nil && bbox.Intersects(encodedFeatures[i].GetGeometry().Bound()) {
					featuresInBbox.Features = append(featuresInBbox.Features, encodedFeatures[i])
				}
			}

			output <- featuresInBbox
		}
	}
	wg.Done()
}

// readFeaturesFromCellFile reads all features from the specified cell and writes them periodically to the output channel.
func (g *GridIndex) readFeaturesFromCellFile(cellX int, cellY int, objectType string) ([]feature.EncodedFeature, error) {
	cellFolderName := path.Join(g.BaseFolder, objectType, strconv.Itoa(cellX))
	cellFileName := path.Join(cellFolderName, strconv.Itoa(cellY)+".cell")

	if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
		sigolo.Tracef("Cell file %s does not exist, I'll return an empty feature list", cellFileName)
		return nil, nil
	} else if err != nil {
		return nil, errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	g.featureCacheMutex.Lock()
	if cachedFeatures, ok := g.featureCache[cellFileName]; ok {
		sigolo.Tracef("Use features from cache for cell file %s", cellFileName)
		g.featureCacheMutex.Unlock()
		return cachedFeatures, nil
	}
	g.featureCache[cellFileName] = []feature.EncodedFeature{}
	g.featureCacheMutex.Unlock()

	sigolo.Tracef("Read cell file %s", cellFileName)
	data, err := os.ReadFile(cellFileName)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, objectType)
	}

	readFeatureChannel := make(chan []feature.EncodedFeature)
	go func() {
		for readFeatures := range readFeatureChannel {
			// TODO not-null check needed for the features?
			g.featureCacheMutex.Lock()
			g.featureCache[cellFileName] = append(g.featureCache[cellFileName], readFeatures...)
			g.featureCacheMutex.Unlock()
		}
	}()

	switch objectType {
	case "node":
		g.readNodesFromCellData(readFeatureChannel, data)
	case "way":
		g.readWaysFromCellData(readFeatureChannel, data)
	default:
		panic("Unsupported object type to read: " + objectType)
	}

	close(readFeatureChannel)

	return g.featureCache[cellFileName], nil
}

func (g *GridIndex) readNodesFromCellData(output chan []feature.EncodedFeature, data []byte) {
	outputBuffer := make([]feature.EncodedFeature, 1000)
	currentBufferPos := 0

	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeNodeData".
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		lon := math.Float32frombits(binary.LittleEndian.Uint32(data[pos+8:]))
		lat := math.Float32frombits(binary.LittleEndian.Uint32(data[pos+12:]))
		numEncodedKeyBytes := int(binary.LittleEndian.Uint32(data[pos+16:]))
		numValues := int(binary.LittleEndian.Uint32(data[pos+20:]))
		numWayIds := int(binary.LittleEndian.Uint16(data[pos+24:]))
		wayIdsBytes := numWayIds * 8

		headerBytesCount := 8 + 4 + 4 + 4 + 4 + 2 // = 26

		sigolo.Tracef("Read feature pos=%d, id=%d, lon=%f, lat=%f, numKeys=%d, numValues=%d", pos, osmId, lon, lat, numEncodedKeyBytes, numValues)

		encodedValuesBytes := numValues * 3 // Multiplication since each value is an int with 3 bytes

		encodedKeys := make([]byte, numEncodedKeyBytes)
		encodedValues := make([]int, numValues)
		copy(encodedKeys[:], data[pos+headerBytesCount:])

		for i := 0; i < numValues; i++ {
			b := data[pos+headerBytesCount+numEncodedKeyBytes+i*3:]
			encodedValues[i] = int(uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16)
		}

		wayIds := make([]osm.WayID, numWayIds)
		wayIdsStartIndex := headerBytesCount + numEncodedKeyBytes + encodedValuesBytes
		for i := 0; i < numWayIds; i++ {
			wayIds[i] = osm.WayID(binary.LittleEndian.Uint64(data[wayIdsStartIndex+i*8:]))
		}

		encodedFeature := &feature.EncodedNodeFeature{
			AbstractEncodedFeature: feature.AbstractEncodedFeature{
				ID:       osmId,
				Geometry: &orb.Point{float64(lon), float64(lat)},
				Keys:     encodedKeys,
				Values:   encodedValues,
			},
			WayIds: wayIds,
		}
		if g.checkFeatureValidity {
			sigolo.Debugf("Check validity of feature %d", encodedFeature.ID)
			g.checkValidity(encodedFeature)
		}

		outputBuffer[currentBufferPos] = encodedFeature
		currentBufferPos++

		if currentBufferPos == len(outputBuffer)-1 {
			output <- outputBuffer
			outputBuffer = make([]feature.EncodedFeature, len(outputBuffer))
			currentBufferPos = 0
		}

		pos += headerBytesCount + numEncodedKeyBytes + encodedValuesBytes + wayIdsBytes
	}

	output <- outputBuffer
}

func (g *GridIndex) readWaysFromCellData(output chan []feature.EncodedFeature, data []byte) {
	outputBuffer := make([]feature.EncodedFeature, 1000)
	currentBufferPos := 0
	totalReadFeatures := 0

	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeWayData".

		/*
			Read general information of the feature
		*/
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		numEncodedKeyBytes := int(binary.LittleEndian.Uint32(data[pos+8:]))
		numValues := int(binary.LittleEndian.Uint32(data[pos+12:]))
		encodedValuesBytes := numValues * 3 // Multiplication since each value is an int with 3 bytes
		numNodes := int(binary.LittleEndian.Uint16(data[pos+16:]))
		nodeBytes := numNodes * 16

		headerBytesCount := 8 + 4 + 4 + 2

		sigolo.Tracef("Read feature pos=%d, id=%d, numKeys=%d, numValues=%d", pos, osmId, numEncodedKeyBytes, numValues)

		/*
			Read the keys and tags of the feature
		*/
		encodedKeys := make([]byte, numEncodedKeyBytes)
		encodedValues := make([]int, numValues)
		copy(encodedKeys[:], data[pos+headerBytesCount:])

		encodedValuesStartIndex := pos + headerBytesCount + numEncodedKeyBytes
		for i := 0; i < numValues; i++ {
			b := data[encodedValuesStartIndex+i*3:]
			encodedValues[i] = int(uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16)
		}

		/*
			Read the node-IDs of the way
		*/
		var nodes osm.WayNodes
		nodesStartIndex := encodedValuesStartIndex + encodedValuesBytes
		for i := 0; i < numNodes; i++ {
			nodeIdIndex := nodesStartIndex + i*16
			lonIndex := nodesStartIndex + i*16 + 8
			latIndex := nodesStartIndex + i*16 + 12
			nodes = append(nodes, osm.WayNode{
				ID:  osm.NodeID(binary.LittleEndian.Uint64(data[nodeIdIndex:])),
				Lon: float64(math.Float32frombits(binary.LittleEndian.Uint32(data[lonIndex:]))),
				Lat: float64(math.Float32frombits(binary.LittleEndian.Uint32(data[latIndex:]))),
			})
		}

		/*
			Actually create the encoded feature
		*/
		var lineString orb.LineString
		for _, node := range nodes {
			lineString = append(lineString, orb.Point{node.Lon, node.Lat})
		}
		encodedFeature := feature.EncodedWayFeature{
			AbstractEncodedFeature: feature.AbstractEncodedFeature{
				ID:       osmId,
				Keys:     encodedKeys,
				Values:   encodedValues,
				Geometry: &lineString,
			},
			Nodes: nodes,
		}
		if g.checkFeatureValidity {
			sigolo.Debugf("Check validity of feature %d", encodedFeature.ID)
			g.checkValidity(&encodedFeature)
		}

		outputBuffer[currentBufferPos] = &encodedFeature
		currentBufferPos++

		if currentBufferPos == len(outputBuffer)-1 {
			output <- outputBuffer
			outputBuffer = make([]feature.EncodedFeature, len(outputBuffer))
			currentBufferPos = 0
		}

		pos += headerBytesCount + numEncodedKeyBytes + encodedValuesBytes + nodeBytes
		totalReadFeatures++
	}

	output <- outputBuffer
}

func (g *GridIndex) readNodeToWayMappingFromCellData(cellX int, cellY int) (map[uint64][]osm.WayID, error) {
	cellFolderName := path.Join(g.BaseFolder, feature.OsmObjWay.String(), strconv.Itoa(cellX))
	cellFileName := path.Join(cellFolderName, strconv.Itoa(cellY)+".cell")

	if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
		sigolo.Tracef("Cell file %s does not exist, I'll return an empty feature list", cellFileName)
		return nil, nil
	} else if err != nil {
		return nil, errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	sigolo.Tracef("Read cell file %s", cellFileName)
	data, err := os.ReadFile(cellFileName)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, feature.OsmObjWay.String())
	}

	nodeToWays := map[uint64][]osm.WayID{}

	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeWayData".

		/*
			Read general information of the feature
		*/
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		numEncodedKeyBytes := int(binary.LittleEndian.Uint32(data[pos+8:]))
		numValues := int(binary.LittleEndian.Uint32(data[pos+12:]))
		encodedValuesBytes := numValues * 3 // Multiplication since each value is an int with 3 bytes
		numNodes := int(binary.LittleEndian.Uint16(data[pos+16:]))
		nodeBytes := numNodes * 16

		headerBytesCount := 8 + 4 + 4 + 2

		sigolo.Tracef("Read feature pos=%d, id=%d, numKeys=%d, numValues=%d", pos, osmId, numEncodedKeyBytes, numValues)

		encodedValuesStartIndex := pos + headerBytesCount + numEncodedKeyBytes

		/*
			Read the node-IDs of the way
		*/
		nodesStartIndex := encodedValuesStartIndex + encodedValuesBytes
		for i := 0; i < numNodes; i++ {
			nodeIdIndex := nodesStartIndex + i*16
			nodeId := binary.LittleEndian.Uint64(data[nodeIdIndex:])

			if _, ok := nodeToWays[nodeId]; !ok {
				nodeToWays[nodeId] = []osm.WayID{osm.WayID(osmId)}
			} else {
				nodeToWays[nodeId] = append(nodeToWays[nodeId], osm.WayID(osmId))
			}
		}

		pos += headerBytesCount + numEncodedKeyBytes + encodedValuesBytes + nodeBytes
	}

	return nodeToWays, nil
}

func (g *GridIndex) checkValidity(encodedFeature feature.EncodedFeature) {
	// Check keys
	if len(encodedFeature.GetKeys()) > len(g.TagIndex.keyMap) {
		sigolo.Fatalf("Invalid length of keys in feature %d: Expected less than %d but found %d", encodedFeature.GetID(), len(g.TagIndex.keyMap), len(encodedFeature.GetKeys()))
	}

	// Check values
	numberOfSetKeys := 0
	for keyIndex := 0; keyIndex < len(encodedFeature.GetKeys())*8; keyIndex++ {
		if encodedFeature.HasKey(keyIndex) {
			valueIndex := encodedFeature.GetValueIndex(keyIndex)
			if valueIndex > len(g.TagIndex.valueMap[keyIndex])-1 {
				sigolo.Fatalf("Invalid key value found in feature %d: keyIndex=%d, valueIndex=%d, allowedMaxValueIndex=%d", encodedFeature.GetID(), keyIndex, valueIndex, len(g.TagIndex.valueMap[keyIndex])-1)
			}
			numberOfSetKeys++
		}
	}

	if numberOfSetKeys > len(encodedFeature.GetValues()) {
		sigolo.Fatalf("Invalid number of value indices found in feature %d: Expected %d values but found %d", encodedFeature.GetID(), len(encodedFeature.GetValues())-1, numberOfSetKeys)
	}
}

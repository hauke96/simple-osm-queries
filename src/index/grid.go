package index

import (
	"bufio"
	"context"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
	"github.com/paulmach/osm/osmxml"
	"github.com/pkg/errors"
	"io"
	"os"
	"path"
	"runtime"
	"soq/feature"
	"soq/util"
	"strconv"
	"strings"
	"sync"
	"time"
)

const GridIndexFolder = "grid-index"

var (
	// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with the 3 bytes DatatypeInt24).
	// TODO Store nodes with ways (only IDs to relations, because it can be fetched from the cell of the node) and relations (only IDs to relations, because it can be fetched from the cell of the node)
	nodeBinarySchema = util.BinarySchema{
		Items: []util.BinaryItem{
			&util.BinaryDataItem{FieldName: "ID", BinaryType: util.DatatypeInt64},
			&util.BinaryDataItem{FieldName: "Lon", BinaryType: util.DatatypeFloat32},
			&util.BinaryDataItem{FieldName: "Lat", BinaryType: util.DatatypeFloat32},
			&util.BinaryRawCollectionItem{FieldName: "EncodedKeys", BinaryType: util.DatatypeByte},
			&util.BinaryRawCollectionItem{FieldName: "EncodedValues", BinaryType: util.DatatypeInt24},
			&util.BinaryRawCollectionItem{FieldName: "WayIds", BinaryType: util.DatatypeInt64},
		},
	}

	// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes DatatypeInt24).
	// TODO Store relations: only IDs to relations, because its data can be fetched from the cell of the way
	wayBinarySchema = util.BinarySchema{
		Items: []util.BinaryItem{
			&util.BinaryDataItem{FieldName: "ID", BinaryType: util.DatatypeInt64},
			&util.BinaryRawCollectionItem{FieldName: "EncodedKeys", BinaryType: util.DatatypeByte},
			&util.BinaryRawCollectionItem{FieldName: "EncodedValues", BinaryType: util.DatatypeInt24},
			&util.BinaryCollectionItem{
				FieldName: "Nodes",
				ItemSchema: util.BinarySchema{
					Items: []util.BinaryItem{
						&util.BinaryDataItem{FieldName: "ID", BinaryType: util.DatatypeInt64},
						&util.BinaryDataItem{FieldName: "Lon", BinaryType: util.DatatypeFloat32},
						&util.BinaryDataItem{FieldName: "Lat", BinaryType: util.DatatypeFloat32},
					},
				},
			},
		},
	}
)

type NodeBinaryDao struct {
	ID            uint64
	Lon           float64
	Lat           float64
	EncodedKeys   []byte
	EncodedValues []int
	WayIds        []int64
}

func (d NodeBinaryDao) getExpectedByteSize() int {
	encodedKeysSize := 4 + len(d.EncodedKeys)
	encodedValuesSize := 4 + len(d.EncodedValues)*3 // *3 because of the Int24 type
	wayIdsSize := 4 + len(d.WayIds)*8

	// ID + size of lat/lon + the lists
	return 8 + 4 + 4 + encodedKeysSize + encodedValuesSize + wayIdsSize
}

type WayNodeBinaryDao struct {
	ID  uint64
	Lon float64
	Lat float64
}

type WayBinaryDao struct {
	ID            uint64
	EncodedKeys   []byte
	EncodedValues []int
	Nodes         []WayNodeBinaryDao
}

func (d WayBinaryDao) getExpectedByteSize() int {
	encodedKeysSize := 4 + len(d.EncodedKeys)
	encodedValuesSize := 4 + len(d.EncodedValues)*3 // *3 because of the Int24 type
	nodesSize := 4 + len(d.Nodes)*16

	// ID + size of the lists
	return 8 + encodedKeysSize + encodedValuesSize + nodesSize
}

type GridIndex struct {
	TagIndex             *TagIndex
	CellWidth            float64
	CellHeight           float64
	BaseFolder           string
	cacheFileHandles     map[string]*os.File
	cacheFileWriters     map[string]*bufio.Writer
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

	runtime.GC()

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

	runtime.GC()

	g.addWayIdsToNodesInCells(cells)

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
				runtime.GC()
			}
		}()

		g.readNodesFromCellData(readFeatureChannel, data)

		close(readFeatureChannel)

		// Add way information to nodes
		//sigolo.Debugf("Add way IDs for nodes in cell %v", cell)
		//nodes, err := g.readFeaturesFromCellFile(cell.X(), cell.Y(), feature.OsmObjNode.String())
		//sigolo.FatalCheck(err)
		//err = g.clearCellFile(cell, feature.OsmObjNode.String())
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
		runtime.GC()
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

	writer, cached = g.cacheFileWriters[cellFileName]
	if cached {
		sigolo.Tracef("Cell file %s already exist and cached", cellFileName)
		return writer, nil
	}

	// Cell file not cached
	var file *os.File

	if _, err = os.Stat(cellFileName); err == nil {
		// Cell file does exist -> open it
		sigolo.Debugf("Cell file %s already exist but is not cached, I'll open it", cellFileName)
		file, err = os.OpenFile(cellFileName, os.O_APPEND|os.O_RDWR, 0666)
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

	g.cacheFileHandles[cellFileName] = file

	writer = bufio.NewWriter(file)
	g.cacheFileWriters[cellFileName] = writer

	return writer, nil
}

func (g *GridIndex) clearCellFile(cell CellIndex, objectType string) error {
	cellFolderName := g.BaseFolder + "/" + objectType + "/" + strconv.Itoa(cell.X())
	cellFileName := cellFolderName + "/" + strconv.Itoa(cell.Y()) + ".cell"
	return os.Remove(cellFileName)
}

func (g *GridIndex) writeNodeData(encodedFeature *feature.EncodedNodeFeature, f io.Writer) error {
	// The number of key-bins to store is determined by the bin with the highest index that is not empty (i.e. all 0s).
	// If only the first bin contains some 1s (i.e. keys that are set on the feature) and the next 100 bins are empty,
	// then there's no reason to store those empty bins. This reduced the cell-file size for hamburg-latest (45 MB PBF)
	// by a factor of ten!
	encodedKeyBytes := 0
	for i := 0; i < len(encodedFeature.GetKeys()); i++ {
		if encodedFeature.GetKeys()[i] != 0 {
			encodedKeyBytes = i + 1
		}
	}

	wayIdInts := make([]int64, len(encodedFeature.WayIds))
	for i, wayId := range encodedFeature.WayIds {
		wayIdInts[i] = int64(wayId)
	}

	dao := NodeBinaryDao{
		ID:            encodedFeature.ID,
		Lon:           encodedFeature.Geometry.(*orb.Point).Lon(),
		Lat:           encodedFeature.Geometry.(*orb.Point).Lat(),
		EncodedKeys:   encodedFeature.GetKeys()[0:encodedKeyBytes],
		EncodedValues: encodedFeature.GetValues(),
		WayIds:        wayIdInts,
	}

	data := make([]byte, dao.getExpectedByteSize())

	_, err := nodeBinarySchema.Write(dao, data, 0)
	if err != nil {
		return errors.Wrapf(err, "Unable to create binary data for node %d", encodedFeature.ID)
	}

	_, err = f.Write(data)
	if err != nil {
		return errors.Wrapf(err, "Unable to write node %d to cell file", encodedFeature.ID)
	}

	return nil
}

func (g *GridIndex) writeWayData(encodedFeature *feature.EncodedWayFeature, f io.Writer) error {
	// The number of key-bins to store is determined by the bin with the highest index that is not empty (i.e. all 0s).
	// If only the first bin contains some 1s (i.e. keys that are set on the feature) and the next 100 bins are empty,
	// then there's no reason to store those empty bins. This reduced the cell-file size for hamburg-latest (45 MB PBF)
	// by a factor of ten!
	encodedKeyBytes := 0
	for i := 0; i < len(encodedFeature.GetKeys()); i++ {
		if encodedFeature.GetKeys()[i] != 0 {
			encodedKeyBytes = i + 1
		}
	}

	var nodeDaos []WayNodeBinaryDao
	for _, node := range encodedFeature.Nodes {
		nodeDaos = append(nodeDaos, WayNodeBinaryDao{
			ID:  uint64(node.ID),
			Lon: node.Lon,
			Lat: node.Lat,
		})
	}

	dao := WayBinaryDao{
		ID:            encodedFeature.ID,
		EncodedKeys:   encodedFeature.GetKeys()[0:encodedKeyBytes],
		EncodedValues: encodedFeature.GetValues(),
		Nodes:         nodeDaos,
	}

	data := make([]byte, dao.getExpectedByteSize())

	_, err := wayBinarySchema.Write(dao, data, 0)
	if err != nil {
		return errors.Wrapf(err, "Unable to create binary data for node %d", encodedFeature.ID)
	}

	_, err = f.Write(data)
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
	if _, ok := g.featureCache[cellFileName]; ok {
		sigolo.Tracef("Use features from cache for cell file %s", cellFileName)
		g.featureCacheMutex.Unlock()
		return g.featureCache[cellFileName], nil
	} else {
		g.featureCache[cellFileName] = []feature.EncodedFeature{}
	}
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
	var err error

	for pos := 0; pos < len(data); {
		dao := NodeBinaryDao{}
		pos, err = nodeBinarySchema.Read(&dao, data, pos)
		sigolo.FatalCheck(err) // TODO better return error?

		wayIds := make([]osm.WayID, len(dao.WayIds))
		for i, wayId := range dao.WayIds {
			wayIds[i] = osm.WayID(wayId)
		}

		encodedFeature := &feature.EncodedNodeFeature{
			AbstractEncodedFeature: feature.AbstractEncodedFeature{
				ID:       dao.ID,
				Geometry: &orb.Point{dao.Lon, dao.Lat},
				Keys:     dao.EncodedKeys,
				Values:   dao.EncodedValues,
			},
			WayIds: wayIds,
		}

		if g.checkFeatureValidity {
			sigolo.Debugf("Check validity of node feature %d", encodedFeature.ID)
			g.checkValidity(encodedFeature)
		}

		outputBuffer[currentBufferPos] = encodedFeature
		currentBufferPos++

		if currentBufferPos == len(outputBuffer)-1 {
			output <- outputBuffer
			outputBuffer = make([]feature.EncodedFeature, len(outputBuffer))
			currentBufferPos = 0
		}
	}

	output <- outputBuffer
}

func (g *GridIndex) readWaysFromCellData(output chan []feature.EncodedFeature, data []byte) {
	outputBuffer := make([]feature.EncodedFeature, 1000)
	currentBufferPos := 0
	var err error

	for pos := 0; pos < len(data); {
		dao := WayBinaryDao{}
		pos, err = wayBinarySchema.Read(&dao, data, pos)
		sigolo.FatalCheck(err) // TODO better return error?

		var lineString orb.LineString
		var nodes osm.WayNodes
		for _, nodeDao := range dao.Nodes {
			lineString = append(lineString, orb.Point{nodeDao.Lon, nodeDao.Lat})
			nodes = append(nodes, osm.WayNode{
				ID:  osm.NodeID(nodeDao.ID),
				Lon: nodeDao.Lon,
				Lat: nodeDao.Lat,
			})
		}

		encodedFeature := &feature.EncodedWayFeature{
			AbstractEncodedFeature: feature.AbstractEncodedFeature{
				ID:       dao.ID,
				Geometry: lineString,
				Keys:     dao.EncodedKeys,
				Values:   dao.EncodedValues,
			},
			Nodes: nodes,
		}

		if g.checkFeatureValidity {
			sigolo.Debugf("Check validity of way feature %d", encodedFeature.ID)
			g.checkValidity(encodedFeature)
		}

		outputBuffer[currentBufferPos] = encodedFeature
		currentBufferPos++

		if currentBufferPos == len(outputBuffer)-1 {
			output <- outputBuffer
			outputBuffer = make([]feature.EncodedFeature, len(outputBuffer))
			currentBufferPos = 0
		}
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

	dao := WayBinaryDao{} // TODO Test if this recycling really works
	for pos := 0; pos < len(data); {
		pos, err = wayBinarySchema.Read(&dao, data, pos)
		sigolo.FatalCheck(err) // TODO better return error?

		for _, nodeDao := range dao.Nodes {
			if _, ok := nodeToWays[nodeDao.ID]; !ok {
				nodeToWays[nodeDao.ID] = []osm.WayID{osm.WayID(dao.ID)}
			} else {
				nodeToWays[nodeDao.ID] = append(nodeToWays[nodeDao.ID], osm.WayID(dao.ID))
			}
		}
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

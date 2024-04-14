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
	checkFeatureValidity bool
	nodeToPositionMap    map[osm.NodeID]orb.Point
	featureCache         map[string][]feature.EncodedFeature // Filename to feature within it
}

type CellIndex [2]int

func LoadGridIndex(indexBaseFolder string, cellWidth float64, cellHeight float64, checkFeatureValidity bool, tagIndex *TagIndex) *GridIndex {
	return &GridIndex{
		TagIndex:             tagIndex,
		CellWidth:            cellWidth,
		CellHeight:           cellHeight,
		BaseFolder:           path.Join(indexBaseFolder, GridIndexFolder),
		checkFeatureValidity: checkFeatureValidity,
		featureCache:         map[string][]feature.EncodedFeature{},
	}
}

func (g *GridIndex) Import(inputFile string) error {
	if !strings.HasSuffix(inputFile, ".osm") && !strings.HasSuffix(inputFile, ".pbf") {
		sigolo.Error("Input file must be an .osm or .pbf file")
		os.Exit(1)
	}

	f, err := os.Open(inputFile)
	sigolo.FatalCheck(err)
	defer f.Close()

	var scanner osm.Scanner
	if strings.HasSuffix(inputFile, ".osm") {
		scanner = osmxml.New(context.Background(), f)
	} else if strings.HasSuffix(inputFile, ".pbf") {
		scanner = osmpbf.New(context.Background(), f, 1)
	}
	defer scanner.Close()

	err = os.RemoveAll(g.BaseFolder)
	if err != nil {
		return errors.Wrapf(err, "Unable to remove grid-index base folder %s", g.BaseFolder)
	}

	sigolo.Info("Start processing geometries from input data")
	importStartTime := time.Now()

	var encodedFeature feature.EncodedFeature
	var cellX, cellY int

	g.cacheFileHandles = map[string]*os.File{}
	g.cacheFileWriters = map[string]*bufio.Writer{}
	g.nodeToPositionMap = map[osm.NodeID]orb.Point{}

	for scanner.Scan() {
		obj := scanner.Object()

		if obj.ObjectID().Type() != osm.TypeNode && obj.ObjectID().Type() != osm.TypeWay {
			// TODO Add relation support
			continue
		}

		encodedFeature, err = g.toEncodedFeature(obj)
		if err != nil {
			return err
		}

		cells := map[CellIndex]CellIndex{} // just a lookup table to quickly see if a cell has already been collected

		switch osmObj := obj.(type) {
		case *osm.Node:
			cellX, cellY = g.getCellIdForCoordinate(osmObj.Lon, osmObj.Lat)
			cellItem := CellIndex{cellX, cellY}
			cells[cellItem] = cellItem
			g.nodeToPositionMap[osmObj.ID] = orb.Point{osmObj.Lon, osmObj.Lat}
		case *osm.Way:
			for i, node := range osmObj.Nodes {
				node.Lon = g.nodeToPositionMap[node.ID][0]
				node.Lat = g.nodeToPositionMap[node.ID][1]
				osmObj.Nodes[i] = node

				cellX, cellY = g.getCellIdForCoordinate(node.Lon, node.Lat)
				cellItem := CellIndex{cellX, cellY}
				if _, ok := cells[cellItem]; !ok {
					cells[cellItem] = cellItem
				}
			}
		}
		// TODO	 Implement relation handling
		//case *osm.Relation:

		for _, cell := range cells {
			err = g.writeOsmObjectToCell(cell[0], cell[1], obj, encodedFeature)
			if err != nil {
				return err
			}
		}
	}

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

	importDuration := time.Since(importStartTime)
	sigolo.Infof("Created OSM object index from OSM data in %s", importDuration)

	return nil
}

func (g *GridIndex) writeOsmObjectToCell(cellX int, cellY int, obj osm.Object, encodedFeature feature.EncodedFeature) error {
	var f io.Writer
	var err error

	sigolo.Tracef("Write OSM object to cell x=%d, y=%d, obj=%#v", cellX, cellY, obj.ObjectID())

	switch osmObj := obj.(type) {
	case *osm.Node:
		f, err = g.getCellFile(cellX, cellY, "node")
		if err != nil {
			return err
		}
		err = g.writeNodeData(osmObj.ID, encodedFeature.(*feature.EncodedNodeFeature), f)
		if err != nil {
			return errors.Wrapf(err, "Unable to write node %d to cell x=%d, y=%d", osmObj.ID, cellX, cellY)
		}
		return nil
	case *osm.Way:
		f, err = g.getCellFile(cellX, cellY, "way")
		if err != nil {
			return err
		}
		err = g.writeWayData(osmObj.ID, osmObj.Nodes, encodedFeature.(*feature.EncodedWayFeature), f)
		if err != nil {
			return errors.Wrapf(err, "Unable to write way %d to cell x=%d, y=%d", osmObj.ID, cellX, cellY)
		}
		return nil
	}
	// TODO	 Implement relation handling
	//case *osm.Relation:

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

func (g *GridIndex) writeNodeData(osmId osm.NodeID, encodedFeature *feature.EncodedNodeFeature, f io.Writer) error {
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
	for i := 0; i < len(encodedFeature.GetKeys()); i++ {
		if encodedFeature.GetKeys()[i] != 0 {
			numKeys = i + 1
		}
	}

	encodedKeyBytes := numKeys                               // Is already a byte-array -> no division by 8 needed
	encodedValueBytes := len(encodedFeature.GetValues()) * 3 // Int array and int = 4 bytes

	headerBytesCount := 8 + 4 + 4 + 4 + 4 // = 24
	byteCount := headerBytesCount
	byteCount += encodedKeyBytes
	byteCount += encodedValueBytes

	data := make([]byte, byteCount)

	point := encodedFeature.Geometry.(*orb.Point)

	binary.LittleEndian.PutUint64(data[0:], uint64(osmId))
	binary.LittleEndian.PutUint32(data[8:], math.Float32bits(float32(point.Lon())))
	binary.LittleEndian.PutUint32(data[12:], math.Float32bits(float32(point.Lat())))
	binary.LittleEndian.PutUint32(data[16:], uint32(numKeys))
	binary.LittleEndian.PutUint32(data[20:], uint32(len(encodedFeature.GetValues())))

	copy(data[headerBytesCount:], encodedFeature.GetKeys()[0:numKeys])
	for i, v := range encodedFeature.GetValues() {
		b := data[headerBytesCount+encodedKeyBytes+i*3:]
		b[0] = byte(v)
		b[1] = byte(v >> 8)
		b[2] = byte(v >> 16)
	}

	sigolo.Tracef("Write feature %d pos=%#v, byteCount=%d, numKeys=%d, numValues=%d", osmId, point, byteCount, numKeys, len(encodedFeature.GetValues()))

	_, err := f.Write(data)
	if err != nil {
		return errors.Wrapf(err, "Unable to write node %d to cell file", osmId)
	}
	return nil
}

func (g *GridIndex) writeWayData(osmId osm.WayID, nodes osm.WayNodes, encodedFeature *feature.EncodedWayFeature, f io.Writer) error {
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
	numKeys := 0
	for i := 0; i < len(encodedFeature.GetKeys()); i++ {
		if encodedFeature.GetKeys()[i] != 0 {
			numKeys = i + 1
		}
	}

	encodedKeyBytes := numKeys                               // Is already a byte-array -> no division by 8 needed
	encodedValueBytes := len(encodedFeature.GetValues()) * 3 // Int array and int = 4 bytes
	nodeIdBytes := len(nodes) * 16                           // Each ID is a 64-bit int

	headerByteCount := 8 + 4 + 4 + 2
	byteCount := headerByteCount
	byteCount += encodedKeyBytes
	byteCount += encodedValueBytes
	byteCount += nodeIdBytes

	data := make([]byte, byteCount)

	/*
		Write header
	*/
	binary.LittleEndian.PutUint64(data[0:], uint64(osmId))
	binary.LittleEndian.PutUint32(data[8:], uint32(numKeys))
	binary.LittleEndian.PutUint32(data[12:], uint32(len(encodedFeature.GetValues())))
	binary.LittleEndian.PutUint16(data[16:], uint16(len(nodes)))

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
	for i, node := range nodes {
		binary.LittleEndian.PutUint64(data[nodeIdStartIndex+i*16:], uint64(node.ID))
		binary.LittleEndian.PutUint32(data[nodeIdStartIndex+i*16+8:], math.Float32bits(float32(node.Lon)))
		binary.LittleEndian.PutUint32(data[nodeIdStartIndex+i*16+12:], math.Float32bits(float32(node.Lat)))
	}

	sigolo.Tracef("Write feature %d byteCount=%d, numKeys=%d, numValues=%d, numNodeIds=%d", osmId, byteCount, numKeys, len(encodedFeature.GetValues()), len(nodes))

	_, err := f.Write(data)
	if err != nil {
		return errors.Wrapf(err, "Unable to write node %d to cell file", osmId)
	}
	return nil
}

// getCellIdsForCoordinate returns the cell ID (i.e. position) for the given coordinate.
func (g *GridIndex) getCellIdForCoordinate(x float64, y float64) (int, int) {
	return int(x / g.CellWidth), int(y / g.CellHeight)
}

func (g *GridIndex) toEncodedFeature(obj osm.Object) (feature.EncodedFeature, error) {
	var tags osm.Tags
	var geometry orb.Geometry
	var osmId uint64

	switch osmObj := obj.(type) {
	case *osm.Node:
		tags = osmObj.Tags
		point := osmObj.Point()
		geometry = &point
		osmId = uint64(osmObj.ID)
	case *osm.Way:
		tags = osmObj.Tags
		lineString := osmObj.LineString()
		geometry = &lineString
		osmId = uint64(osmObj.ID)
	}
	// TODO Implement relation handling
	//case *osm.Relation:

	encodedKeys, encodedValues := g.TagIndex.encodeTags(tags)

	abstractEncodedFeature := feature.AbstractEncodedFeature{
		ID:       osmId,
		Geometry: geometry,
		Keys:     encodedKeys,
		Values:   encodedValues,
	}

	switch osmObj := obj.(type) {
	case *osm.Node:
		return &feature.EncodedNodeFeature{
			AbstractEncodedFeature: abstractEncodedFeature,
		}, nil
	case *osm.Way:
		return &feature.EncodedWayFeature{
			AbstractEncodedFeature: abstractEncodedFeature,
			Nodes:                  osmObj.Nodes,
		}, nil
	}
	// TODO Implement relation handling
	//case *osm.Relation:

	return nil, errors.Errorf("Converting OSM object of type '%s' not supported", obj.ObjectID().Type())
}

func (g *GridIndex) Get(bbox *orb.Bound, objectType string) (chan []feature.EncodedFeature, error) {
	sigolo.Debugf("Get feature from bbox=%#v", bbox)
	minCellX, minCellY := g.getCellIdForCoordinate(bbox.Min.Lon(), bbox.Min.Lat())
	maxCellX, maxCellY := g.getCellIdForCoordinate(bbox.Max.Lon(), bbox.Max.Lat())

	resultChannel := make(chan []feature.EncodedFeature, 10)

	numThreads := 3
	var wg sync.WaitGroup
	wg.Add(numThreads)
	go func() {
		// Group the cells into columns of equal size so that each goroutine below can handle on column.
		cellColumns := maxCellX - minCellX + 1 // min and max are inclusive, therefore +1
		threadColumns := cellColumns / numThreads

		for i := 0; i < numThreads; i++ {
			minColX := minCellX + i*threadColumns
			maxColX := minCellX + (i+1)*threadColumns
			if i != 0 {
				// To prevent overlapping columns
				minColX++
			}
			if i == numThreads-1 {
				// Last column: Make sure it goes til the requested end
				maxColX = maxCellX
			}

			go g.getFeaturesForCells(resultChannel, &wg, bbox, minColX, maxColX, minCellY, maxCellY, objectType)
		}
	}()

	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	return resultChannel, nil
}

func (g *GridIndex) GetNodes(nodes osm.WayNodes) (chan []feature.EncodedFeature, error) {
	cells := map[CellIndex][]uint64{}            // just a lookup table to quickly see if a cell has already been collected
	innerCellBounds := map[CellIndex]orb.Bound{} // just a lookup table to quickly see if a cell has already been collected
	for _, node := range nodes {
		cellX, cellY := g.getCellIdForCoordinate(node.Lon, node.Lat)
		cellItem := CellIndex{cellX, cellY}
		if _, ok := cells[cellItem]; !ok {
			// New cell -> Create it and add first node ID
			cells[cellItem] = []uint64{uint64(node.ID)}
			innerCellBounds[cellItem] = node.Point().Bound()
		} else {
			// Cell has been seen before -> just add the new node ID
			cells[cellItem] = append(cells[cellItem], uint64(node.ID))
			innerCellBounds[cellItem] = innerCellBounds[cellItem].Union(node.Point().Bound())
		}
	}

	var resultChannel = make(chan []feature.EncodedFeature, 10)

	if len(cells) == 0 {
		close(resultChannel)
		return resultChannel, nil
	}

	go func() {
		for cell, nodeIds := range cells {
			innerCellBound := innerCellBounds[cell]
			outputBuffer := make([]feature.EncodedFeature, 1000)
			currentBufferPos := 0

			unfilteredFeatures, err := g.readFeaturesFromCellFile(cell[0], cell[1], feature.OsmObjNode.String())
			if err != nil {
				sigolo.FatalCheck(err)
			}
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

					for j := 0; i < len(nodeIds); i++ {
						if encodedFeature.GetID() == nodeIds[j] {
							outputBuffer[currentBufferPos] = encodedFeature
							currentBufferPos++

							if currentBufferPos == len(outputBuffer)-1 {
								resultChannel <- outputBuffer
								outputBuffer = make([]feature.EncodedFeature, len(outputBuffer))
								currentBufferPos = 0
							}
							break
						}
					}
				}
			}

			resultChannel <- outputBuffer
		}
		close(resultChannel)
	}()

	return resultChannel, nil
}

func (g *GridIndex) getFeaturesForCells(output chan []feature.EncodedFeature, wg *sync.WaitGroup, bbox *orb.Bound, minCellX int, maxCellX int, minCellY int, maxCellY int, objectType string) {
	sigolo.Tracef("Get feature for cell column from=%d, to=%d", minCellX, maxCellX)
	for cellX := minCellX; cellX <= maxCellX; cellX++ {
		for cellY := minCellY; cellY <= maxCellY; cellY++ {
			var featuresInBbox []feature.EncodedFeature

			encodedFeatures, err := g.readFeaturesFromCellFile(cellX, cellY, objectType)
			if err != nil {
				sigolo.FatalCheck(err)
			}

			for i := 0; i < len(encodedFeatures); i++ {
				if encodedFeatures[i] != nil && bbox.Intersects(encodedFeatures[i].GetGeometry().Bound()) {
					featuresInBbox = append(featuresInBbox, encodedFeatures[i])
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

	if _, ok := g.featureCache[cellFileName]; ok {
		sigolo.Tracef("Use features from cache for cell file %s", cellFileName)
	} else {
		sigolo.Tracef("Read cell file %s", cellFileName)
		data, err := os.ReadFile(cellFileName)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, objectType)
		}

		g.featureCache[cellFileName] = []feature.EncodedFeature{}

		readFeatureChannel := make(chan []feature.EncodedFeature)
		go func() {
			for readFeatures := range readFeatureChannel {
				// TODO not-null check needed for the features?
				// TODO Prevent concurrent map writes
				g.featureCache[cellFileName] = append(g.featureCache[cellFileName], readFeatures...)
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
	}

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

		headerBytesCount := 8 + 4 + 4 + 4 + 4 // = 24

		sigolo.Tracef("Read feature pos=%d, id=%d, lon=%f, lat=%f, numKeys=%d, numValues=%d", pos, osmId, lon, lat, numEncodedKeyBytes, numValues)

		encodedValuesBytes := numValues * 3 // Multiplication since each value is an int with 3 bytes

		encodedKeys := make([]byte, numEncodedKeyBytes)
		encodedValues := make([]int, numValues)
		copy(encodedKeys[:], data[pos+headerBytesCount:])

		for i := 0; i < numValues; i++ {
			b := data[pos+headerBytesCount+numEncodedKeyBytes+i*3:]
			encodedValues[i] = int(uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16)
		}

		encodedFeature := &feature.EncodedWayFeature{
			AbstractEncodedFeature: feature.AbstractEncodedFeature{
				ID:       osmId,
				Geometry: &orb.Point{float64(lon), float64(lat)},
				Keys:     encodedKeys,
				Values:   encodedValues,
			},
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

		pos += headerBytesCount + numEncodedKeyBytes + encodedValuesBytes
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
		nodesStartIndex := encodedValuesStartIndex + numValues*3
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

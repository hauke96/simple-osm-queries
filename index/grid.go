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
}

type CellIndex [2]int

func LoadGridIndex(indexBaseFolder string, cellWidth float64, cellHeight float64, checkFeatureValidity bool, tagIndex *TagIndex) *GridIndex {
	return &GridIndex{
		TagIndex:             tagIndex,
		CellWidth:            cellWidth,
		CellHeight:           cellHeight,
		BaseFolder:           path.Join(indexBaseFolder, GridIndexFolder),
		checkFeatureValidity: checkFeatureValidity,
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

	var feature *EncodedFeature
	var cellX, cellY int

	g.cacheFileHandles = map[string]*os.File{}
	g.cacheFileWriters = map[string]*bufio.Writer{}
	g.nodeToPositionMap = map[osm.NodeID]orb.Point{}

	for scanner.Scan() {
		obj := scanner.Object()
		feature = g.toEncodedFeature(obj)
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
			err = g.writeOsmObjectToCell(cell[0], cell[1], obj, feature)
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

func (g *GridIndex) writeOsmObjectToCell(cellX int, cellY int, obj osm.Object, feature *EncodedFeature) error {
	var f io.Writer
	var err error

	sigolo.Tracef("Write OSM object to cell x=%d, y=%d, obj=%#v", cellX, cellY, obj.ObjectID())

	switch osmObj := obj.(type) {
	case *osm.Node:
		f, err = g.getCellFile(cellX, cellY, "node")
		if err != nil {
			return err
		}
		err = g.writeNodeData(osmObj.ID, feature, f)
		if err != nil {
			return errors.Wrapf(err, "Unable to write node %d to cell x=%d, y=%d", osmObj.ID, cellX, cellY)
		}
		return nil
	case *osm.Way:
		f, err = g.getCellFile(cellX, cellY, "way")
		if err != nil {
			return err
		}
		err = g.writeWayData(osmObj.ID, osmObj.Nodes, feature, f)
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

func (g *GridIndex) writeNodeData(osmId osm.NodeID, feature *EncodedFeature, f io.Writer) error {
	/*
		Entry format:
		// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes).

		Names: | osmId | lon | lat | num. keys | num. values |   encodedKeys   |   encodedValues   |
		Bytes: |   8   |  4  |  4  |     4     |      4      | <num. keys> / 8 | <num. values> * 3 |

		The encodedKeys is a bit-string (each key 1 bit), that why the division by 8 happens. The stored value is the
		number of bytes in the keys array of the feature (i.e. "len(feature.keys)"). The encodedValue part, however,
		is an int-array, therefore, we need the multiplication with 4.
	*/

	// The number of key-bins to store is determined by the bin with the highest index that is not empty (i.e. all 0s).
	// If only the first bin contains some 1s (i.e. keys that are set on the feature) and the next 100 bins are empty,
	// then there's no reason to store those empty bins. This reduced the cell-file size for hamburg-latest (45 MB PBF)
	// by a factor of ten!
	numKeys := 0
	for i := 0; i < len(feature.keys); i++ {
		if feature.keys[i] != 0 {
			numKeys = i + 1
		}
	}

	encodedKeyBytes := numKeys                   // Is already a byte-array -> no division by 8 needed
	encodedValueBytes := len(feature.values) * 3 // Int array and int = 4 bytes

	headerBytesCount := 8 + 4 + 4 + 4 + 4 // = 24
	byteCount := headerBytesCount
	byteCount += encodedKeyBytes
	byteCount += encodedValueBytes

	data := make([]byte, byteCount)

	point := feature.Geometry.(orb.Point)

	binary.LittleEndian.PutUint64(data[0:], uint64(osmId))
	binary.LittleEndian.PutUint32(data[8:], math.Float32bits(float32(point.Lon())))
	binary.LittleEndian.PutUint32(data[12:], math.Float32bits(float32(point.Lat())))
	binary.LittleEndian.PutUint32(data[16:], uint32(numKeys))
	binary.LittleEndian.PutUint32(data[20:], uint32(len(feature.values)))

	copy(data[headerBytesCount:], feature.keys[0:numKeys])
	for i, v := range feature.values {
		b := data[headerBytesCount+encodedKeyBytes+i*3:]
		b[0] = byte(v)
		b[1] = byte(v >> 8)
		b[2] = byte(v >> 16)
	}

	sigolo.Tracef("Write feature %d pos=%#v, byteCount=%d, numKeys=%d, numValues=%d", osmId, point, byteCount, numKeys, len(feature.values))

	_, err := f.Write(data)
	if err != nil {
		return errors.Wrapf(err, "Unable to write node %d to cell file", osmId)
	}
	return nil
}

func (g *GridIndex) writeWayData(osmId osm.WayID, nodes osm.WayNodes, feature *EncodedFeature, f io.Writer) error {
	/*
		Entry format:
		// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes).

		Names: | osmId | num. keys | num. values | num. nodes |   encodedKeys   |   encodedValues   |       nodes       |
		Bytes: |   8   |     4     |      4      |      2     | <num. keys> / 8 | <num. values> * 3 | <num. nodes> * 16 |

		The encodedKeys is a bit-string (each key 1 bit), that why the division by 8 happens. The stored value is the
		number of bytes in the keys array of the feature (i.e. "len(feature.keys)"). The encodedValue part, however,
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
	for i := 0; i < len(feature.keys); i++ {
		if feature.keys[i] != 0 {
			numKeys = i + 1
		}
	}

	encodedKeyBytes := numKeys                   // Is already a byte-array -> no division by 8 needed
	encodedValueBytes := len(feature.values) * 3 // Int array and int = 4 bytes
	nodeIdBytes := len(nodes) * 16               // Each ID is a 64-bit int

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
	binary.LittleEndian.PutUint32(data[12:], uint32(len(feature.values)))
	binary.LittleEndian.PutUint16(data[16:], uint16(len(nodes)))

	/*
		Write keys and values
	*/
	copy(data[headerByteCount:], feature.keys[0:numKeys])
	for i, v := range feature.values {
		b := data[headerByteCount+encodedKeyBytes+i*3:]
		b[0] = byte(v)
		b[1] = byte(v >> 8)
		b[2] = byte(v >> 16)
	}

	/*
		Write nodes
	*/
	nodeIdStartIndex := headerByteCount + encodedKeyBytes + len(feature.values)*3
	for i, node := range nodes {
		binary.LittleEndian.PutUint64(data[nodeIdStartIndex+i*16:], uint64(node.ID))
		binary.LittleEndian.PutUint32(data[nodeIdStartIndex+i*16+8:], uint32(float32(node.Lon)))
		binary.LittleEndian.PutUint32(data[nodeIdStartIndex+i*16+12:], uint32(float32(node.Lat)))
	}

	sigolo.Tracef("Write feature %d byteCount=%d, numKeys=%d, numValues=%d, numNodeIds=%d", osmId, byteCount, numKeys, len(feature.values), len(nodes))

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

func (g *GridIndex) toEncodedFeature(obj osm.Object) *EncodedFeature {
	var tags osm.Tags
	var geometry orb.Geometry
	var osmId uint64

	switch osmObj := obj.(type) {
	case *osm.Node:
		tags = osmObj.Tags
		geometry = osmObj.Point()
		osmId = uint64(osmObj.ID)
	case *osm.Way:
		tags = osmObj.Tags
		// TODO Create geometry
		osmId = uint64(osmObj.ID)
	}
	// TODO Implement relation handling
	//case *osm.Relation:

	encodedKeys, encodedValues := g.TagIndex.encodeTags(tags)

	return &EncodedFeature{
		ID:       osmId,
		Geometry: geometry,
		keys:     encodedKeys,
		values:   encodedValues,
	}
}

func (g *GridIndex) Get(bbox *orb.Bound, objectType string) (chan []*EncodedFeature, error) {
	sigolo.Debugf("Get feature from bbox=%#v", bbox)
	minCellX, minCellY := g.getCellIdForCoordinate(bbox.Min.Lon(), bbox.Min.Lat())
	maxCellX, maxCellY := g.getCellIdForCoordinate(bbox.Max.Lon(), bbox.Max.Lat())

	resultChannel := make(chan []*EncodedFeature, 10)

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

			go g.getFeaturesForCells(resultChannel, &wg, minColX, maxColX, minCellY, maxCellY, objectType)
		}
	}()

	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	return resultChannel, nil
}

func (g *GridIndex) getFeaturesForCells(output chan []*EncodedFeature, wg *sync.WaitGroup, minCellX int, maxCellX int, minCellY int, maxCellY int, objectType string) {
	sigolo.Tracef("Get feature for cell column from=%d, to=%d", minCellX, maxCellX)
	for cellX := minCellX; cellX <= maxCellX; cellX++ {
		for cellY := minCellY; cellY <= maxCellY; cellY++ {
			err := g.readFeaturesFromCellFile(output, cellX, cellY, objectType)
			if err != nil {
				sigolo.FatalCheck(err)
			}
		}
	}
	wg.Done()
}

// readFeaturesFromCellFile reads all features from the specified cell and writes them periodically to the output channel.
func (g *GridIndex) readFeaturesFromCellFile(output chan []*EncodedFeature, cellX int, cellY int, objectType string) error {
	cellFolderName := path.Join(g.BaseFolder, objectType, strconv.Itoa(cellX))
	cellFileName := path.Join(cellFolderName, strconv.Itoa(cellY)+".cell")

	if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
		sigolo.Tracef("Cell file %s does not exist, I'll return an empty feature list", cellFileName)
		return nil
	} else if err != nil {
		return errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	sigolo.Tracef("Read cell file %s", cellFileName)
	data, err := os.ReadFile(cellFileName)
	if err != nil {
		return errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, objectType)
	}

	switch objectType {
	case "node":
		g.readNodesFromCellData(output, data)
	case "way":
		g.readWaysFromCellData(output, data)
	default:
		panic("Unsupported object type to read: " + objectType)
	}

	return nil
}

func (g *GridIndex) readNodesFromCellData(output chan []*EncodedFeature, data []byte) {
	outputBuffer := make([]*EncodedFeature, 1000)
	currentBufferPos := 0
	totalReadFeatures := 0

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

		feature := &EncodedFeature{
			ID:       osmId,
			Geometry: &orb.Point{float64(lon), float64(lat)},
			keys:     encodedKeys,
			values:   encodedValues,
		}
		if g.checkFeatureValidity {
			sigolo.Debugf("Check validity of feature %d", feature.ID)
			g.checkValidity(feature)
		}

		outputBuffer[currentBufferPos] = feature
		currentBufferPos++

		if currentBufferPos == len(outputBuffer)-1 {
			output <- outputBuffer
			outputBuffer = make([]*EncodedFeature, len(outputBuffer))
			currentBufferPos = 0
		}

		pos += headerBytesCount + numEncodedKeyBytes + encodedValuesBytes
		totalReadFeatures++
	}

	output <- outputBuffer
}

func (g *GridIndex) readWaysFromCellData(output chan []*EncodedFeature, data []byte) {
	outputBuffer := make([]*EncodedFeature, 1000)
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
		numNodes := int(binary.LittleEndian.Uint32(data[pos+16:]))
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
				Lat: float64(binary.LittleEndian.Uint32(data[lonIndex:])),
				Lon: float64(binary.LittleEndian.Uint32(data[latIndex:])),
			})
		}

		/*
			Actually create the encoded feature
		*/
		feature := &EncodedFeature{
			ID:     osmId,
			keys:   encodedKeys,
			values: encodedValues,
			nodes:  nodes,
			// TODO Build LineString geometry out of nodes
		}
		if g.checkFeatureValidity {
			sigolo.Debugf("Check validity of feature %d", feature.ID)
			g.checkValidity(feature)
		}

		outputBuffer[currentBufferPos] = feature
		currentBufferPos++

		if currentBufferPos == len(outputBuffer)-1 {
			output <- outputBuffer
			outputBuffer = make([]*EncodedFeature, len(outputBuffer))
			currentBufferPos = 0
		}

		pos += headerBytesCount + numEncodedKeyBytes + encodedValuesBytes + nodeBytes
		totalReadFeatures++
	}

	output <- outputBuffer
}

func (g *GridIndex) checkValidity(feature *EncodedFeature) {
	// Check keys
	if len(feature.keys) > len(g.TagIndex.keyMap) {
		sigolo.Fatalf("Invalid length of keys in feature %d: Expected less than %d but found %d", feature.ID, len(g.TagIndex.keyMap), len(feature.keys))
	}

	// Check values
	numberOfSetKeys := 0
	for keyIndex := 0; keyIndex < len(feature.keys)*8; keyIndex++ {
		if feature.HasKey(keyIndex) {
			valueIndex := feature.GetValueIndex(keyIndex)
			if valueIndex > len(g.TagIndex.valueMap[keyIndex])-1 {
				sigolo.Fatalf("Invalid key value found in feature %d: keyIndex=%d, valueIndex=%d, allowedMaxValueIndex=%d", feature.ID, keyIndex, valueIndex, len(g.TagIndex.valueMap[keyIndex])-1)
			}
			numberOfSetKeys++
		}
	}

	if numberOfSetKeys > len(feature.values) {
		sigolo.Fatalf("Invalid number of value indices found in feature %d: Expected %d values but found %d", feature.ID, len(feature.values)-1, numberOfSetKeys)
	}
}

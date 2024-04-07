package index

import (
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
	TagIndex   *TagIndex
	CellWidth  float64
	CellHeight float64
	BaseFolder string
}

func LoadGridIndex(indexBaseFolder string, cellWidth float64, cellHeight float64, tagIndex *TagIndex) *GridIndex {
	return &GridIndex{
		TagIndex:   tagIndex,
		CellWidth:  cellWidth,
		CellHeight: cellHeight,
		BaseFolder: path.Join(indexBaseFolder, GridIndexFolder),
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

	sigolo.Debug("Start processing geometries from input data")
	importStartTime := time.Now()

	var feature *EncodedFeature
	var cellX, cellY int

	for scanner.Scan() {
		obj := scanner.Object()
		switch osmObj := obj.(type) {
		case *osm.Node:
			cellX, cellY = g.getCellIdForCoordinate(osmObj.Lon, osmObj.Lat)
		}
		// TODO Implement way handling
		//case *osm.Way:
		// TODO	 Implement relation handling
		//case *osm.Relation:

		feature = g.toEncodedFeature(obj)
		err = g.writeOsmObjectToCell(cellX, cellY, obj, feature)
		if err != nil {
			return err
		}
	}

	importDuration := time.Since(importStartTime)
	sigolo.Debugf("Created OSM object index from OSM data in %s", importDuration)

	return nil
}

func (g *GridIndex) writeOsmObjectToCell(cellX int, cellY int, obj osm.Object, feature *EncodedFeature) error {
	var f *os.File
	var err error

	sigolo.Tracef("Write OSM object to cell x=%d, y=%d, obj=%s", cellX, cellY, obj.ObjectID().String())

	defer func() {
		if f != nil {
			sigolo.Tracef("Close cell file %s for object x=%d, y=%d, obj=%s", f.Name(), cellX, cellY, obj.ObjectID().String())
			err = f.Close()
			sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for grid-index store %s", f.Name()))
		} else {
			sigolo.Tracef("No cell file to close (x=%d, y=%d, obj=%s), there's probably an error opening/creating it", cellX, cellY, obj.ObjectID().String())
		}
	}()

	switch osmObj := obj.(type) {
	case *osm.Node:
		f, err = g.getCellFile(cellX, cellY, "node")
		if err != nil {
			return err
		}
		err = g.writeNodeData(osmObj.ID, feature, f)
		if err != nil {
			return errors.Wrapf(err, "Unable to write node %d to cell file %s", osmObj.ID, f.Name())
		}
		return nil
	}
	// TODO Implement way handling
	//case *osm.Way:
	// TODO	 Implement relation handling
	//case *osm.Relation:

	return nil
}

func (g *GridIndex) getCellFile(cellX int, cellY int, objectType string) (*os.File, error) {
	cellFolderName := path.Join(g.BaseFolder, objectType, strconv.Itoa(cellX))
	err := os.MkdirAll(cellFolderName, os.ModePerm)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to create cell folder %s for cellY=%d", cellFolderName, cellY)
	}

	cellFileName := path.Join(cellFolderName, strconv.Itoa(cellY)+".cell")

	var file *os.File

	if _, err = os.Stat(cellFileName); err == nil {
		sigolo.Tracef("Cell file %s already exist, I'll open it", cellFileName)
		file, err = os.OpenFile(cellFileName, os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to open cell file %s", cellFileName)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		sigolo.Debugf("Cell file %s does not exist, I'll create it", cellFileName)
		file, err = os.Create(cellFileName)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to create new cell file %s", cellFileName)
		}
	} else {
		return nil, errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	return file, nil
}

func (g *GridIndex) writeNodeData(id osm.NodeID, feature *EncodedFeature, f io.Writer) error {
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
			numKeys = i
		}
	}

	encodedKeyBytes := numKeys                   // Is already a byte-array -> no division by 8 needed
	encodedValueBytes := len(feature.values) * 3 // Int array and int = 4 bytes

	byteCount := 8 + 4 + 4 + 4 + 4 // = 24
	byteCount += encodedKeyBytes
	byteCount += encodedValueBytes

	data := make([]byte, byteCount)

	point := feature.Geometry.(orb.Point)

	binary.LittleEndian.PutUint64(data[0:], uint64(id))
	binary.LittleEndian.PutUint32(data[8:], math.Float32bits(float32(point.Lon())))
	binary.LittleEndian.PutUint32(data[12:], math.Float32bits(float32(point.Lat())))
	binary.LittleEndian.PutUint32(data[16:], uint32(numKeys))
	binary.LittleEndian.PutUint32(data[20:], uint32(len(feature.values)))

	copy(data[24:], feature.keys[:])
	for i, v := range feature.values {
		b := data[24+encodedKeyBytes+i*3:]
		b[0] = byte(v)
		b[1] = byte(v >> 8)
		b[2] = byte(v >> 16)
	}

	sigolo.Tracef("Write feature %d pos=%#v, byteCount=%d, numKeys=%d, numValues=%d", id, point, byteCount, numKeys, len(feature.values))

	_, err := f.Write(data)
	if err != nil {
		return errors.Wrapf(err, "Unable to write node %d to cell file", id)
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
	switch osmObj := obj.(type) {
	case *osm.Node:
		tags = osmObj.Tags
		geometry = orb.Point{osmObj.Lon, osmObj.Lat}
	}
	// TODO Implement way handling
	//case *osm.Way:
	// TODO Implement relation handling
	//case *osm.Relation:

	encodedKeys, encodedValues := g.TagIndex.encodeTags(tags)

	return &EncodedFeature{
		Geometry: geometry,
		keys:     encodedKeys,
		values:   encodedValues,
	}
}

func (g *GridIndex) Get(bbox *orb.Bound, objectType string) (chan []EncodedFeature, error) {
	minCellX, minCellY := g.getCellIdForCoordinate(bbox.Min.Lon(), bbox.Min.Lat())
	maxCellX, maxCellY := g.getCellIdForCoordinate(bbox.Max.Lon(), bbox.Max.Lat())
	sigolo.Infof("minX=%d, maxX=%d", minCellX, maxCellX)

	resultChannel := make(chan []EncodedFeature, 10)

	numThreads := 3
	var wg sync.WaitGroup
	wg.Add(numThreads)
	go func() {
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
			sigolo.Infof("from=%d, to=%d", minColX, maxColX)
			go g.getFeaturesForCells(resultChannel, &wg, minColX, maxColX, minCellY, maxCellY, objectType)
		}

		// min and max are inclusive but we don't want to query a columns twice, therefore +1
		//go g.getFeaturesForCells(resultChannel, &wg, minCellX+(cellColumns/2+1), maxCellX, minCellY, maxCellY, objectType)
	}()

	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	return resultChannel, nil
}

func (g *GridIndex) getFeaturesForCells(output chan []EncodedFeature, wg *sync.WaitGroup, minCellX int, maxCellX int, minCellY int, maxCellY int, objectType string) {
	for cellX := minCellX; cellX <= maxCellX; cellX++ {
		for cellY := minCellY; cellY <= maxCellY; cellY++ {
			err := g.readFeaturesFromCell(output, cellX, cellY, objectType)
			if err != nil {
				sigolo.FatalCheck(err)
			}
		}
	}
	wg.Done()
}

// readFeaturesFromCell reads all features from the specified cell and writes them periodically to the output channel.
func (g *GridIndex) readFeaturesFromCell(output chan []EncodedFeature, cellX int, cellY int, objectType string) error {
	cellFolderName := path.Join(g.BaseFolder, objectType, strconv.Itoa(cellX))
	cellFileName := path.Join(cellFolderName, strconv.Itoa(cellY)+".cell")

	if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
		sigolo.Debugf("Cell file %s does not exist, I'll return an empty feature list", cellFileName)
		return nil
	} else if err != nil {
		return errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	sigolo.Tracef("Read cell file %s", cellFileName)
	data, err := os.ReadFile(cellFileName)
	if err != nil {
		return errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, objectType)
	}

	outputBuffer := make([]EncodedFeature, 1000)
	currentBufferPos := 0

	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeNodeData".
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		lon := math.Float32frombits(binary.LittleEndian.Uint32(data[pos+8:]))
		lat := math.Float32frombits(binary.LittleEndian.Uint32(data[pos+12:]))
		numKeys := int(binary.LittleEndian.Uint32(data[pos+16:]))
		numValues := int(binary.LittleEndian.Uint32(data[pos+20:]))

		sigolo.Tracef("Read feature pos=%d, id=%d, lon=%f, lat=%f, numKeys=%d, numValues=%d", pos, osmId, lon, lat, numKeys, numValues)

		encodedKeyBytes := numKeys          // Division since a bit-string is stored (each key got one bit) and +1 because the plain division on integers is a floor operation.
		encodedValuesBytes := numValues * 3 // Multiplication since each value is an int with 4 bytes

		encodedKeys := make([]byte, numKeys)
		encodedValues := make([]int, numValues)
		copy(encodedKeys[:], data[pos+24:])

		for i := 0; i < numValues; i++ {
			b := data[(pos + 24 + encodedKeyBytes + i*3):]
			encodedValues[i] = int(uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16)
		}

		outputBuffer[currentBufferPos] = EncodedFeature{
			Geometry: &orb.Point{float64(lon), float64(lat)},
			keys:     encodedKeys,
			values:   encodedValues,
		}

		if currentBufferPos == len(outputBuffer)-1 {
			output <- outputBuffer
			outputBuffer = make([]EncodedFeature, len(outputBuffer))
			currentBufferPos = 0
		}

		pos += 24 + encodedKeyBytes + encodedValuesBytes
	}

	output <- outputBuffer

	return nil
}

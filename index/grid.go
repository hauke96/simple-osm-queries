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
	"time"
)

const gridIndexFilename = "./grid-index/"

type GridIndex struct {
	tagIndex    *TagIndex
	cellWidth   float64
	cellHeight  float64
	indexFolder string
}

func LoadGridIndexFromFile(filename string, tagIndex *TagIndex) (*GridIndex, error) {
	//TODO implement me
	panic("implement me")
}

func (g *GridIndex) Import(inputFile string) (GeometryIndex, error) {
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
			return nil, err
		}
	}

	importDuration := time.Since(importStartTime)
	sigolo.Debugf("Created indices from OSM data in %s", importDuration)

	// TODO
	return nil, nil
}

func (g *GridIndex) writeOsmObjectToCell(cellX int, cellY int, obj osm.Object, feature *EncodedFeature) error {
	var f *os.File
	var err error

	defer func() {
		if f != nil {
			err = f.Close()
			sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for grid-index store %s", f.Name()))
		}
	}()

	switch osmObj := obj.(type) {
	case *osm.Node:
		f, err = g.getCellFile(cellX, cellY, "node")
		if err != nil {
			return err
		}
		return g.writeNodeData(osmObj.ID, feature, f)
	}
	// TODO Implement way handling
	//case *osm.Way:
	// TODO	 Implement relation handling
	//case *osm.Relation:

	return nil
}

func (g *GridIndex) getCellFile(cellX int, cellY int, objectType string) (*os.File, error) {
	filename := path.Join(g.indexFolder, objectType, strconv.Itoa(cellX), strconv.Itoa(cellY)+".cell")
	file, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to open cell file %s", filename)
	}
	return file, nil
}

func (g *GridIndex) writeNodeData(id osm.NodeID, feature *EncodedFeature, f io.Writer) error {
	/*
		Entry format:

		Names: | osmId | lon | lat | num. keys | num. values |   encodedKeys   |   encodedValues   |
		Bytes: |   8   |  8  |  8  |     4     |      4      | <num. keys> / 8 | <num. values> * 4 |

		The encodedKeys is a bit-string (each key 1 bit), that why the division by 8 happens. The encodedValue part,
		however, is an int-array, therefore, we need the multiplication with 4.
	*/
	encodedKeyBytes := len(feature.keys) // Is already a byte-array -> no division by 8 needed
	encodedValueBytes := len(feature.values) * 4

	byteCount := 8 + 8 + 8 + 4 + 4
	byteCount += encodedKeyBytes
	byteCount += encodedValueBytes

	data := make([]byte, byteCount)

	point := feature.geometry.(orb.Point)

	binary.LittleEndian.PutUint64(data[0:], uint64(id))
	binary.LittleEndian.PutUint64(data[8:], math.Float64bits(point.Lon()))
	binary.LittleEndian.PutUint64(data[16:], math.Float64bits(point.Lat()))
	binary.LittleEndian.PutUint32(data[24:], uint32(len(feature.keys)))
	binary.LittleEndian.PutUint32(data[28:], uint32(len(feature.values)))
	copy(data[32:], feature.keys[:])
	for i, v := range feature.values {
		binary.LittleEndian.PutUint32(data[32+encodedKeyBytes+i*4:], uint32(v))
	}

	_, err := f.Write(data)
	return err
}

// getCellIdsForCoordinate returns the cell ID (i.e. position) for the given coordinate.
func (g *GridIndex) getCellIdForCoordinate(x float64, y float64) (int, int) {
	return int(x / g.cellWidth), int(y / g.cellHeight)
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

	encodedKeys, encodedValues := g.tagIndex.encodeTags(tags)

	return &EncodedFeature{
		geometry: geometry,
		keys:     encodedKeys,
		values:   encodedValues,
	}
}

func (g *GridIndex) Get(bbox BBOX) chan []EncodedFeature {
	//TODO implement me
	panic("implement me")
}

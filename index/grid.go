package index

import (
	"context"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
	"github.com/paulmach/osm/osmxml"
	"os"
	"strings"
	"time"
)

const gridIndexFilename = "./grid-index/"

type GridIndex struct {
	tagIndex   *TagIndex
	cellWidth  float64
	cellHeight float64
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

	for scanner.Scan() {
		obj := scanner.Object()
		switch osmObj := obj.(type) {
		case *osm.Node:
			feature := g.toEncodedFeature(osmObj)
			cellX, cellY := g.getCellIdForCoordinate(osmObj.Lon, osmObj.Lat)
			// TODO Write feature into cell on disk
		}
		// TODO Implement way handling
		//case *osm.Way:
		// TODO Implement relation handling
		//case *osm.Relation:
	}

	importDuration := time.Since(importStartTime)
	sigolo.Debugf("Created indices from OSM data in %s", importDuration)

	err = g.SaveToFile(gridIndexFilename)
	sigolo.FatalCheck(err)

	// TODO
	return nil, nil
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

func (g *GridIndex) SaveToFile(filename string) error {
	//TODO implement me
	panic("implement me")
}

func (g *GridIndex) LoadFromFile(filename string) (GeometryIndex, error) {
	//TODO implement me
	panic("implement me")
}

func (g *GridIndex) Get(bbox BBOX) chan []EncodedFeature {
	//TODO implement me
	panic("implement me")
}

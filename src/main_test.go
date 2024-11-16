package main

import (
	"soq/importing"
	"testing"
)

func TestMainImport(t *testing.T) {
	importing.Import("../hessen-latest-with-locations.osm.pbf", defaultCellSize, defaultCellSize, indexBaseFolder)
}

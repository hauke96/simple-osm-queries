package main

import (
	"soq/importing"
	"testing"
)

func TestMainImport(t *testing.T) {
	importing.Import("../hamburg-latest.osm.pbf", defaultCellSize, defaultCellSize, indexBaseFolder)
}

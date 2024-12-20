package main

import (
	"soq/importing"
	"testing"
)

func TestMainImport(t *testing.T) {
	importing.Import("../test.osm.pbf", defaultCellSize, defaultCellSize, indexBaseFolder)
}

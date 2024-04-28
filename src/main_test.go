package main

import (
	"soq/importing"
	"testing"
)

func _TestMainImport(t *testing.T) {
	importing.Import("../test.osm.pbf", defaultCellSize, defaultCellSize, indexBaseFolder)
}

package main

import (
	"github.com/hauke96/sigolo/v2"
	"soq/importing"
	"soq/profiler"
	"testing"
)

func TestMainImport(t *testing.T) {
	sigolo.SetDefaultLogLevel(sigolo.LOG_DEBUG)
	importing.Import("../test.osm.pbf", defaultCellSize, defaultCellSize, indexBaseFolder)
	profiler.PrintResults()
}

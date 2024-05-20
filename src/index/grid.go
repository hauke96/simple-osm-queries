package index

import (
	"bufio"
	"context"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
	"github.com/paulmach/osm/osmxml"
	"github.com/pkg/errors"
	"io"
	"os"
	"path"
	"soq/feature"
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
	cacheFileMutexes     map[io.Writer]*sync.Mutex
	cacheFileMutex       *sync.Mutex
	checkFeatureValidity bool
	nodeToWayMap         map[osm.NodeID]osm.Ways             // TODO remove if not needed
	featureCache         map[string][]feature.EncodedFeature // Filename to feature within it
	featureCacheMutex    *sync.Mutex
}

func LoadGridIndex(indexBaseFolder string, cellWidth float64, cellHeight float64, checkFeatureValidity bool, tagIndex *TagIndex) *GridIndex {
	return &GridIndex{
		TagIndex:             tagIndex,
		CellWidth:            cellWidth,
		CellHeight:           cellHeight,
		BaseFolder:           path.Join(indexBaseFolder, GridIndexFolder),
		checkFeatureValidity: checkFeatureValidity,
		featureCache:         map[string][]feature.EncodedFeature{},
		featureCacheMutex:    &sync.Mutex{},
		cacheFileMutex:       &sync.Mutex{},
	}
}

func (g *GridIndex) Import(inputFile string) error {
	err := os.RemoveAll(g.BaseFolder)
	if err != nil {
		return errors.Wrapf(err, "Unable to remove grid-index base folder %s", g.BaseFolder)
	}

	g.cacheFileHandles = map[string]*os.File{}
	g.cacheFileWriters = map[string]*bufio.Writer{}
	g.cacheFileMutexes = map[io.Writer]*sync.Mutex{}
	g.nodeToWayMap = map[osm.NodeID]osm.Ways{}
	g.featureCache = map[string][]feature.EncodedFeature{}
	g.featureCacheMutex = &sync.Mutex{}
	g.cacheFileMutex = &sync.Mutex{}

	cells := map[CellIndex]CellIndex{}

	sigolo.Debug("Read OSM data and write them as raw encoded features")

	err = g.convertOsmToRawEncodedFeatures(inputFile, cells)
	if err != nil {
		return err
	}
	g.closeOpenFileHandles()

	g.addWayIdsToNodesInCells(cells)
	g.closeOpenFileHandles()

	return nil
}

func (g *GridIndex) convertOsmToRawEncodedFeatures(inputFile string, cells map[CellIndex]CellIndex) error {
	sigolo.Info("Start converting OSM data to raw encoded features")
	importStartTime := time.Now()

	file, scanner, err := g.getScanner(inputFile)
	if err != nil {
		return err
	}

	defer file.Close()
	defer scanner.Close()

	var emptyWayIds []osm.WayID
	var emptyRelationIds []osm.RelationID

	firstWayHasBeenProcessed := false

	numThreads := 10
	osmWayQueue := make(chan *osm.Way, numThreads*2)
	osmWaySync := &sync.WaitGroup{}
	for i := 0; i < numThreads; i++ {
		osmWaySync.Add(1)
		go g.createAndWriteRawWayFeature(osmWayQueue, osmWaySync)
	}
	tempEncodedValues := g.TagIndex.newTempEncodedValueArray()

	sigolo.Debug("Start processing nodes (1/2)")
	for scanner.Scan() {
		obj := scanner.Object()

		switch osmObj := obj.(type) {
		case *osm.Node:
			cell := g.GetCellIndexForCoordinate(osmObj.Lon, osmObj.Lat)

			nodeFeature, err := g.toEncodedNodeFeature(osmObj, emptyWayIds, emptyRelationIds, tempEncodedValues)
			sigolo.FatalCheck(err)
			err = g.writeOsmObjectToCell(cell.X(), cell.Y(), nodeFeature)
			sigolo.FatalCheck(err)

			cells[cell] = cell
		case *osm.Way:
			if !firstWayHasBeenProcessed {
				sigolo.Debug("Start processing ways (2/2)")
				firstWayHasBeenProcessed = true
			}

			osmWayQueue <- osmObj
		}
		// TODO	 Implement relation handling
		//case *osm.Relation:
	}

	close(osmWayQueue)
	osmWaySync.Wait()

	importDuration := time.Since(importStartTime)
	sigolo.Infof("Created raw encoded features from OSM data in %s", importDuration)

	return nil
}

func (g *GridIndex) getScanner(inputFile string) (*os.File, osm.Scanner, error) {
	if !strings.HasSuffix(inputFile, ".osm") && !strings.HasSuffix(inputFile, ".pbf") {
		return nil, nil, errors.Errorf("Input file %s must be an .osm or .pbf file", inputFile)
	}

	f, err := os.Open(inputFile)
	sigolo.FatalCheck(err)

	var scanner osm.Scanner
	if strings.HasSuffix(inputFile, ".osm") {
		scanner = osmxml.New(context.Background(), f)
	} else if strings.HasSuffix(inputFile, ".pbf") {
		scanner = osmpbf.New(context.Background(), f, 1)
	}
	return f, scanner, err
}

func (g *GridIndex) createAndWriteRawWayFeature(osmWayChannel chan *osm.Way, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	tempEncodedValues := g.TagIndex.newTempEncodedValueArray()
	var emptyRelationIds []osm.RelationID

	for osmObj := range osmWayChannel {
		wayFeature, err := g.toEncodedWayFeature(osmObj, emptyRelationIds, tempEncodedValues)
		sigolo.FatalCheck(err)

		wayFeatureData := g.getWayData(wayFeature)

		savedCells := map[CellIndex]bool{}
		for _, node := range osmObj.Nodes {
			cell := g.GetCellIndexForCoordinate(node.Lon, node.Lat)

			if _, ok := savedCells[cell]; !ok {
				f, err := g.getCellFile(cell.X(), cell.Y(), feature.OsmObjWay.String())
				sigolo.FatalCheck(err)

				err = g.writeData(wayFeature, wayFeatureData, f)
				sigolo.FatalCheck(err)

				savedCells[cell] = true
			}
		}
	}
}

func (g *GridIndex) closeOpenFileHandles() {
	var err error
	g.cacheFileMutex.Lock()
	sigolo.Debugf("Close remaining open file handles")
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
	g.cacheFileHandles = map[string]*os.File{}
	g.cacheFileWriters = map[string]*bufio.Writer{}
	g.cacheFileMutexes = map[io.Writer]*sync.Mutex{}
	g.cacheFileMutex.Unlock()
}

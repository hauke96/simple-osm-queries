package index

import (
	"bufio"
	"context"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
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

func (g *GridIndex) Import(inputFile string, nodesOfRelations []osm.NodeID, waysOfRelations []osm.WayID) error {
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

	sigolo.Debug("Read OSM data and write them as raw encoded features")

	err, nodeCells := g.convertOsmToRawEncodedFeatures(inputFile, nodesOfRelations, waysOfRelations)
	if err != nil {
		return err
	}
	g.closeOpenFileHandles()

	g.addAdditionalIdsToObjectsInCells(nodeCells)
	g.closeOpenFileHandles()

	return nil
}

// convertOsmToRawEncodedFeatures Reads the input PBF file and converts all OSM objects into raw encoded features and
// writes them into their respective cells. The returned cell map contains all cells that contain nodes.
func (g *GridIndex) convertOsmToRawEncodedFeatures(inputFile string, nodesOfRelations []osm.NodeID, waysOfRelations []osm.WayID) (error, map[CellIndex]CellIndex) {
	sigolo.Info("Start converting OSM data to raw encoded features")
	importStartTime := time.Now()

	file, scanner, err := g.getScanner(inputFile)
	if err != nil {
		return err, nil
	}

	defer file.Close()
	defer scanner.Close()

	var emptyWayIds []osm.WayID
	var emptyRelationIds []osm.RelationID

	nodeCells := map[CellIndex]CellIndex{}

	nodeToBound := map[osm.NodeID]*orb.Bound{}
	for _, nodeId := range nodesOfRelations {
		nodeToBound[nodeId] = nil
	}

	wayToBound := map[osm.WayID]*orb.Bound{}
	for _, wayId := range waysOfRelations {
		wayToBound[wayId] = nil
	}

	firstWayHasBeenProcessed := false
	firstRelationHasBeenProcessed := false

	numThreads := 10
	osmWayQueue := make(chan *osm.Way, numThreads*2)
	osmWaySync := &sync.WaitGroup{}
	for i := 0; i < numThreads; i++ {
		osmWaySync.Add(1)
		go g.createAndWriteRawWayFeature(osmWayQueue, osmWaySync)
	}
	tempEncodedValues := g.TagIndex.newTempEncodedValueArray()

	sigolo.Debug("Start processing nodes (1/3)")
	for scanner.Scan() {
		obj := scanner.Object()

		switch osmObj := obj.(type) {
		case *osm.Node:
			if _, ok := nodeToBound[osmObj.ID]; ok {
				bbox := osmObj.Point().Bound()
				nodeToBound[osmObj.ID] = &bbox
			}

			cell := g.GetCellIndexForCoordinate(osmObj.Lon, osmObj.Lat)

			nodeFeature, err := g.toEncodedNodeFeature(osmObj, emptyWayIds, emptyRelationIds, tempEncodedValues)
			sigolo.FatalCheck(err)
			err = g.writeOsmObjectToCell(cell.X(), cell.Y(), nodeFeature)
			sigolo.FatalCheck(err)

			nodeCells[cell] = cell
		case *osm.Way:
			if !firstWayHasBeenProcessed {
				sigolo.Debug("Start processing ways (2/3)")
				firstWayHasBeenProcessed = true
			}

			if _, ok := wayToBound[osmObj.ID]; ok {
				bbox := osmObj.LineString().Bound()
				wayToBound[osmObj.ID] = &bbox
			}

			osmWayQueue <- osmObj
		case *osm.Relation:
			if !firstRelationHasBeenProcessed {
				sigolo.Debug("Start processing relations (3/3)")
				firstRelationHasBeenProcessed = true
			}

			var bbox *orb.Bound
			var memberBbox *orb.Bound
			var nodeIds []osm.NodeID
			var wayIds []osm.WayID
			var childRelationIds []osm.RelationID

			for _, member := range osmObj.Members {
				switch member.Type {
				case osm.TypeNode:
					id := osm.NodeID(member.Ref)
					nodeIds = append(nodeIds, id)
					memberBbox = nodeToBound[id]
				case osm.TypeWay:
					id := osm.WayID(member.Ref)
					wayIds = append(wayIds, id)
					memberBbox = wayToBound[id]
				case osm.TypeRelation:
					id := osm.RelationID(member.Ref)
					childRelationIds = append(childRelationIds, id)
				}

				if memberBbox == nil {
					// Members can be outside of dataset and therefore do not appear in our map
					continue
				}

				if bbox == nil {
					bbox = memberBbox
				} else {
					newBbox := bbox.Union(*memberBbox)
					bbox = &newBbox
				}
			}

			// TODO handle relations that only contain relations and therefore to not (currently) have a bbox
			if bbox == nil {
				continue
			}

			minCell := g.GetCellIndexForCoordinate(bbox.Min.Lon(), bbox.Min.Lat())
			maxCell := g.GetCellIndexForCoordinate(bbox.Max.Lon(), bbox.Max.Lat())

			for cellX := minCell.X(); cellX <= maxCell.X(); cellX++ {
				for cellY := minCell.Y(); cellY <= maxCell.Y(); cellY++ {
					nodeFeature, err := g.toEncodedRelationFeature(osmObj, bbox, nodeIds, wayIds, childRelationIds, tempEncodedValues)
					sigolo.FatalCheck(err)
					err = g.writeOsmObjectToCell(cellX, cellY, nodeFeature)
					sigolo.FatalCheck(err)
				}
			}
		}
	}

	close(osmWayQueue)
	osmWaySync.Wait()

	importDuration := time.Since(importStartTime)
	sigolo.Infof("Created raw encoded features from OSM data in %s", importDuration)

	return nil, nodeCells
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

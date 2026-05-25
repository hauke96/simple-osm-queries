package importing

import (
	"math"
	"os"
	"soq/common"
	"soq/index"
	"soq/index/importing"
	"soq/index/storage"
	"soq/osm"
	"strings"
	"time"

	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
	"github.com/paulmach/orb/geojson"
	"github.com/pkg/errors"
)

func Import(inputFile string, cellWidth float64, cellHeight float64, baseFolder string) error {
	if !strings.HasSuffix(inputFile, ".osm") && !strings.HasSuffix(inputFile, ".pbf") {
		sigolo.Error("Input file must be an .osm or .pbf file")
		os.Exit(1)
	}

	sigolo.Infof("Start import of OSM data file %s", inputFile)
	importStartTime := time.Now()

	sigolo.Debugf("Remove the index base folder %s", baseFolder)
	err := os.RemoveAll(baseFolder)
	if err != nil {
		return errors.Wrapf(err, "Unable to remove grid-index base folder %s", baseFolder)
	}

	// TODO Idea: Determine node density during tag index creation. The write temp features into the cell-extents instead of one huge file. This prevents reading this huge file over and over again.

	//
	// 1. Create tag index
	//
	sigolo.Info("Create tag-index")
	currentStepStartTime := time.Now()

	tagIndexCreator := index.NewTagIndexCreator()
	osmDensityAggregator := osm.NewOsmDensityAggregator(cellWidth, cellHeight)

	osmReader := osm.NewOsmReader()
	err = osmReader.Read(inputFile, tagIndexCreator, osmDensityAggregator)
	if err != nil {
		return errors.Wrapf(err, "Error importing OSM data")
	}

	sigolo.Debugf("Create and save tag-index")
	tagIndex := tagIndexCreator.CreateTagIndex()
	tagIndex.BaseFolder = baseFolder // TODO Set it here or pass it into some of the above functions?
	err = tagIndex.SaveToFile(index.TagIndexFilename)
	if err != nil {
		return errors.Wrapf(err, "Error writing tag index file to %s", index.TagIndexFilename)
	}
	sigolo.Debugf("Tag-index creation done and stored to disk")

	duration := time.Since(currentStepStartTime)
	sigolo.Infof("Imported OSM data into tag index in %s", duration)

	//
	// 2. Determine sub-extents for temporary features
	//
	sigolo.Info("Determine sub-extents for temporary features")
	cellToNodeCount := osmDensityAggregator.CellToNodeCount

	subExtents := getExtents(cellToNodeCount, 1_000_000, cellWidth, cellHeight)
	sigolo.Debugf("Found %d sub-extents", len(subExtents))

	// TODO Make the GeoJSON creation configurable
	featureCollection := geojson.NewFeatureCollection()
	for _, subExtent := range subExtents {
		geoJsonFeature := geojson.NewFeature(subExtent.ToPolygon(cellWidth, cellHeight))
		featureCollection.Features = append(featureCollection.Features, geoJsonFeature)
	}
	geojsonBytes, err := featureCollection.MarshalJSON()
	if err != nil {
		sigolo.Warnf("Error marshalling sub-extents to GeoJSON: %+v", err)
	} else {
		err = os.WriteFile("./sub-extents.geojson", geojsonBytes, 0644)
		if err != nil {
			sigolo.Warnf("Error writing sub-extent GeoJSON file: %+v", err)
		}
	}

	//
	// 3. Write temp features
	//
	sigolo.Info("Write temporary features")
	currentStepStartTime = time.Now()

	//tmpFeatureRepo := NewTemporaryFeatureRepository(cellWidth, cellHeight, "import-temp-cell")
	//temporaryFeatureImporter := NewTemporaryFeatureImporter(tmpFeatureRepo, tagIndex, subExtents, cellWidth, cellHeight)
	featureStorageWriter := storage.NewFeatureStorageWriter(baseFolder, "index.raw")
	osmToRawFeatureWriter := importing.NewOsmToRawFeaturesImporter(tagIndex, featureStorageWriter, subExtents, cellWidth, cellHeight)

	osmReader = osm.NewOsmReader()
	err = osmReader.Read(inputFile, osmToRawFeatureWriter)
	if err != nil {
		return errors.Wrapf(err, "Error importing OSM data")
	}

	osmReader = nil
	featureStorageWriter = nil
	osmToRawFeatureWriter = nil

	duration = time.Since(currentStepStartTime)
	sigolo.Infof("Imported OSM data into temp features in %s", duration)

	//
	// 4. Read temp features and write them into cells
	//
	sigolo.Info("Read temp features and write them as normal features into cells")
	currentStepStartTime = time.Now()

	sigolo.Debugf("Start processing %d sub-extents", len(subExtents))
	featureStorageWriter = storage.NewFeatureStorageWriter(baseFolder, "index")
	featureStorageReader := storage.NewFeatureStorageReader(baseFolder, "index.raw", cellWidth, cellHeight)
	featureStorageReader.InitRelationCache()
	for i, subExtent := range subExtents {
		currentSubExtentStartTime := time.Now()
		sigolo.Debugf("=== Writing final data  /  Step 1/2 (nodes and ways)  /  sub-extent %d / %d ===", i+1, len(subExtents))
		sigolo.Tracef("Sub-extent bound: [%d/%d, %d/%d]", subExtent.LowerLeftCell().X(), subExtent.LowerLeftCell().Y(), subExtent.UpperRightCell().X(), subExtent.UpperRightCell().Y())

		nodes, ways := featureStorageReader.ReadRawDataWithParentIds(subExtent)

		sizeBeforeWriting := getIndexFileSize(baseFolder)

		/*
			Nodes
		*/
		sizeBeforeWritingPartOfCell := getIndexFileSize(baseFolder)
		for _, node := range nodes {
			err = featureStorageWriter.WriteNodeFeature(node, subExtent)
			sigolo.FatalCheck(errors.Wrapf(err, "Unable to write node %d to final index", node.GetID()))
		}
		err = featureStorageWriter.FlushData()
		sigolo.FatalCheck(errors.Wrap(err, "Unable to flush final data from writer"))
		sizeAfterWritingPartOfCell := getIndexFileSize(baseFolder)
		sigolo.Debugf("Size of %d nodes in cell: %.2f MB", len(nodes), float64(sizeAfterWritingPartOfCell-sizeBeforeWritingPartOfCell)/1024/1024)

		/*
			Ways
		*/
		sizeBeforeWritingPartOfCell = getIndexFileSize(baseFolder)
		for _, way := range ways {
			err = featureStorageWriter.WriteWayFeature(way, subExtent)
			sigolo.FatalCheck(errors.Wrapf(err, "Unable to write way %d to final index", way.GetID()))
		}
		err = featureStorageWriter.FlushData()
		sigolo.FatalCheck(errors.Wrap(err, "Unable to flush final data from writer"))
		sizeAfterWritingPartOfCell = getIndexFileSize(baseFolder)
		sigolo.Debugf("Size of %d ways in cell: %.2f MB", len(ways), float64(sizeAfterWritingPartOfCell-sizeBeforeWritingPartOfCell)/1024/1024)

		duration = time.Since(currentSubExtentStartTime)
		sigolo.Debugf("Processed sub-extent %v in %s", subExtent, duration)

		sizeAfterWriting := getIndexFileSize(baseFolder)
		sigolo.Debugf("Size of Cell: %.2f MB", float64(sizeAfterWriting-sizeBeforeWriting)/1024/1024)
	}

	for i, subExtent := range subExtents {
		sigolo.Debugf("=== Writing final data  /  Step 2/2 (relations)  /  sub-extent %d / %d ===", i+1, len(subExtents))
		sigolo.Tracef("Sub-extent bound: [%d/%d, %d/%d]", subExtent.LowerLeftCell().X(), subExtent.LowerLeftCell().Y(), subExtent.UpperRightCell().X(), subExtent.UpperRightCell().Y())

		/*
			Relations
		*/
		sizeBeforeWritingPartOfCell := getIndexFileSize(baseFolder)
		relationsInExtent := featureStorageReader.GetRelationsInExtent(subExtent)
		for _, relation := range relationsInExtent {
			err = featureStorageWriter.WriteRelationFeature(relation, subExtent)
			sigolo.FatalCheck(errors.Wrapf(err, "Unable to write relation %d to final index", relation.GetID()))
		}
		err = featureStorageWriter.FlushData()
		sigolo.FatalCheck(errors.Wrap(err, "Unable to flush final data from writer"))
		sizeAfterWritingPartOfCell := getIndexFileSize(baseFolder)
		sigolo.Debugf("Size of %d relations in cell: %.2f MB", len(relationsInExtent), float64(sizeAfterWritingPartOfCell-sizeBeforeWritingPartOfCell)/1024/1024)
	}

	duration = time.Since(currentStepStartTime)
	sigolo.Infof("Created grid index in %s", duration)

	duration = time.Since(importStartTime)
	sigolo.Infof("Finished import in %s", duration)

	return nil
}

func getIndexFileSize(baseFolder string) int64 {
	filePath := baseFolder + "/index"
	fi, err := os.Stat(filePath)
	if err != nil {
		sigolo.Warnf("Could not get size of file %s", filePath)
		return -1
	}
	return fi.Size()
}

func getExtents(originalCellToNodeCount map[common.CellIndex]int, nodePerExtentThreshold int, cellWidth float64, cellHeight float64) []common.CellExtent {
	result := []common.CellExtent{}

	// TODO make this configurable:
	toleratedAspectRatio := 4.000001 // 2.0 with some buffer for float inaccuracies

	// Copy original map to not change the import parameter
	cellToNodeCount := make(map[common.CellIndex]int, len(originalCellToNodeCount))
	minCell := common.CellIndex{math.MaxInt32, math.MaxInt32}
	maxCell := common.CellIndex{math.MinInt32, math.MinInt32}
	for cell, count := range originalCellToNodeCount {
		cellToNodeCount[cell] = count
		if cell.X() <= minCell.X() {
			minCell[0] = cell[0]
		}
		if cell.Y() <= minCell.Y() {
			minCell[1] = cell[1]
		}
		if cell.X() >= maxCell.X() {
			maxCell[0] = cell[0]
		}
		if cell.Y() >= maxCell.Y() {
			maxCell[1] = cell[1]
		}
	}

	// Extent stretching over the entire input data
	allCells := make(map[common.CellIndex]bool) // Use map as set. The boolean is no used.
	for x := minCell.X(); x <= maxCell.X(); x++ {
		for y := minCell.Y(); y <= maxCell.Y(); y++ {
			allCells[common.CellIndex{x, y}] = false
		}
	}

	for len(cellToNodeCount) != 0 {
		// Search next start cell that of the low-left-most cell. See sketch below for search pattern.
		startCell := common.CellIndex{math.MaxInt32, math.MaxInt32}
		totalDataAreaWidth := maxCell.X() - minCell.X()
		totalDataAreaHeight := maxCell.Y() - minCell.Y()
		for x := 0; x <= totalDataAreaWidth+totalDataAreaHeight; x++ {
			for y := 0; y <= totalDataAreaHeight && y <= x; y++ {
				/*
					minCell.X() + x - y is used to traverse all possible cells from the bottom left to the top right.

					Sketch:
						X = already checkes
						* = not checked yet
						# = this cell
						% = next cell

						* * * * *
						* * * * *
						% * * * *
						X # * * *
						X X X * *
				*/
				cell := common.CellIndex{minCell.X() + x - y, minCell.Y() + y}
				_, cellIsFreeToUse := cellToNodeCount[cell]
				if cellIsFreeToUse {
					startCell = cell
					break
				}
			}

			if startCell.X() != math.MaxInt32 {
				// Found a start cell
				break
			}
		}
		sigolo.Tracef("Use start cell %+v", startCell)

		// Try to expand the extent. This approach tries to do that in an alternating fashion: First try to expand the
		// extent by 1 cell to the right. Then try the same to the top. Again to the right and so on until the extent
		// covers an area that cannot be expanded without violating the nodePerExtentThreshold.
		newExtent := common.CellExtent{startCell, startCell}
		nodesInNewExtent := cellToNodeCount[startCell]
		for nodesInNewExtent <= nodePerExtentThreshold {
			/*
				Extend right
			*/

			sigolo.Tracef("Try to grow extent %+v in X-direction", newExtent)

			// Go through all cells on the right side of the new extent and check whether the extent can be extended.
			expansionInXDirectionPossible := true
			nodesInNewExtentAfterExpansion := nodesInNewExtent

			// Determine actual aspect ratio of the potentially new geographic area. The +1 is needed, because the
			// .ToPoint returns the lower-left corner of a cell. The +2 is needed, because of the same reason, but
			// additionally we also want to area in case the extent is expanded.
			bottomLeftPoint := newExtent.LowerLeftCell().ToPoint(cellWidth, cellHeight)
			topRightPoint := common.CellIndex{newExtent.UpperRightCell().X() + 2, newExtent.UpperRightCell().Y() + 1}.ToPoint(cellWidth, cellHeight)
			topLeftPoint := orb.Point{bottomLeftPoint.X(), topRightPoint.Y()}
			newWidthInCells := geo.Distance(topLeftPoint, topRightPoint)
			newHeightInCells := geo.Distance(bottomLeftPoint, topLeftPoint)
			aspectRatio := math.Max(newWidthInCells, newHeightInCells) / math.Min(newWidthInCells, newHeightInCells)

			for y := newExtent.LowerLeftCell().Y(); y <= newExtent.UpperRightCell().Y(); y++ {
				cellToCheck := common.CellIndex{newExtent.UpperRightCell().X() + 1, y}
				nodesInNewExtentAfterExpansion += cellToNodeCount[cellToCheck]
				_, cellIsFreeToUse := allCells[cellToCheck]
				if !cellIsFreeToUse || nodesInNewExtentAfterExpansion > nodePerExtentThreshold || aspectRatio > toleratedAspectRatio {
					expansionInXDirectionPossible = false
					break
				}
			}

			if expansionInXDirectionPossible {
				previousExtent := newExtent
				newExtent = common.CellExtent{startCell, common.CellIndex{newExtent.UpperRightCell().X() + 1, newExtent.UpperRightCell().Y()}}
				nodesInNewExtent = nodesInNewExtentAfterExpansion
				sigolo.Tracef("Expanded extent  %+v  -->  %+v  with new node count %d", previousExtent, newExtent, nodesInNewExtent)
			} else {
				sigolo.Tracef("Growing extent %+v in X-direction not possible", newExtent)
			}

			/*
				Extend up
			*/

			sigolo.Tracef("Try to grow extent %+v in Y-direction", newExtent)

			// Go through all cells on the right side of the new extent and check whether the extent can be extended.
			expansionInYDirectionPossible := true
			nodesInNewExtentAfterExpansion = nodesInNewExtent

			// Determine actual aspect ratio of the potentially new geographic area. The +1 is needed, because the
			// .ToPoint returns the lower-left corner of a cell. The +2 is needed, because of the same reason, but
			// additionally we also want to area in case the extent is expanded.
			bottomLeftPoint = newExtent.LowerLeftCell().ToPoint(cellWidth, cellHeight)
			topRightPoint = common.CellIndex{newExtent.UpperRightCell().X() + 1, newExtent.UpperRightCell().Y() + 2}.ToPoint(cellWidth, cellHeight)
			topLeftPoint = orb.Point{bottomLeftPoint.X(), topRightPoint.Y()}
			newWidthInCells = geo.Distance(topLeftPoint, topRightPoint)
			newHeightInCells = geo.Distance(bottomLeftPoint, topLeftPoint)
			aspectRatio = math.Max(newWidthInCells, newHeightInCells) / math.Min(newWidthInCells, newHeightInCells)

			for x := newExtent.LowerLeftCell().X(); x <= newExtent.UpperRightCell().X(); x++ {
				cellToCheck := common.CellIndex{x, newExtent.UpperRightCell().Y() + 1}
				nodesInNewExtentAfterExpansion += cellToNodeCount[cellToCheck]
				_, cellIsFreeToUse := allCells[cellToCheck]
				if !cellIsFreeToUse || nodesInNewExtentAfterExpansion > nodePerExtentThreshold || aspectRatio > toleratedAspectRatio {
					expansionInYDirectionPossible = false
					break
				}
			}

			if expansionInYDirectionPossible {
				previousExtent := newExtent
				newExtent = common.CellExtent{startCell, common.CellIndex{newExtent.UpperRightCell().X(), newExtent.UpperRightCell().Y() + 1}}
				nodesInNewExtent = nodesInNewExtentAfterExpansion
				sigolo.Tracef("Expanded extent  %+v  -->  %+v  with new node count %d", previousExtent, newExtent, nodesInNewExtent)
			} else {
				sigolo.Tracef("Growing extent %+v in Y-direction not possible", newExtent)
			}

			if !expansionInXDirectionPossible && !expansionInYDirectionPossible {
				sigolo.Tracef("Expansion of cell extent %+v was neither in X- nor in Y-direction successful. End expansion.", newExtent)
				break
			}
		}

		for x := newExtent.LowerLeftCell().X(); x <= newExtent.UpperRightCell().X(); x++ {
			for y := newExtent.LowerLeftCell().Y(); y <= newExtent.UpperRightCell().Y(); y++ {
				cellToDelete := common.CellIndex{x, y}
				delete(cellToNodeCount, cellToDelete)
				delete(allCells, cellToDelete)
			}
		}

		result = append(result, newExtent)

		sigolo.Tracef("%d cells remaining", len(cellToNodeCount))
	}

	return result
}

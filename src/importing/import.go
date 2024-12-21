package importing

import (
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb/geojson"
	"github.com/pkg/errors"
	"os"
	"path"
	"soq/common"
	"soq/feature"
	"soq/index"
	"soq/osm"
	"strings"
	"time"
)

func Import(inputFile string, cellWidth float64, cellHeight float64, indexBaseFolder string) error {
	if !strings.HasSuffix(inputFile, ".osm") && !strings.HasSuffix(inputFile, ".pbf") {
		sigolo.Error("Input file must be an .osm or .pbf file")
		os.Exit(1)
	}

	baseFolder := path.Join(indexBaseFolder, index.GridIndexFolder)

	sigolo.Infof("Start import of OSM data file %s", inputFile)
	importStartTime := time.Now()

	// TODO Idea: Determine node density during tag index creation. The write temp features into the cell-extents instead of one huge file. This prevents reading this huge file over and over again.

	//
	// 1. Create tag index
	//
	sigolo.Info("Create tag-index")
	currentStepStartTime := time.Now()

	tagIndexCreator := index.NewTagIndexCreator()
	osmDensityAggregator := osm.NewOsmDensityAggregator(cellWidth, cellHeight)

	osmReader := osm.NewOsmReader()
	err := osmReader.Read(inputFile, tagIndexCreator, osmDensityAggregator)
	if err != nil {
		return errors.Wrapf(err, "Error importing OSM data")
	}

	sigolo.Debugf("Create and save tag-index")
	tagIndex := tagIndexCreator.CreateTagIndex()
	tagIndex.BaseFolder = indexBaseFolder // TODO Set it here or pass it into some of the above functions?
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
	inputDataCellExtent := osmDensityAggregator.InputDataCellExtent

	var subExtents []common.CellExtent

	cellsToProcessedState := map[common.CellIndex]bool{}
	for _, cell := range inputDataCellExtent.GetCellIndices() {
		cellsToProcessedState[cell] = false
	}

	for {
		// Import (2024-11-15) for different file sizes:
		// Hamburg (47 MB): TODO
		// Niedersachsen (675 MB): 4-5m, 4 GB RAM, 2.9 GB temp cell files, 3.8 GB Index
		// Germany (4.2 GB): 4h30m, 16 GB RAM, 32 GB temp cell files, 40 GB Index

		// Experience for a ~500 MB PBF file (2024-11-01):
		//  1_000_000 ~  6 GB RAM / 16 min. / 53 sub-extents
		//  2_000_000 ~  6 GB RAM / 11 min. / 30 sub-extents
		//  5_000_000 ~ 10 GB RAM / 6 min. / 15 sub-extents
		//  6_000_000 ~ 11 GB RAM / 6 min. / 10 sub-extents
		//  7_500_000 ~ 14 GB RAM / 9 min. / 9 sub-extents
		// 10_000_000 ~ 13 GB RAM / 6 min. / 7 sub-extents
		// 20_000_000 ~ 17 GB RAM / 9 min. / 3 sub-extents
		// TODO Make this parameter configurable
		extent := getNextExtent(cellsToProcessedState, cellToNodeCount, 10_000_000)
		if extent == nil {
			break
		}
		subExtents = append(subExtents, *extent)
	}
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

	tmpFeatureRepo := NewTemporaryFeatureRepository(cellWidth, cellHeight, "import-temp-cell")
	temporaryFeatureImporter := NewTemporaryFeatureImporter(tmpFeatureRepo, tagIndex, subExtents, cellWidth, cellHeight)

	osmReader = osm.NewOsmReader()
	err = osmReader.Read(inputFile, temporaryFeatureImporter)
	if err != nil {
		return errors.Wrapf(err, "Error importing OSM data")
	}

	duration = time.Since(currentStepStartTime)
	sigolo.Infof("Imported OSM data into temp features in %s", duration)

	//
	// 4. Read temp features and write them into cells
	//
	sigolo.Info("Read temp features and write them as normal features into cells")
	currentStepStartTime = time.Now()

	sigolo.Debugf("Remove the grid-index base folder %s", baseFolder)
	err = os.RemoveAll(baseFolder)
	if err != nil {
		return errors.Wrapf(err, "Unable to remove grid-index base folder %s", baseFolder)
	}

	sigolo.Debugf("Start processing %d sub-extents", len(subExtents))
	for i, subExtent := range subExtents {
		currentSubExtentStartTime := time.Now()
		sigolo.Debugf("=== Process sub-extent %v (%d / %d) ===", subExtent, i+1, len(subExtents))

		tmpFeatureChannel := make(chan feature.Feature, 1000)
		go tmpFeatureRepo.ReadFeatures(tmpFeatureChannel, subExtent) // TODO error handling
		err = index.ImportTempFeatures(tmpFeatureChannel, baseFolder, cellWidth, cellHeight, subExtent)
		if err != nil {
			return err
		}

		duration = time.Since(currentSubExtentStartTime)
		sigolo.Debugf("Processed sub-extent %v in %s", subExtent, duration)
	}

	duration = time.Since(currentStepStartTime)
	sigolo.Infof("Created grid index in %s", duration)

	duration = time.Since(importStartTime)
	sigolo.Infof("Finished import in %s", duration)

	return nil
}

// getNextExtent determines the next largest extent from the bottom left of the given cell indices. The extent is as
// large as possible without containing more than the given threshold.
func getNextExtent(cellsToProcessedState map[common.CellIndex]bool, cellToNodeCount map[common.CellIndex]int, nodePerExtentThreshold int) *common.CellExtent {
	var startCell *common.CellIndex
	var baseExtent *common.CellExtent

	for cell, _ := range cellsToProcessedState {
		if baseExtent == nil {
			baseExtent = &common.CellExtent{cell, cell}
		} else {
			newExtent := baseExtent.Expand(cell)
			baseExtent = &newExtent
		}
	}

	if baseExtent == nil {
		return nil
	}

	for y := baseExtent.LowerLeftCell().Y(); y <= baseExtent.UpperRightCell().Y() && startCell == nil; y++ {
		for x := baseExtent.LowerLeftCell().X(); x <= baseExtent.UpperRightCell().X() && startCell == nil; x++ {
			cell := common.CellIndex{x, y}
			if !cellsToProcessedState[cell] {
				startCell = &cell
			}
		}
	}

	if startCell == nil {
		return nil
	}
	if cellToNodeCount[*startCell] > nodePerExtentThreshold {
		cellsToProcessedState[*startCell] = true
		return &common.CellExtent{*startCell, *startCell}
	}

	biggestExtent := common.CellExtent{*startCell, *startCell}
	biggestExtentCoveredNodes := cellToNodeCount[*startCell]

	for y := startCell.Y(); y <= baseExtent.UpperRightCell().Y(); y++ {
		for x := startCell.X(); x <= baseExtent.UpperRightCell().X(); x++ {
			extent := common.CellExtent{*startCell, *startCell}
			extent = extent.Expand(common.CellIndex{x, y})

			coveredNodes := 0
			containsAlreadyProcessedCell := false

			for _, c := range extent.GetCellIndices() {
				if cellsToProcessedState[c] {
					containsAlreadyProcessedCell = true
					break
				}

				coveredNodes += cellToNodeCount[c]
			}

			if !containsAlreadyProcessedCell && coveredNodes >= biggestExtentCoveredNodes && coveredNodes <= nodePerExtentThreshold {
				biggestExtent = extent
				biggestExtentCoveredNodes = coveredNodes
			}
		}
	}

	for _, c := range biggestExtent.GetCellIndices() {
		cellsToProcessedState[c] = true
	}

	return &biggestExtent
}

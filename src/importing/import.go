package importing

import (
	"bytes"
	"context"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
	"github.com/paulmach/osm/osmxml"
	"github.com/pkg/errors"
	"os"
	"path"
	"path/filepath"
	"soq/feature"
	"soq/index"
	"strings"
	"time"
)

func Import(inputFile string, cellWidth float64, cellHeight float64, indexBaseFolder string) error {
	if !strings.HasSuffix(inputFile, ".osm") && !strings.HasSuffix(inputFile, ".pbf") {
		sigolo.Error("Input file must be an .osm or .pbf file")
		os.Exit(1)
	}

	sigolo.Infof("Start import of file %s", inputFile)
	importStartTime := time.Now()
	currentStepStartTime := time.Now()

	// TODO Reading the whole file into RAM is problematic for larger datasets. Maybe reading the PBF file twice still works well?
	inputFileData, err := os.ReadFile(inputFile)
	if err != nil {
		return errors.Wrapf(err, "Unable to read input file %s", inputFile)
	}

	duration := time.Since(currentStepStartTime)
	sigolo.Infof("Read input file in %s", duration)

	tagIndex, err := createTagIndex(inputFile, inputFileData, indexBaseFolder)
	sigolo.FatalCheck(err)

	currentStepStartTime = time.Now()
	baseFolder := path.Join(indexBaseFolder, index.GridIndexFolder)
	scannerFactory := func() (osm.Scanner, error) {
		return getOsmScannerFromData(inputFile, inputFileData)
	}

	sigolo.Debugf("Remove the grid-index base folder %s", baseFolder)
	err = os.RemoveAll(baseFolder)
	if err != nil {
		return errors.Wrapf(err, "Unable to remove grid-index base folder %s", baseFolder)
	}

	tempRawFeatureRepo := index.NewRawFeaturesRepository(tagIndex, cellWidth, cellHeight, "import-temp-cell")
	cellToNodeCount, inputDataCellExtent, err := tempRawFeatureRepo.WriteOsmToRawEncodedFeatures(scannerFactory)
	if err != nil {
		return errors.Wrap(err, "Error writing temporary cell for raw features during import")
	}

	var subExtents []index.CellExtent

	cellsToProcessedState := map[index.CellIndex]bool{}
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
		extent := getNextExtent(cellsToProcessedState, cellToNodeCount, 5_000_000)
		if extent == nil {
			break
		}
		subExtents = append(subExtents, *extent)
	}

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

	sigolo.Debugf("Start processing %d sub-extents", len(subExtents))
	for i, subExtent := range subExtents {
		currentSubExtentStartTime := time.Now()
		sigolo.Debugf("Process sub-extent %v (%d / %d)", subExtent, i+1, len(subExtents))

		tempRawFeatureChannel := make(chan feature.EncodedFeature, 1000)
		go tempRawFeatureRepo.ReadFeatures(tempRawFeatureChannel, subExtent) // TODO error handling
		err = index.ImportDataFile(tagIndex, tempRawFeatureChannel, baseFolder, cellWidth, cellHeight, subExtent)
		if err != nil {
			return err
		}

		duration = time.Since(currentSubExtentStartTime)
		sigolo.Debugf("Processed sub-extent %v in %s", subExtent, duration)
	}

	duration = time.Since(currentStepStartTime)
	sigolo.Infof("Created grid index in %s", duration)
	currentStepStartTime = time.Now()

	duration = time.Since(importStartTime)
	sigolo.Infof("Finished import in %s", duration)

	return nil
}

// getNextExtent determines the next largest extent from the bottom left of the given cell indices. The extent is as
// large as possible without containing more than the given threshold.
func getNextExtent(cellsToProcessedState map[index.CellIndex]bool, cellToNodeCount map[index.CellIndex]int, nodePerExtentThreshold int) *index.CellExtent {
	var startCell *index.CellIndex
	var baseExtent *index.CellExtent

	for cell, _ := range cellsToProcessedState {
		if baseExtent == nil {
			baseExtent = &index.CellExtent{cell, cell}
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
			cell := index.CellIndex{x, y}
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
		return &index.CellExtent{*startCell, *startCell}
	}

	biggestExtent := index.CellExtent{*startCell, *startCell}
	biggestExtentCoveredNodes := cellToNodeCount[*startCell]

	for y := startCell.Y(); y <= baseExtent.UpperRightCell().Y(); y++ {
		for x := startCell.X(); x <= baseExtent.UpperRightCell().X(); x++ {
			extent := index.CellExtent{*startCell, *startCell}
			extent = extent.Expand(index.CellIndex{x, y})

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

func createTagIndex(inputFile string, inputFileData []byte, indexBaseFolder string) (*index.TagIndex, error) {
	startTime := time.Now()
	sigolo.Debugf("Create tag index")

	scanner, err := getOsmScannerFromData(inputFile, inputFileData)
	if err != nil {
		return nil, err
	}

	tagIndex := &index.TagIndex{
		BaseFolder: indexBaseFolder,
	}
	err = tagIndex.ImportAndSave(scanner)
	if err != nil {
		return nil, err
	}

	err = scanner.Close()
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to close OSM scanner")
	}

	duration := time.Since(startTime)
	sigolo.Infof("Created tag index and additional data structures in %s", duration)

	return tagIndex, nil
}

func getCellsWithData(inputFile string, inputFileData []byte, cellWidth float64, cellHeight float64) (map[index.CellIndex]index.CellIndex, index.CellExtent, error) {
	startTime := time.Now()
	sigolo.Debugf("Get cells that contain data")

	cellsWithData := map[index.CellIndex]index.CellIndex{}
	var inputDataCellExtent index.CellExtent
	doneCollectingCellsWithData := false

	scanner, err := getOsmScannerFromData(inputFile, inputFileData)
	if err != nil {
		return nil, index.CellExtent{}, err
	}

	for scanner.Scan() && !doneCollectingCellsWithData {
		switch osmObj := scanner.Object().(type) {
		case *osm.Node:
			cell := index.CellIndex{int(osmObj.Lon / cellWidth), int(osmObj.Lat / cellHeight)}
			cellsWithData[cell] = cell
			inputDataCellExtent = inputDataCellExtent.Expand(cell)
		case *osm.Way:
			doneCollectingCellsWithData = true
		}
	}

	err = scanner.Close()
	if err != nil {
		return nil, index.CellExtent{}, errors.Wrapf(err, "Unable to close OSM scanner")
	}

	duration := time.Since(startTime)
	sigolo.Infof("Done determining %d cells with data in %s", len(cellsWithData), duration)

	return cellsWithData, inputDataCellExtent, nil
}

func getOsmScannerFromData(inputFile string, inputFileData []byte) (osm.Scanner, error) {
	if strings.HasSuffix(inputFile, ".osm") {
		return osmxml.New(context.Background(), bytes.NewReader(inputFileData)), nil
	} else if strings.HasSuffix(inputFile, ".pbf") {
		scanner := osmpbf.New(context.Background(), bytes.NewReader(inputFileData), 1)
		return scanner, nil
	}
	return nil, errors.Errorf("Unsupported OSM file type '%s'", filepath.Ext(inputFile))
}

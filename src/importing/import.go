package importing

import (
	"bytes"
	"context"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
	"github.com/paulmach/osm/osmxml"
	"github.com/pkg/errors"
	"os"
	"path"
	"path/filepath"
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

	inputFileData, err := os.ReadFile(inputFile)
	if err != nil {
		return errors.Wrapf(err, "Unable to read input file %s", inputFile)
	}

	duration := time.Since(currentStepStartTime)
	sigolo.Infof("Read input file in %s", duration)
	currentStepStartTime = time.Now()

	scanner, err := getOsmScannerFromData(inputFile, inputFileData)
	if err != nil {
		return err
	}
	tagIndex := &index.TagIndex{
		BaseFolder: indexBaseFolder,
	}
	err, nodesOfRelations, waysOfRelations, _, _ := tagIndex.ImportAndSave(scanner, cellWidth, cellHeight)
	if err != nil {
		return err
	}
	err = scanner.Close()
	if err != nil {
		return errors.Wrapf(err, "Unable to close OSM scanner")
	}

	duration = time.Since(currentStepStartTime)
	sigolo.Infof("Created tag index and additional data structures in %s", duration)
	currentStepStartTime = time.Now()

	baseFolder := path.Join(indexBaseFolder, index.GridIndexFolder)
	err = index.ImportDataFile(inputFile, baseFolder, cellWidth, cellHeight, nodesOfRelations, waysOfRelations, tagIndex)
	if err != nil {
		return err
	}

	duration = time.Since(currentStepStartTime)
	sigolo.Infof("Created grid index in %s", duration)
	currentStepStartTime = time.Now()

	duration = time.Since(importStartTime)
	sigolo.Infof("Finished import in %s", duration)

	return nil
}

func getOsmScannerFromData(inputFile string, inputFileData []byte) (osm.Scanner, error) {
	if strings.HasSuffix(inputFile, ".osm") {
		return osmxml.New(context.Background(), bytes.NewReader(inputFileData)), nil
	} else if strings.HasSuffix(inputFile, ".pbf") {
		return osmpbf.New(context.Background(), bytes.NewReader(inputFileData), 1), nil
	}
	return nil, errors.Errorf("Unsupported OSM file type '%s'", filepath.Ext(inputFile))
}

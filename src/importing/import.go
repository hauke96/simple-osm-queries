package importing

import (
	"github.com/hauke96/sigolo/v2"
	"os"
	"path"
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

	tagIndex := &index.TagIndex{
		BaseFolder: indexBaseFolder,
	}
	err, nodesOfRelations, waysOfRelations := tagIndex.ImportAndSave(inputFile)
	if err != nil {
		return err
	}

	baseFolder := path.Join(indexBaseFolder, index.GridIndexFolder)
	err = index.ImportDataFile(inputFile, baseFolder, cellWidth, cellHeight, nodesOfRelations, waysOfRelations, tagIndex)
	if err != nil {
		return err
	}

	importDuration := time.Since(importStartTime)
	sigolo.Infof("Finished import in %s", importDuration)

	return nil
}

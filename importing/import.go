package importing

import (
	"github.com/hauke96/sigolo/v2"
	"os"
	"path"
	"soq/index"
	"strings"
)

func Import(inputFile string, indexBaseFolder string) {
	if !strings.HasSuffix(inputFile, ".osm") && !strings.HasSuffix(inputFile, ".pbf") {
		sigolo.Error("Input file must be an .osm or .pbf file")
		os.Exit(1)
	}

	tagIndex := &index.TagIndex{
		BaseFolder: indexBaseFolder,
	}
	err := tagIndex.ImportAndSave(inputFile, "tag-index")
	sigolo.FatalCheck(err)

	gridIndex := &index.GridIndex{
		TagIndex:   tagIndex,
		CellWidth:  1,
		CellHeight: 1,
		BaseFolder: path.Join(indexBaseFolder, index.GridIndexFolder),
	}
	_, err = gridIndex.Import(inputFile)
	sigolo.FatalCheck(err)
}

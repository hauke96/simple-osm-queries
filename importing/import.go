package importing

import (
	"github.com/hauke96/sigolo/v2"
	"os"
	"soq/index"
	"strings"
)

func Import(inputFile string) {
	if !strings.HasSuffix(inputFile, ".osm") && !strings.HasSuffix(inputFile, ".pbf") {
		sigolo.Error("Input file must be an .osm or .pbf file")
		os.Exit(1)
	}

	tagIndex := &index.TagIndex{}
	tagIndex.Import(inputFile)

	gridIndex := &index.GridIndex{}
	gridIndex.Import(inputFile)
}

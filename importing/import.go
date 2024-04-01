package importing

import (
	"context"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
	"github.com/paulmach/osm/osmxml"
	"os"
	"soq/storage"
	"strings"
	"time"
)

func Import(inputFile string) {
	if !strings.HasSuffix(inputFile, ".osm") && !strings.HasSuffix(inputFile, ".pbf") {
		sigolo.Error("Input file must be an .osm or .pbf file")
		os.Exit(1)
	}

	f, err := os.Open(inputFile)
	sigolo.FatalCheck(err)
	defer f.Close()

	var scanner osm.Scanner
	if strings.HasSuffix(inputFile, ".osm") {
		scanner = osmxml.New(context.Background(), f)
	} else if strings.HasSuffix(inputFile, ".pbf") {
		scanner = osmpbf.New(context.Background(), f, 1)
	}
	defer scanner.Close()

	sigolo.Debug("Start processing input data")
	importStartTime := time.Now()

	// TODO Use maps to quickly find keys and tags
	var keyMap []string     // [key-index] -> key-string
	var valueMap [][]string // [key-index][value-index] -> value-string

	var tagIndex *storage.TagIndex
	for scanner.Scan() {
		obj := scanner.Object()
		switch osmObj := obj.(type) {
		case *osm.Node:
			for _, tag := range osmObj.Tags {
				// Search for the given key in the key map to get its index
				keyIndex := -1
				for i, k := range keyMap {
					if k == tag.Key {
						keyIndex = i
						break
					}
				}

				if keyIndex != -1 {
					// Key already exists and so does its value map. Check is value already appeared and if not, add it.
					containsValue := false
					for _, value := range valueMap[keyIndex] {
						if value == tag.Value {
							containsValue = true
							break
						}
					}
					if !containsValue {
						// Value not yet seen -> Add to value-map
						valueMap[keyIndex] = append(valueMap[keyIndex], tag.Value)
					}
				} else {
					// Key appeared for the first time -> Create maps and add entry
					keyMap = append(keyMap, tag.Key)
					valueMap = append(valueMap, []string{tag.Value})
				}
			}

			tagIndex = storage.NewTagIndex(keyMap, valueMap)
		}
		// TODO Implement way handling
		//case *osm.Way:
		// TODO Implement relation handling
		//case *osm.Relation:
	}

	importDuration := time.Since(importStartTime)
	sigolo.Tracef("%+v", tagIndex)
	sigolo.Debugf("Created indices from OSM data in %s", importDuration)

	err = tagIndex.Save()
	sigolo.FatalCheck(err)
}

package index

import (
	"bufio"
	"bytes"
	"context"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
	"github.com/paulmach/osm/osmxml"
	"github.com/pkg/errors"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

const TagIndexFilename = "tag-index"
const NotFound = -1

type TagIndex struct {
	BaseFolder string
	keyMap     []string   // The index value of a key is the position in this array.
	valueMap   [][]string // Array index is here the key index. I.e. valueMap[key] contains the list of value strings.

	// Only used during import. These are not persisted and will be nil during query phase (i.e. after reading the tag-
	// index from disk).
	keyReverseMap   map[string]int   // Helper map: key-string -> key-index
	valueReverseMap []map[string]int // Helper map: value-string -> value-index in value[key-index]-array
}

func LoadTagIndex(baseFolder string) (*TagIndex, error) {
	tagIndexFile, err := os.Open(path.Join(baseFolder, TagIndexFilename))
	sigolo.FatalCheck(err)

	defer func() {
		err = tagIndexFile.Close()
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for tag-index store %s", baseFolder))
	}()

	var keyMap []string
	var valueMap [][]string
	scanner := bufio.NewScanner(tagIndexFile)
	buf := make([]byte, 0, 64*1024)   // 64k buffer
	scanner.Buffer(buf, 10*1024*1024) // 10M max buffer size
	lineCounter := 0
	for scanner.Scan() {
		// 0 = key
		// 1 = values separated by "|"
		splitLine := strings.SplitN(scanner.Text(), "=", 2)
		if len(splitLine) != 2 {
			return nil, errors.Errorf("Wrong format of line %d: '=' expected separating key and value list", lineCounter)
		}

		key := splitLine[0]
		values := splitLine[1]
		sigolo.Tracef("Found key=%s with %d values", key, len(values))

		keyMap = append(keyMap, key)
		valueMap = append(valueMap, strings.Split(values, "|"))

		lineCounter++
	}

	if err = scanner.Err(); err != nil {
		return nil, errors.Wrapf(err, "Error while scanning tag-index file %s", baseFolder)
	}

	index := &TagIndex{
		BaseFolder: path.Base(baseFolder),
		keyMap:     keyMap,
		valueMap:   valueMap,
	}
	//index.Print()

	return index, nil
}

func (i *TagIndex) ImportAndSave(inputFile string) error {
	if !strings.HasSuffix(inputFile, ".osm") && !strings.HasSuffix(inputFile, ".pbf") {
		sigolo.Error("Input file must be an .osm or .pbf file")
		os.Exit(1)
	}

	f, err := os.Open(inputFile)
	if err != nil {
		return errors.Wrapf(err, "Unable to open input file %s", inputFile)
	}
	defer f.Close()

	var scanner osm.Scanner
	if strings.HasSuffix(inputFile, ".osm") {
		scanner = osmxml.New(context.Background(), f)
	} else if strings.HasSuffix(inputFile, ".pbf") {
		scanner = osmpbf.New(context.Background(), f, 1)
	}
	defer scanner.Close()

	sigolo.Debug("Start processing tags from input data")
	importStartTime := time.Now()

	var keyMap []string                  // [key-index] -> key-string
	keyReverseMap := map[string]int{}    // Helper map: key-string -> key-index
	var valueMap [][]string              // [key-index][value-index] -> value-string
	var valueReverseMap []map[string]int // Helper array from keyIndex to a map from value-string to value-index (the index in the valueMap[key-index]-array)

	for scanner.Scan() {
		obj := scanner.Object()
		switch osmObj := obj.(type) {
		case *osm.Node:
			for _, tag := range osmObj.Tags {
				// Search for the given key in the key map to get its index
				keyIndex, keyAlreadyStored := keyReverseMap[tag.Key]

				if keyAlreadyStored {
					// Key already exists and so does its value map. Check if value already appeared and if not, add it.
					_, containsValue := valueReverseMap[keyIndex][tag.Value]
					if !containsValue {
						// Value not yet seen -> Add to value-map
						valueMap[keyIndex] = append(valueMap[keyIndex], tag.Value)
						valueReverseMap[keyIndex][tag.Value] = len(valueMap) - 1
					}
				} else {
					// Key appeared for the first time -> Create maps and add entry
					keyMap = append(keyMap, tag.Key)
					keyIndex = len(keyMap) - 1
					keyReverseMap[tag.Key] = keyIndex

					valueMap = append(valueMap, []string{tag.Value})
					valueReverseMap = append(valueReverseMap, map[string]int{})
					valueReverseMap[keyIndex][tag.Value] = 0
				}
			}
		}
		// TODO Implement way handling
		//case *osm.Way:
		// TODO Implement relation handling
		//case *osm.Relation:
	}

	// Make sure the values are sorted so that comparison operators work. We can change the order as we want, because
	// OSM objects are not yet stored, this happens in a separate index.
	for i, values := range valueMap {
		sort.Strings(values)
		valueMap[i] = values
	}

	// Update the newly sorted value reverse map. Otherwise the value indices are all mixed up
	for keyIndex, _ := range keyMap {
		for valueIndex, value := range valueMap[keyIndex] {
			valueReverseMap[keyIndex][value] = valueIndex
		}
	}

	i.keyMap = keyMap
	i.keyReverseMap = keyReverseMap
	i.valueMap = valueMap
	i.valueReverseMap = valueReverseMap

	importDuration := time.Since(importStartTime)
	//i.Print()
	sigolo.Debugf("Created tag-index from OSM data in %s", importDuration)

	return i.SaveToFile(TagIndexFilename)
}

// GetKeyIndexFromKeyString returns the numerical index representation of the given key string and "NotFound" if the key doesn't exist.
func (i *TagIndex) GetKeyIndexFromKeyString(key string) int {
	for idx, k := range i.keyMap {
		if k == key {
			return idx
		}
	}
	return NotFound
}

func (i *TagIndex) GetIndicesFromKeyValueStrings(key string, value string) (int, int) {
	keyIndex := i.GetKeyIndexFromKeyString(key)
	if keyIndex == NotFound {
		return NotFound, NotFound
	}

	for idx, v := range i.valueMap[keyIndex] {
		if v == value {
			return keyIndex, idx
		}
	}
	return NotFound, NotFound
}

// GetKeyFromIndex returns the string representation of the given key index.
func (i *TagIndex) GetKeyFromIndex(key int) string {
	return i.keyMap[key]
}

// GetValueForKey returns the string representation of the given key-value indices and "" is the value doesn't exist.
func (i *TagIndex) GetValueForKey(key int, value int) string {
	if key >= len(i.valueMap) {
		return ""
	}
	valueMap := i.valueMap[key]
	if value >= len(i.valueMap) {
		return ""
	}
	return valueMap[value]
}

func (i *TagIndex) encodeTags(tags osm.Tags) ([]byte, []int) {
	// See EncodedFeature for details on the array that are created here.
	if len(tags) == 0 {
		return []byte{}, []int{}
	}

	encodedKeys := make([]byte, len(i.keyMap)/8+1)

	// Contains the values for each keyIndex, or nil if the key is not set. The empty places will be removed below.
	tempEncodedValues := make([]*int, len(encodedKeys)*8)

	for _, tag := range tags {
		keyIndex := i.keyReverseMap[tag.Key]
		valueIndex := i.valueReverseMap[keyIndex][tag.Value]

		// Set 1 for the given key because it's set
		bin := keyIndex / 8      // Element of the array
		idxInBin := keyIndex % 8 // Bit position within the byte
		encodedKeys[bin] |= 1 << idxInBin
		tempEncodedValues[keyIndex] = &valueIndex
	}

	// Now we know all keys that are set and can determine the order of the values for the array.
	encodedValues := make([]int, len(tags))
	encodedValuesCounter := 0
	for pos := 0; pos < len(tempEncodedValues); pos++ {
		if tempEncodedValues[pos] != nil {
			// Key at "pos" is set -> store its value
			encodedValues[encodedValuesCounter] = *tempEncodedValues[pos]
			encodedValuesCounter++
		}
	}

	return encodedKeys, encodedValues
}

func (i *TagIndex) SaveToFile(filename string) error {
	err := os.RemoveAll(i.BaseFolder)
	if err != nil {
		return errors.Wrapf(err, "Unable to remove tag-index base folder %s", i.BaseFolder)
	}

	err = os.MkdirAll(i.BaseFolder, os.ModePerm)
	if err != nil {
		return errors.Wrapf(err, "Unable to create tag-index base folder %s", i.BaseFolder)
	}

	filepath := path.Join(i.BaseFolder, filename)
	f, err := os.Create(filepath)
	sigolo.FatalCheck(err)

	defer func() {
		err = f.Close()
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for tag-index store %s", filepath))
	}()

	return i.WriteAsString(f)
}

func (i *TagIndex) WriteAsString(f io.Writer) error {
	for keyIndex, values := range i.valueMap {
		valueString := strings.Join(values, "|")
		valueString = strings.ReplaceAll(valueString, "\n", "\\n")

		line := i.keyMap[keyIndex] + "=" + valueString + "\n"
		_, err := f.Write([]byte(line))
		if err != nil {
			return errors.Wrapf(err, "Unable to write to tag-index store %s", TagIndexFilename)
		}
	}
	return nil
}

func (i *TagIndex) Print() {
	if !sigolo.ShouldLogTrace() {
		return
	}
	buffer := bytes.NewBuffer([]byte{})
	err := i.WriteAsString(buffer)
	if err == nil {
		sigolo.Tracef("Tag-index:\n%+v", buffer.String())
	} else {
		sigolo.Tracef("Error writing tag-index to string: %v", err)
	}
}

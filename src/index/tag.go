package index

import (
	"bufio"
	"bytes"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/osm"
	"github.com/pkg/errors"
	"io"
	"os"
	"path"
	"soq/common"
	"strings"
)

const TagIndexFilename = "tag-index"
const NotFound = -1

type TagIndexCreator struct {
	keyMap          []string         // [key-index] -> key-string
	keyReverseMap   map[string]int   // Helper map: key-string -> key-index
	valueMap        [][]string       // [key-index][value-index] -> value-string
	valueReverseMap []map[string]int // Helper array from keyIndex to a map from value-string to value-index (the index in the valueMap[key-index]-array)
}

func NewTagIndexCreator() *TagIndexCreator {
	return &TagIndexCreator{
		keyMap:          []string{},
		keyReverseMap:   map[string]int{},
		valueMap:        [][]string{},
		valueReverseMap: []map[string]int{},
	}
}

func (w *TagIndexCreator) Name() string {
	return "TagIndexCreator"
}

func (t *TagIndexCreator) Init() error {
	return nil
}

func (t *TagIndexCreator) HandleNode(node *osm.Node) error {
	t.addTagsToIndex(node.Tags)
	return nil
}

func (t *TagIndexCreator) HandleWay(way *osm.Way) error {
	t.addTagsToIndex(way.Tags)
	return nil
}

func (t *TagIndexCreator) HandleRelation(relation *osm.Relation) error {
	t.addTagsToIndex(relation.Tags)
	return nil
}

func (t *TagIndexCreator) Done() error {
	// Make sure the values are sorted so that comparison operators work. We can change the order as we want, because
	// OSM objects are not yet stored, this happens in a separate index.
	sigolo.Debug("Sort values for each key")
	for i, values := range t.valueMap {
		t.valueMap[i] = common.Sort(values)
	}

	return nil
}

func (t *TagIndexCreator) CreateTagIndex() *TagIndex {
	return NewTagIndex(t.keyMap, t.valueMap)
}

func (t *TagIndexCreator) addTagsToIndex(tags osm.Tags) {
	// Add the keys and values to the maps for the index.
	for _, tag := range tags {
		// Search for the given key in the key map to get its index
		keyIndex, keyAlreadyStored := t.keyReverseMap[tag.Key]

		if keyAlreadyStored {
			// Key already exists and so does its value map. Check if value already appeared and if not, add it.
			_, containsValue := t.valueReverseMap[keyIndex][tag.Value]
			if !containsValue {
				// Value not yet seen -> Add to value-map
				t.valueMap[keyIndex] = append(t.valueMap[keyIndex], tag.Value)
				t.valueReverseMap[keyIndex][tag.Value] = len(t.valueMap) - 1
			}
		} else {
			// Key appeared for the first time -> Create maps and add entry
			t.keyMap = append(t.keyMap, tag.Key)
			keyIndex = len(t.keyMap) - 1
			t.keyReverseMap[tag.Key] = keyIndex

			t.valueMap = append(t.valueMap, []string{tag.Value})
			t.valueReverseMap = append(t.valueReverseMap, map[string]int{})
			t.valueReverseMap[keyIndex][tag.Value] = 0
		}
	}
}

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

	reader := bufio.NewReader(tagIndexFile)
	lineCounter := 0
	nextLinePartBytes, isPrefix, err := reader.ReadLine()
	for err == nil {
		lineBytes := make([]byte, len(nextLinePartBytes))
		copy(lineBytes, nextLinePartBytes)

		// The line might be very long and only the first part is returned. Therefore, we need to collect the rest of the line.
		for isPrefix && err == nil {
			nextLinePartBytes, isPrefix, err = reader.ReadLine()
			lineBytes = append(lineBytes, nextLinePartBytes...)
		}
		line := string(lineBytes)

		splitLine := strings.SplitN(line, "=", 2)
		if len(splitLine) != 2 {
			lineStart := line
			if len(lineStart) > 100 {
				lineStart = lineStart[0:100]
			}
			return nil, errors.Errorf("Wrong format of line %d: '=' expected separating key and value list. Start of line was: %s", lineCounter, lineStart)
		}

		key := splitLine[0]
		values := splitLine[1]
		values = strings.ReplaceAll(values, "$$NEWLINE$$", "\n")
		values = strings.ReplaceAll(values, "$$EQUAL$$", "=")
		valueEntries := strings.Split(values, "|")
		for j, value := range valueEntries {
			valueEntries[j] = strings.ReplaceAll(value, "$$PIPE$$", "|")
		}
		sigolo.Tracef("Found key=%s with %d values", key, len(valueEntries))

		keyMap = append(keyMap, key)
		valueMap = append(valueMap, valueEntries)

		lineCounter++
		nextLinePartBytes, isPrefix, err = reader.ReadLine()
	}

	index := &TagIndex{
		BaseFolder: path.Base(baseFolder),
		keyMap:     keyMap,
		valueMap:   valueMap,
	}

	return index, nil
}

func NewTagIndex(keyMap []string, valueMap [][]string) *TagIndex {
	index := &TagIndex{
		keyMap:   keyMap,
		valueMap: valueMap,
	}

	index.keyReverseMap = map[string]int{}
	for i, k := range index.keyMap {
		index.keyReverseMap[k] = i
	}
	index.updateValueReverseMap()

	return index
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

// GetNextLowerValueIndexForKey returns the next smaller value for the given key-index and value. A return value of -1
// means, that the given value is lower than the lowest value for the given key. The boolean is set to "false" when a
// smaller value has been found. If the exact value exists, then the exact value will be returned with the boolean set
// to "true".
func (i *TagIndex) GetNextLowerValueIndexForKey(key int, value string) (int, bool) {
	for idx, v := range i.valueMap[key] {
		if v == value {
			return idx, true
		}
		if common.IsLessThan(value, v) {
			// This is the first value from the map that is larger than the given one -> the previous value is therefore
			// the next lower one for the given parameter. This returns -1 is the given value is lower than the lowest
			// value for the given key.
			return idx - 1, false
		}
	}

	// Found no larger one -> The largest value in the value map for the given key is the next smaller one for the given parameter.
	return len(i.valueMap[key]) - 1, false
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
	if value >= len(valueMap) {
		return ""
	}
	return valueMap[value]
}

// NewTempEncodedValueArray creates a new int array, which is used as temporary storage during the EncodeTags function.
// Creating this array manually is a performance enhancement, since it can be reused.
func (i *TagIndex) NewTempEncodedValueArray() []int {
	return make([]int, len(i.keyMap)+8)
}

// EncodeTags returns the encoded keys and values. The tempEncodedValues array can be reused to enhance performance
// by not allocating a new array for each call of this function.
func (i *TagIndex) EncodeTags(tags osm.Tags) ([]int, []int) {
	numberOfTags := len(tags)
	if numberOfTags == 0 {
		return []int{}, []int{}
	}

	encodedKeys := make([]int, numberOfTags)
	encodedValues := make([]int, numberOfTags)
	for pos := 0; pos < numberOfTags; pos++ {
		keyIndex := i.keyReverseMap[tags[pos].Key]
		valueIndex := i.valueReverseMap[keyIndex][tags[pos].Value]

		encodedKeys[pos] = keyIndex
		encodedValues[pos] = valueIndex
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
		sigolo.Tracef("Flush tag-index file %s to disk", filepath)
		err = f.Close()
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for tag-index store %s", filepath))
	}()

	sigolo.Debugf("Write tag-index to %s", filepath)
	return i.WriteAsString(f)
}

func (i *TagIndex) WriteAsString(f io.Writer) error {
	for keyIndex, values := range i.valueMap {
		for j, value := range values {
			values[j] = strings.ReplaceAll(value, "|", "$$PIPE$$")
		}
		valueString := strings.Join(values, "|")
		valueString = strings.ReplaceAll(valueString, "\n", "$$NEWLINE$$")
		valueString = strings.ReplaceAll(valueString, "=", "$$EQUAL$$")

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
		sigolo.Tracef("Error writing tag-index to string: %+v", err)
	}
}

func (i *TagIndex) updateValueReverseMap() {
	i.valueReverseMap = make([]map[string]int, len(i.keyMap))
	for keyIndex, _ := range i.keyMap {
		i.valueReverseMap[keyIndex] = map[string]int{}
		for valueIndex, value := range i.valueMap[keyIndex] {
			i.valueReverseMap[keyIndex][value] = valueIndex
		}
	}
}

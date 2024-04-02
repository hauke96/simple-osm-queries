package index

import (
	"bufio"
	"bytes"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/osm"
	"github.com/pkg/errors"
	"io"
	"os"
	"sort"
	"strings"
)

const tagIndexFilename = "./tag-index"
const NotFound = -1

type TagIndex struct {
	keyMap   []string   // The index value of a key is the position in this array.
	valueMap [][]string // Array index is here the key index. I.e. valueMap[key] contains the list of value strings.
}

func NewTagIndex(keyMap []string, valueMap [][]string) *TagIndex {
	for i, values := range valueMap {
		sort.Strings(values)
		valueMap[i] = values
	}

	return &TagIndex{
		keyMap:   keyMap,
		valueMap: valueMap,
	}
}

func LoadTagIndex() (*TagIndex, error) {
	f, err := os.Open(tagIndexFilename)
	sigolo.FatalCheck(err)

	defer func() {
		err = f.Close()
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for tag-index store %s", tagIndexFilename))
	}()

	var keyMap []string
	var valueMap [][]string
	scanner := bufio.NewScanner(f)
	lineCounter := 0
	for scanner.Scan() {
		// 0 = key
		// 1 = values separated by "|"
		splitLine := strings.Split(scanner.Text(), "=")
		if len(splitLine) != 2 {
			return nil, errors.Errorf("Wrong format of line %d: '=' expected separating key and value list", lineCounter)
		}

		key := splitLine[0]
		values := splitLine[1]
		sigolo.Tracef("Found key=%s with values=%s", key, values)

		keyMap = append(keyMap, key)
		valueMap = append(valueMap, strings.Split(values, "|"))

		lineCounter++
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Wrapf(err, "Error while scanning tag-index file %s", tagIndexFilename)
	}

	index := NewTagIndex(keyMap, valueMap)
	index.Print()

	return index, nil
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

	encodedKeys := make([]byte, len(i.keyMap)/8+1)
	bitPosToValue := map[int]int{} // Position in bit-string to numeric representation of value
	for _, tag := range tags {
		keyIndex, valueIndex := i.GetIndicesFromKeyValueStrings(tag.Key, tag.Value)

		// Set 1 for the given key because it's set
		bin := keyIndex / 8      // Element of the array
		idxInBin := keyIndex % 8 // Bit position within the byte
		encodedKeys[bin] |= 1 << idxInBin

		// Store encoded value for later
		bitPosToValue[keyIndex] = valueIndex
	}

	// Now we know all keys that are set and can determine the order of the values for the array.
	encodedValues := make([]int, len(tags))
	setKeyCounter := 0
	for pos := 0; pos < len(i.keyMap); pos++ {
		bin := pos / 8      // Element of the array
		idxInBin := pos % 8 // Bit position within the byte
		if encodedKeys[bin] & 1 << idxInBin {
			// Key at "pos" is set -> store its value
			encodedValues[setKeyCounter] = bitPosToValue[pos]
			setKeyCounter++
		}
	}

	return encodedKeys, encodedValues
}

func (i *TagIndex) SaveToFile() error {
	f, err := os.Create(tagIndexFilename)
	sigolo.FatalCheck(err)

	defer func() {
		err = f.Close()
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for tag-index store %s", tagIndexFilename))
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
			return errors.Wrapf(err, "Unable to write to tag-index store %s", tagIndexFilename)
		}
	}
	return nil
}

func (i *TagIndex) Print() {
	if sigolo.GetCurrentLogLevel() > sigolo.LOG_TRACE {
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

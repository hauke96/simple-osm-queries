package index

import (
	"bufio"
	"bytes"
	"github.com/hauke96/sigolo/v2"
	"github.com/pkg/errors"
	"io"
	"os"
	"sort"
	"strings"
)

const filename = "./tag-index"
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
	f, err := os.Open(filename)
	sigolo.FatalCheck(err)

	defer func() {
		err = f.Close()
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for tag-index store %s", filename))
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

		keyMap = append(keyMap, splitLine[0])
		valueMap = append(valueMap, strings.Split(splitLine[0], "|"))

		lineCounter++
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Wrapf(err, "Error while scanning tag-index file %s", filename)
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

func (i *TagIndex) GetValueIndexFromKeyValueStrings(key string, value string) int {
	keyIndex := i.GetKeyIndexFromKeyString(key)
	if keyIndex == NotFound {
		return NotFound
	}

	for idx, v := range i.valueMap[keyIndex] {
		if v == value {
			return idx
		}
	}
	return NotFound
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

func (i *TagIndex) SaveToFile() error {
	f, err := os.Create(filename)
	sigolo.FatalCheck(err)

	defer func() {
		err = f.Close()
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for tag-index store %s", filename))
	}()

	for keyIndex, values := range i.valueMap {
		valueString := strings.Join(values, "|")
		valueString = strings.ReplaceAll(valueString, "\n", "\\n")

		_, err = f.WriteString(i.keyMap[keyIndex] + "=" + valueString + "\n")
		if err != nil {
			return errors.Wrapf(err, "Unable to write to tag-index store %s", filename)
		}
	}

	return i.WriteAsString(f)
}

func (i *TagIndex) WriteAsString(f io.Writer) error {
	for keyIndex, values := range i.valueMap {
		valueString := strings.Join(values, "|")
		valueString = strings.ReplaceAll(valueString, "\n", "\\n")

		line := i.keyMap[keyIndex] + "=" + valueString + "\n"
		_, err := f.Write([]byte(line))
		if err != nil {
			return errors.Wrapf(err, "Unable to write to tag-index store %s", filename)
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

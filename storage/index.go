package storage

type TagIndex struct {
	keyMap   []string   // The index value of a key is the position in this array.
	valueMap [][]string // Array index is here the key index. I.e. valueMap[key] contains the list of value strings.
}

func NewTagIndex(keyMap []string, valueMap [][]string) *TagIndex {
	return &TagIndex{
		keyMap:   keyMap,
		valueMap: valueMap,
	}
}

// GetIndexFromKey returns the numerical index representation of the given key string and -1 if the key doesn't exist.
func (i *TagIndex) GetIndexFromKey(key string) int {
	for i, k := range i.keyMap {
		if k == key {
			return i
		}
	}
	return -1
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

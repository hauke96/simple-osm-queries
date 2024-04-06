package index

import "github.com/paulmach/orb"

type EncodedFeature struct {
	Geometry orb.Geometry // TODO Own geometry for easier (de)serialization?

	// A bit-string defining which keys are set and which aren't. A 1 at index i says that the key with numeric
	// representation i is set.
	keys []byte

	// A list of all set values. The i-th entry corresponds to the i-th 1 in the keys bit-string and holds the numeric
	// representation of the value. This means the amount of entries in this array is equal to the amount of ones in
	// the keys bit-string.
	values []int
}

func (f *EncodedFeature) HasTag(keyIndex int, valueIndex int) bool {
	bin := keyIndex / 8      // Element of the array
	idxInBin := keyIndex % 8 // Bit position within the byte
	isKeySet := f.keys[bin]&(1<<idxInBin) == 0
	if !isKeySet {
		return false
	}

	// Go through all bits to count the number of 1's.
	// TODO This can probably be optimised by preprocessing this (i.e. map from keyIndex to position in values array)
	valueIndexPosition := 0
	for i := 0; i < keyIndex; i++ {
		bin = i / 8      // Element of the array
		idxInBin = i % 8 // Bit position within the byte
		if f.keys[bin]&(1<<idxInBin) == 0 {
			// Key at "i" is set -> store its value
			valueIndexPosition++
		}
	}

	return f.values[valueIndexPosition] == valueIndex
}

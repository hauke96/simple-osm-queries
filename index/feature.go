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

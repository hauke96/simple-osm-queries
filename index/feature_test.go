package index

import (
	"soq/util"
	"testing"
)

func TestEncodedFeature_GetValueIndex(t *testing.T) {
	// Arrange
	feature := EncodedFeature{
		Geometry: nil,
		keys:     []byte{138}, // Little endian: 0101 0001 -> available key indices are 1, 3 and 7
		values:   []int{5, 6, 7},
	}

	// Act & Assert
	util.AssertTrue(t, feature.HasKey(1))
	value1 := feature.GetValueIndex(1)
	util.AssertEqual(t, 5, value1)

	util.AssertTrue(t, feature.HasKey(3))
	value3 := feature.GetValueIndex(3)
	util.AssertEqual(t, 6, value3)

	util.AssertTrue(t, feature.HasKey(7))
	value7 := feature.GetValueIndex(7)
	util.AssertEqual(t, 7, value7)

	util.AssertFalse(t, feature.HasKey(0))
	util.AssertFalse(t, feature.HasKey(2))
	util.AssertFalse(t, feature.HasKey(4))
	util.AssertFalse(t, feature.HasKey(5))
	util.AssertFalse(t, feature.HasKey(6))
}

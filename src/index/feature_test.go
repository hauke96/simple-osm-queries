package index

import (
	"soq/common"
	"testing"
)

func TestEncodedFeature_GetValueIndex(t *testing.T) {
	// Arrange
	feature := EncodedNodeFeature{
		AbstractEncodedFeature: AbstractEncodedFeature{
			Geometry: nil,
			Keys:     []byte{138}, // Little endian: 0101 0001 -> available key indices are 1, 3 and 7
			Values:   []int{5, 6, 7},
		},
	}

	// Act & Assert
	common.AssertTrue(t, feature.HasKey(1))
	value1 := feature.GetValueIndex(1)
	common.AssertEqual(t, 5, value1)

	common.AssertTrue(t, feature.HasKey(3))
	value3 := feature.GetValueIndex(3)
	common.AssertEqual(t, 6, value3)

	common.AssertTrue(t, feature.HasKey(7))
	value7 := feature.GetValueIndex(7)
	common.AssertEqual(t, 7, value7)

	common.AssertFalse(t, feature.HasKey(0))
	common.AssertFalse(t, feature.HasKey(2))
	common.AssertFalse(t, feature.HasKey(4))
	common.AssertFalse(t, feature.HasKey(5))
	common.AssertFalse(t, feature.HasKey(6))
}

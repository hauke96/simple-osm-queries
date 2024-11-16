package index

import (
	"soq/common"
	"testing"
)

func TestTag_GetNextLowerValueIndexForKey(t *testing.T) {
	// Arrange
	tagIndex := &TagIndex{
		valueMap: [][]string{
			{"v1", "v2", "v4"},
		},
	}

	// Act & Assert
	valueIndex, foundExactValue := tagIndex.GetNextLowerValueIndexForKey(0, "v1")
	common.AssertEqual(t, 0, valueIndex)
	common.AssertTrue(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "v2")
	common.AssertEqual(t, 1, valueIndex)
	common.AssertTrue(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "v4")
	common.AssertEqual(t, 2, valueIndex)
	common.AssertTrue(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "v3")
	common.AssertEqual(t, 1, valueIndex)
	common.AssertFalse(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "v0")
	common.AssertEqual(t, NotFound, valueIndex)
	common.AssertFalse(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "v5")
	common.AssertEqual(t, 2, valueIndex)
	common.AssertFalse(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "v2.5")
	common.AssertEqual(t, 1, valueIndex)
	common.AssertFalse(t, foundExactValue)
}

func TestTag_GetNextLowerValueIndexForKey_mixedNumbersAndStrings(t *testing.T) {
	// Arrange
	tagIndex := &TagIndex{
		valueMap: [][]string{
			{"1", "1m", "1.5 m", "2.5"},
		},
	}

	// Act & Assert
	valueIndex, foundExactValue := tagIndex.GetNextLowerValueIndexForKey(0, "1")
	common.AssertEqual(t, 0, valueIndex)
	common.AssertTrue(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "1m")
	common.AssertEqual(t, 1, valueIndex)
	common.AssertTrue(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "1.5 m")
	common.AssertEqual(t, 2, valueIndex)
	common.AssertTrue(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "1.5")
	common.AssertEqual(t, 1, valueIndex)
	common.AssertFalse(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "0")
	common.AssertEqual(t, NotFound, valueIndex)
	common.AssertFalse(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "2")
	common.AssertEqual(t, 2, valueIndex)
	common.AssertFalse(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "2m")
	common.AssertEqual(t, 2, valueIndex)
	common.AssertFalse(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "2.5m")
	common.AssertEqual(t, 3, valueIndex)
	common.AssertFalse(t, foundExactValue)
}

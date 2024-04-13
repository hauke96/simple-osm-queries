package index

import (
	"soq/util"
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
	util.AssertEqual(t, 0, valueIndex)
	util.AssertTrue(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "v2")
	util.AssertEqual(t, 1, valueIndex)
	util.AssertTrue(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "v4")
	util.AssertEqual(t, 2, valueIndex)
	util.AssertTrue(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "v3")
	util.AssertEqual(t, 1, valueIndex)
	util.AssertFalse(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "v0")
	util.AssertEqual(t, NotFound, valueIndex)
	util.AssertFalse(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "v5")
	util.AssertEqual(t, 2, valueIndex)
	util.AssertFalse(t, foundExactValue)

	valueIndex, foundExactValue = tagIndex.GetNextLowerValueIndexForKey(0, "v2.5")
	util.AssertEqual(t, 1, valueIndex)
	util.AssertFalse(t, foundExactValue)
}

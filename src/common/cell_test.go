package common

import (
	"testing"
)

func TestGridCellIndex_isBelowOrLeftOf(t *testing.T) {
	cell := CellIndex{10, 10}
	/*
		[ 9,11]   [10,11]   [11,11]

		[ 9,10]   [10,10]   [11,10]

		[ 9, 9]   [10, 9]   [11, 9]
	*/

	// First Column
	AssertTrue(t, cell.isBelowOrLeftOf(CellIndex{9, 11}))
	AssertFalse(t, cell.isBelowOrLeftOf(CellIndex{9, 10}))
	AssertFalse(t, cell.isBelowOrLeftOf(CellIndex{9, 9}))

	// Second column
	AssertTrue(t, cell.isBelowOrLeftOf(CellIndex{10, 11}))
	AssertFalse(t, cell.isBelowOrLeftOf(CellIndex{10, 10}))
	AssertFalse(t, cell.isBelowOrLeftOf(CellIndex{10, 9}))

	// Third column
	AssertTrue(t, cell.isBelowOrLeftOf(CellIndex{11, 11}))
	AssertTrue(t, cell.isBelowOrLeftOf(CellIndex{11, 10}))
	AssertTrue(t, cell.isBelowOrLeftOf(CellIndex{11, 9}))
}

func TestGridCellIndex_isAboveOrRightOf(t *testing.T) {
	cell := CellIndex{10, 10}
	/*
		[ 9,11]   [10,11]   [11,11]

		[ 9,10]   [10,10]   [11,10]

		[ 9, 9]   [10, 9]   [11, 9]
	*/

	// First Column
	AssertTrue(t, cell.isAboveOrRightOf(CellIndex{9, 11}))
	AssertTrue(t, cell.isAboveOrRightOf(CellIndex{9, 10}))
	AssertTrue(t, cell.isAboveOrRightOf(CellIndex{9, 9}))

	// Second column
	AssertFalse(t, cell.isAboveOrRightOf(CellIndex{10, 11}))
	AssertFalse(t, cell.isAboveOrRightOf(CellIndex{10, 10}))
	AssertTrue(t, cell.isAboveOrRightOf(CellIndex{10, 9}))

	// Third column
	AssertFalse(t, cell.isAboveOrRightOf(CellIndex{11, 11}))
	AssertFalse(t, cell.isAboveOrRightOf(CellIndex{11, 10}))
	AssertTrue(t, cell.isAboveOrRightOf(CellIndex{11, 9}))
}

func TestGridCellExtent_expand(t *testing.T) {
	extent := CellExtent{CellIndex{10, 10}, CellIndex{20, 20}}

	AssertEqual(t, extent, extent.Expand(CellIndex{10, 10}))
	AssertEqual(t, extent, extent.Expand(CellIndex{15, 15}))
	AssertEqual(t, extent, extent.Expand(CellIndex{20, 20}))

	AssertEqual(t, CellExtent{CellIndex{9, 9}, CellIndex{20, 20}}, extent.Expand(CellIndex{9, 9}))
	AssertEqual(t, CellExtent{CellIndex{10, 10}, CellIndex{21, 21}}, extent.Expand(CellIndex{21, 21}))
	AssertEqual(t, CellExtent{CellIndex{9, 10}, CellIndex{20, 21}}, extent.Expand(CellIndex{9, 21}))
	AssertEqual(t, CellExtent{CellIndex{10, 9}, CellIndex{21, 20}}, extent.Expand(CellIndex{21, 9}))
}

func TestGridCellExtent_contains(t *testing.T) {
	extent := CellExtent{
		CellIndex{10, 10},
		CellIndex{20, 20},
	}

	// Lower-left corner
	AssertFalse(t, extent.Contains(CellIndex{9, 11}))
	AssertFalse(t, extent.Contains(CellIndex{9, 10}))
	AssertFalse(t, extent.Contains(CellIndex{9, 9}))
	AssertTrue(t, extent.Contains(CellIndex{10, 11}))
	AssertTrue(t, extent.Contains(CellIndex{10, 10}))
	AssertFalse(t, extent.Contains(CellIndex{10, 9}))
	AssertTrue(t, extent.Contains(CellIndex{11, 11}))
	AssertTrue(t, extent.Contains(CellIndex{11, 10}))
	AssertFalse(t, extent.Contains(CellIndex{11, 9}))

	// Lower-right corner
	AssertTrue(t, extent.Contains(CellIndex{19, 11}))
	AssertTrue(t, extent.Contains(CellIndex{19, 10}))
	AssertFalse(t, extent.Contains(CellIndex{19, 9}))
	AssertTrue(t, extent.Contains(CellIndex{20, 11}))
	AssertTrue(t, extent.Contains(CellIndex{20, 10}))
	AssertFalse(t, extent.Contains(CellIndex{20, 9}))
	AssertFalse(t, extent.Contains(CellIndex{21, 11}))
	AssertFalse(t, extent.Contains(CellIndex{21, 10}))
	AssertFalse(t, extent.Contains(CellIndex{21, 9}))

	// Upper-left corner
	AssertFalse(t, extent.Contains(CellIndex{9, 21}))
	AssertFalse(t, extent.Contains(CellIndex{9, 20}))
	AssertFalse(t, extent.Contains(CellIndex{9, 19}))
	AssertFalse(t, extent.Contains(CellIndex{10, 21}))
	AssertTrue(t, extent.Contains(CellIndex{10, 20}))
	AssertTrue(t, extent.Contains(CellIndex{10, 19}))
	AssertFalse(t, extent.Contains(CellIndex{11, 21}))
	AssertTrue(t, extent.Contains(CellIndex{11, 20}))
	AssertTrue(t, extent.Contains(CellIndex{11, 19}))

	// Upper-right corner
	AssertFalse(t, extent.Contains(CellIndex{19, 21}))
	AssertTrue(t, extent.Contains(CellIndex{19, 20}))
	AssertTrue(t, extent.Contains(CellIndex{19, 19}))
	AssertFalse(t, extent.Contains(CellIndex{20, 21}))
	AssertTrue(t, extent.Contains(CellIndex{20, 20}))
	AssertTrue(t, extent.Contains(CellIndex{20, 19}))
	AssertFalse(t, extent.Contains(CellIndex{21, 21}))
	AssertFalse(t, extent.Contains(CellIndex{21, 20}))
	AssertFalse(t, extent.Contains(CellIndex{21, 19}))
}

func TestGridCellExtent_containsLonLat(t *testing.T) {
	extent := CellExtent{
		CellIndex{10, 10},
		CellIndex{20, 20},
	}

	// Lower-left corner
	AssertFalse(t, extent.ContainsLonLat(90, 110, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(90, 100, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(90, 90, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(100, 110, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(100, 100, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(100, 90, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(110, 110, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(110, 100, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(110, 90, 10, 10))

	// Lower-right corner
	AssertTrue(t, extent.ContainsLonLat(190, 110, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(190, 100, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(190, 90, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(200, 110, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(200, 100, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(200, 90, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(210, 110, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(210, 100, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(210, 90, 10, 10))

	// Upper-left corner
	AssertFalse(t, extent.ContainsLonLat(90, 210, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(90, 200, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(90, 190, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(100, 210, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(100, 200, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(100, 190, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(110, 210, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(110, 200, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(110, 190, 10, 10))

	// Upper-right corner
	AssertFalse(t, extent.ContainsLonLat(190, 210, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(190, 200, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(190, 190, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(200, 210, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(200, 200, 10, 10))
	AssertTrue(t, extent.ContainsLonLat(200, 190, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(210, 210, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(210, 200, 10, 10))
	AssertFalse(t, extent.ContainsLonLat(210, 190, 10, 10))
}

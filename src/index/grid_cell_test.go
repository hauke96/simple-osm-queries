package index

import (
	"soq/util"
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
	util.AssertTrue(t, cell.isBelowOrLeftOf(CellIndex{9, 11}))
	util.AssertFalse(t, cell.isBelowOrLeftOf(CellIndex{9, 10}))
	util.AssertFalse(t, cell.isBelowOrLeftOf(CellIndex{9, 9}))

	// Second column
	util.AssertTrue(t, cell.isBelowOrLeftOf(CellIndex{10, 11}))
	util.AssertFalse(t, cell.isBelowOrLeftOf(CellIndex{10, 10}))
	util.AssertFalse(t, cell.isBelowOrLeftOf(CellIndex{10, 9}))

	// Third column
	util.AssertTrue(t, cell.isBelowOrLeftOf(CellIndex{11, 11}))
	util.AssertTrue(t, cell.isBelowOrLeftOf(CellIndex{11, 10}))
	util.AssertTrue(t, cell.isBelowOrLeftOf(CellIndex{11, 9}))
}

func TestGridCellIndex_isAboveOrRightOf(t *testing.T) {
	cell := CellIndex{10, 10}
	/*
		[ 9,11]   [10,11]   [11,11]

		[ 9,10]   [10,10]   [11,10]

		[ 9, 9]   [10, 9]   [11, 9]
	*/

	// First Column
	util.AssertTrue(t, cell.isAboveOrRightOf(CellIndex{9, 11}))
	util.AssertTrue(t, cell.isAboveOrRightOf(CellIndex{9, 10}))
	util.AssertTrue(t, cell.isAboveOrRightOf(CellIndex{9, 9}))

	// Second column
	util.AssertFalse(t, cell.isAboveOrRightOf(CellIndex{10, 11}))
	util.AssertFalse(t, cell.isAboveOrRightOf(CellIndex{10, 10}))
	util.AssertTrue(t, cell.isAboveOrRightOf(CellIndex{10, 9}))

	// Third column
	util.AssertFalse(t, cell.isAboveOrRightOf(CellIndex{11, 11}))
	util.AssertFalse(t, cell.isAboveOrRightOf(CellIndex{11, 10}))
	util.AssertTrue(t, cell.isAboveOrRightOf(CellIndex{11, 9}))
}

func TestGridCellExtent_expand(t *testing.T) {
	extent := CellExtent{CellIndex{10, 10}, CellIndex{20, 20}}

	util.AssertEqual(t, extent, extent.Expand(CellIndex{10, 10}))
	util.AssertEqual(t, extent, extent.Expand(CellIndex{15, 15}))
	util.AssertEqual(t, extent, extent.Expand(CellIndex{20, 20}))

	util.AssertEqual(t, CellExtent{CellIndex{9, 9}, CellIndex{20, 20}}, extent.Expand(CellIndex{9, 9}))
	util.AssertEqual(t, CellExtent{CellIndex{10, 10}, CellIndex{21, 21}}, extent.Expand(CellIndex{21, 21}))
	util.AssertEqual(t, CellExtent{CellIndex{9, 10}, CellIndex{20, 21}}, extent.Expand(CellIndex{9, 21}))
	util.AssertEqual(t, CellExtent{CellIndex{10, 9}, CellIndex{21, 20}}, extent.Expand(CellIndex{21, 9}))
}

func TestGridCellExtent_contains(t *testing.T) {
	extent := CellExtent{
		CellIndex{10, 10},
		CellIndex{20, 20},
	}

	// Lower-left corner
	util.AssertFalse(t, extent.contains(CellIndex{9, 11}))
	util.AssertFalse(t, extent.contains(CellIndex{9, 10}))
	util.AssertFalse(t, extent.contains(CellIndex{9, 9}))
	util.AssertTrue(t, extent.contains(CellIndex{10, 11}))
	util.AssertTrue(t, extent.contains(CellIndex{10, 10}))
	util.AssertFalse(t, extent.contains(CellIndex{10, 9}))
	util.AssertTrue(t, extent.contains(CellIndex{11, 11}))
	util.AssertTrue(t, extent.contains(CellIndex{11, 10}))
	util.AssertFalse(t, extent.contains(CellIndex{11, 9}))

	// Lower-right corner
	util.AssertTrue(t, extent.contains(CellIndex{19, 11}))
	util.AssertTrue(t, extent.contains(CellIndex{19, 10}))
	util.AssertFalse(t, extent.contains(CellIndex{19, 9}))
	util.AssertTrue(t, extent.contains(CellIndex{20, 11}))
	util.AssertTrue(t, extent.contains(CellIndex{20, 10}))
	util.AssertFalse(t, extent.contains(CellIndex{20, 9}))
	util.AssertFalse(t, extent.contains(CellIndex{21, 11}))
	util.AssertFalse(t, extent.contains(CellIndex{21, 10}))
	util.AssertFalse(t, extent.contains(CellIndex{21, 9}))

	// Upper-left corner
	util.AssertFalse(t, extent.contains(CellIndex{9, 21}))
	util.AssertFalse(t, extent.contains(CellIndex{9, 20}))
	util.AssertFalse(t, extent.contains(CellIndex{9, 19}))
	util.AssertFalse(t, extent.contains(CellIndex{10, 21}))
	util.AssertTrue(t, extent.contains(CellIndex{10, 20}))
	util.AssertTrue(t, extent.contains(CellIndex{10, 19}))
	util.AssertFalse(t, extent.contains(CellIndex{11, 21}))
	util.AssertTrue(t, extent.contains(CellIndex{11, 20}))
	util.AssertTrue(t, extent.contains(CellIndex{11, 19}))

	// Upper-right corner
	util.AssertFalse(t, extent.contains(CellIndex{19, 21}))
	util.AssertTrue(t, extent.contains(CellIndex{19, 20}))
	util.AssertTrue(t, extent.contains(CellIndex{19, 19}))
	util.AssertFalse(t, extent.contains(CellIndex{20, 21}))
	util.AssertTrue(t, extent.contains(CellIndex{20, 20}))
	util.AssertTrue(t, extent.contains(CellIndex{20, 19}))
	util.AssertFalse(t, extent.contains(CellIndex{21, 21}))
	util.AssertFalse(t, extent.contains(CellIndex{21, 20}))
	util.AssertFalse(t, extent.contains(CellIndex{21, 19}))
}

func TestGridCellExtent_containsLonLat(t *testing.T) {
	extent := CellExtent{
		CellIndex{10, 10},
		CellIndex{20, 20},
	}

	// Lower-left corner
	util.AssertFalse(t, extent.containsLonLat(90, 110, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(90, 100, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(90, 90, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(100, 110, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(100, 100, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(100, 90, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(110, 110, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(110, 100, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(110, 90, 10, 10))

	// Lower-right corner
	util.AssertTrue(t, extent.containsLonLat(190, 110, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(190, 100, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(190, 90, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(200, 110, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(200, 100, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(200, 90, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(210, 110, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(210, 100, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(210, 90, 10, 10))

	// Upper-left corner
	util.AssertFalse(t, extent.containsLonLat(90, 210, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(90, 200, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(90, 190, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(100, 210, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(100, 200, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(100, 190, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(110, 210, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(110, 200, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(110, 190, 10, 10))

	// Upper-right corner
	util.AssertFalse(t, extent.containsLonLat(190, 210, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(190, 200, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(190, 190, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(200, 210, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(200, 200, 10, 10))
	util.AssertTrue(t, extent.containsLonLat(200, 190, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(210, 210, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(210, 200, 10, 10))
	util.AssertFalse(t, extent.containsLonLat(210, 190, 10, 10))
}

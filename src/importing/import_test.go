package importing

import (
	"soq/index"
	"soq/util"
	"testing"
)

func TestImport_getNextExtent(t *testing.T) {
	c00 := index.CellIndex{0, 0}
	c10 := index.CellIndex{1, 0}
	c20 := index.CellIndex{2, 0}
	c01 := index.CellIndex{0, 1}
	c11 := index.CellIndex{1, 1}
	c21 := index.CellIndex{2, 1}
	c02 := index.CellIndex{0, 2}
	c12 := index.CellIndex{1, 2}
	c22 := index.CellIndex{2, 2}

	cellsToProcessedState := map[index.CellIndex]bool{}

	cellsToProcessedState[c00] = false
	cellsToProcessedState[c10] = false
	cellsToProcessedState[c20] = false

	cellsToProcessedState[c01] = false
	cellsToProcessedState[c11] = false
	cellsToProcessedState[c21] = false

	cellsToProcessedState[c02] = false
	cellsToProcessedState[c12] = false
	cellsToProcessedState[c22] = false

	cellToNodeCount := map[index.CellIndex]int{}

	/*
		10	3	0
		0	0	20
		0	0	0
	*/

	cellToNodeCount[c00] = 0
	cellToNodeCount[c10] = 0
	cellToNodeCount[c20] = 0

	cellToNodeCount[c01] = 0
	cellToNodeCount[c11] = 0
	cellToNodeCount[c21] = 20

	cellToNodeCount[c02] = 10
	cellToNodeCount[c12] = 3
	cellToNodeCount[c22] = 0

	extent := getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
	util.AssertEqual(t, &index.CellExtent{index.CellIndex{0, 0}, index.CellIndex{1, 1}}, extent)

	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
	util.AssertEqual(t, &index.CellExtent{index.CellIndex{2, 0}, index.CellIndex{2, 0}}, extent)

	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
	util.AssertEqual(t, &index.CellExtent{index.CellIndex{2, 1}, index.CellIndex{2, 1}}, extent)

	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
	util.AssertEqual(t, &index.CellExtent{index.CellIndex{0, 2}, index.CellIndex{0, 2}}, extent)

	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
	util.AssertEqual(t, &index.CellExtent{index.CellIndex{1, 2}, index.CellIndex{2, 2}}, extent)

	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
	util.AssertNil(t, extent)
}

func TestImport_getNextExtent_rightMostExtent(t *testing.T) {
	c00 := index.CellIndex{0, 0}
	c10 := index.CellIndex{1, 0}
	c20 := index.CellIndex{2, 0}
	c01 := index.CellIndex{0, 1}
	c11 := index.CellIndex{1, 1}
	c21 := index.CellIndex{2, 1}
	c02 := index.CellIndex{0, 2}
	c12 := index.CellIndex{1, 2}
	c22 := index.CellIndex{2, 2}

	cellsToProcessedState := map[index.CellIndex]bool{}

	cellsToProcessedState[c00] = false
	cellsToProcessedState[c10] = false
	cellsToProcessedState[c20] = false

	cellsToProcessedState[c01] = false
	cellsToProcessedState[c11] = false
	cellsToProcessedState[c21] = false

	cellsToProcessedState[c02] = false
	cellsToProcessedState[c12] = false
	cellsToProcessedState[c22] = false

	cellToNodeCount := map[index.CellIndex]int{}

	/*
		1	2	0
		0	3	0
		2	2	2
	*/

	cellToNodeCount[c00] = 2
	cellToNodeCount[c10] = 2
	cellToNodeCount[c20] = 2

	cellToNodeCount[c01] = 0
	cellToNodeCount[c11] = 3
	cellToNodeCount[c21] = 0

	cellToNodeCount[c02] = 1
	cellToNodeCount[c12] = 2
	cellToNodeCount[c22] = 0

	extent := getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
	util.AssertEqual(t, &index.CellExtent{index.CellIndex{0, 0}, index.CellIndex{1, 0}}, extent)

	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
	util.AssertEqual(t, &index.CellExtent{index.CellIndex{2, 0}, index.CellIndex{2, 2}}, extent)

	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
	util.AssertEqual(t, &index.CellExtent{index.CellIndex{0, 1}, index.CellIndex{1, 1}}, extent)

	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
	util.AssertEqual(t, &index.CellExtent{index.CellIndex{0, 2}, index.CellIndex{1, 2}}, extent)

	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
	util.AssertNil(t, extent)
}

package importing

import (
	"soq/common"
	"testing"

	"github.com/hauke96/sigolo/v2"
)

//func TestImport_getNextExtent(t *testing.T) {
//	c00 := index.CellIndex{0, 0}
//	c10 := index.CellIndex{1, 0}
//	c20 := index.CellIndex{2, 0}
//	c01 := index.CellIndex{0, 1}
//	c11 := index.CellIndex{1, 1}
//	c21 := index.CellIndex{2, 1}
//	c02 := index.CellIndex{0, 2}
//	c12 := index.CellIndex{1, 2}
//	c22 := index.CellIndex{2, 2}
//
//	cellsToProcessedState := map[index.CellIndex]bool{}
//
//	cellsToProcessedState[c00] = false
//	cellsToProcessedState[c10] = false
//	cellsToProcessedState[c20] = false
//
//	cellsToProcessedState[c01] = false
//	cellsToProcessedState[c11] = false
//	cellsToProcessedState[c21] = false
//
//	cellsToProcessedState[c02] = false
//	cellsToProcessedState[c12] = false
//	cellsToProcessedState[c22] = false
//
//	cellToNodeCount := map[index.CellIndex]int{}
//
//	/*
//		10	3	0
//		0	0	20
//		0	0	0
//	*/
//
//	cellToNodeCount[c00] = 0
//	cellToNodeCount[c10] = 0
//	cellToNodeCount[c20] = 0
//
//	cellToNodeCount[c01] = 0
//	cellToNodeCount[c11] = 0
//	cellToNodeCount[c21] = 20
//
//	cellToNodeCount[c02] = 10
//	cellToNodeCount[c12] = 3
//	cellToNodeCount[c22] = 0
//
//	extent := getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
//	common.AssertEqual(t, &index.CellExtent{index.CellIndex{0, 0}, index.CellIndex{1, 1}}, extent)
//
//	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
//	common.AssertEqual(t, &index.CellExtent{index.CellIndex{2, 0}, index.CellIndex{2, 0}}, extent)
//
//	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
//	common.AssertEqual(t, &index.CellExtent{index.CellIndex{2, 1}, index.CellIndex{2, 1}}, extent)
//
//	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
//	common.AssertEqual(t, &index.CellExtent{index.CellIndex{0, 2}, index.CellIndex{0, 2}}, extent)
//
//	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
//	common.AssertEqual(t, &index.CellExtent{index.CellIndex{1, 2}, index.CellIndex{2, 2}}, extent)
//
//	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
//	common.AssertNil(t, extent)
//}
//
//func TestImport_getNextExtent_rightMostExtent(t *testing.T) {
//	c00 := index.CellIndex{0, 0}
//	c10 := index.CellIndex{1, 0}
//	c20 := index.CellIndex{2, 0}
//	c01 := index.CellIndex{0, 1}
//	c11 := index.CellIndex{1, 1}
//	c21 := index.CellIndex{2, 1}
//	c02 := index.CellIndex{0, 2}
//	c12 := index.CellIndex{1, 2}
//	c22 := index.CellIndex{2, 2}
//
//	cellsToProcessedState := map[index.CellIndex]bool{}
//
//	cellsToProcessedState[c00] = false
//	cellsToProcessedState[c10] = false
//	cellsToProcessedState[c20] = false
//
//	cellsToProcessedState[c01] = false
//	cellsToProcessedState[c11] = false
//	cellsToProcessedState[c21] = false
//
//	cellsToProcessedState[c02] = false
//	cellsToProcessedState[c12] = false
//	cellsToProcessedState[c22] = false
//
//	cellToNodeCount := map[index.CellIndex]int{}
//
//	/*
//		1	2	0
//		0	3	0
//		2	2	2
//	*/
//
//	cellToNodeCount[c00] = 2
//	cellToNodeCount[c10] = 2
//	cellToNodeCount[c20] = 2
//
//	cellToNodeCount[c01] = 0
//	cellToNodeCount[c11] = 3
//	cellToNodeCount[c21] = 0
//
//	cellToNodeCount[c02] = 1
//	cellToNodeCount[c12] = 2
//	cellToNodeCount[c22] = 0
//
//	extent := getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
//	common.AssertEqual(t, &index.CellExtent{index.CellIndex{0, 0}, index.CellIndex{1, 0}}, extent)
//
//	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
//	common.AssertEqual(t, &index.CellExtent{index.CellIndex{2, 0}, index.CellIndex{2, 2}}, extent)
//
//	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
//	common.AssertEqual(t, &index.CellExtent{index.CellIndex{0, 1}, index.CellIndex{1, 1}}, extent)
//
//	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
//	common.AssertEqual(t, &index.CellExtent{index.CellIndex{0, 2}, index.CellIndex{1, 2}}, extent)
//
//	extent = getNextExtent(cellsToProcessedState, cellToNodeCount, 5)
//	common.AssertNil(t, extent)
//}

func TestImport_getExtents(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)

	cellToNodeCountValues := [][]int{
		//0 1  2  3  4  5  6  7  8  9
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},  // 9
		{0, 0, 0, 0, 0, 0, 0, 0, 12, 0}, // 8
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},  // 7
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},  // 6
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},  // 5
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},  // 4
		{0, 0, 1, 9, 0, 1, 0, 0, 0, 0},  // 3
		{0, 0, 0, 0, 0, 9, 0, 0, 0, 0},  // 2
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},  // 1
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},  // 0
	}

	cellToNodeCount := map[common.CellIndex]int{}
	cellsToProcessedState := map[common.CellIndex]bool{}

	for y, row := range cellToNodeCountValues {
		y = len(cellToNodeCountValues) - 1 - y // The y-axis goes from bottom to top (s. comments above)
		for x, value := range row {
			cellIndex := common.CellIndex{x, y}
			cellToNodeCount[cellIndex] = value
			cellsToProcessedState[cellIndex] = false
		}
	}

	nodePerExtentThreshold := 10

	// Act & Assert
	expectedExtents := []common.CellExtent{
		{common.CellIndex{0, 0}, common.CellIndex{4, 9}},
		{common.CellIndex{5, 0}, common.CellIndex{9, 7}},
		{common.CellIndex{5, 8}, common.CellIndex{7, 9}},
		{common.CellIndex{8, 8}, common.CellIndex{9, 9}},
	}

	actualExtents := getExtents(cellToNodeCount, nodePerExtentThreshold)

	common.AssertContainsExactlyInAnyOrder(t, expectedExtents, actualExtents)
}

func TestImport_getExtents_preventNonSquareishExtents(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)

	cellToNodeCountValues := [][]int{
		//0 1  2  3  4  5  6  7  8  9
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 9
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 8
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 7
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 6
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 5
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 4
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 3
		{8, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 2
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 1
		{7, 0, 0, 0, 0, 0, 2, 0, 9, 0}, // 0
	}

	cellToNodeCount := map[common.CellIndex]int{}
	cellsToProcessedState := map[common.CellIndex]bool{}

	for y, row := range cellToNodeCountValues {
		y = len(cellToNodeCountValues) - 1 - y // The y-axis goes from bottom to top (s. comments above)
		for x, value := range row {
			cellIndex := common.CellIndex{x, y}
			cellToNodeCount[cellIndex] = value
			cellsToProcessedState[cellIndex] = false
		}
	}

	nodePerExtentThreshold := 10

	// Act & Assert
	expectedExtents := []common.CellExtent{
		{common.CellIndex{0, 0}, common.CellIndex{3, 1}},
		{common.CellIndex{4, 0}, common.CellIndex{7, 5}},
		{common.CellIndex{0, 2}, common.CellIndex{3, 9}},
		{common.CellIndex{8, 0}, common.CellIndex{9, 3}},
		{common.CellIndex{8, 0}, common.CellIndex{9, 3}},
		{common.CellIndex{8, 0}, common.CellIndex{9, 3}},
	}

	actualExtents := getExtents(cellToNodeCount, nodePerExtentThreshold)

	common.AssertContainsExactlyInAnyOrder(t, expectedExtents, actualExtents)
}

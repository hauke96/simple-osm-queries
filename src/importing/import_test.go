package importing

import (
	"soq/common"
	"testing"

	"github.com/hauke96/sigolo/v2"
)

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
		{common.CellIndex{8, 8}, common.CellIndex{8, 8}},
		{common.CellIndex{9, 8}, common.CellIndex{9, 9}},
		{common.CellIndex{8, 9}, common.CellIndex{8, 9}},
	}

	actualExtents := getExtents(cellToNodeCount, nodePerExtentThreshold, 0.0001, 0.0001)

	common.AssertContainsExactlyInAnyOrder(t, expectedExtents, actualExtents)
}

func TestImport_getExtents_lessNodesThanThreshold(t *testing.T) {
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

	nodePerExtentThreshold := 1000

	// Act & Assert
	expectedExtents := []common.CellExtent{
		{common.CellIndex{0, 0}, common.CellIndex{9, 9}},
	}

	actualExtents := getExtents(cellToNodeCount, nodePerExtentThreshold, 0.0001, 0.0001)

	common.AssertContainsExactlyInAnyOrder(t, expectedExtents, actualExtents)
}

func TestImport_getExtents_missingCells(t *testing.T) {
	// Arrange
	sigolo.SetDefaultLogLevel(sigolo.LOG_TRACE)

	// 0 values will be missing cells below
	cellToNodeCountValues := [][]int{
		//0 1  2  3  4  5  6  7  8  9
		{0, 0, 0, 0, 0, 0, 1, 1, 1, 1}, // 9
		{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}, // 8
		{0, 0, 0, 0, 1, 1, 1, 1, 1, 0}, // 7
		{0, 0, 0, 0, 1, 1, 1, 1, 0, 0}, // 6
		{0, 0, 0, 0, 1, 1, 1, 1, 0, 0}, // 5
		{0, 0, 0, 1, 1, 1, 1, 1, 0, 0}, // 4
		{1, 1, 1, 9, 1, 1, 0, 0, 0, 0}, // 3
		{1, 1, 1, 1, 1, 1, 0, 0, 0, 0}, // 2
		{1, 1, 1, 1, 1, 1, 0, 0, 0, 0}, // 1
		{1, 1, 1, 1, 1, 1, 0, 0, 0, 0}, // 0
	}

	cellToNodeCount := map[common.CellIndex]int{}
	cellsToProcessedState := map[common.CellIndex]bool{}

	for y, row := range cellToNodeCountValues {
		y = len(cellToNodeCountValues) - 1 - y // The y-axis goes from bottom to top (s. comments above)
		for x, value := range row {
			if value == 0 {
				continue
			}
			cellIndex := common.CellIndex{x, y}
			cellToNodeCount[cellIndex] = value
			cellsToProcessedState[cellIndex] = false
		}
	}

	nodePerExtentThreshold := 24

	// Act & Assert
	expectedExtents := []common.CellExtent{
		{common.CellIndex{0, 0}, common.CellIndex{3, 3}},
		{common.CellIndex{4, 0}, common.CellIndex{9, 6}},
		{common.CellIndex{4, 7}, common.CellIndex{9, 9}},
		{common.CellIndex{3, 4}, common.CellIndex{3, 5}}, // 3,5 is empty but we expand anyway
	}

	actualExtents := getExtents(cellToNodeCount, nodePerExtentThreshold, 0.0001, 0.0001)

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
		{common.CellIndex{0, 2}, common.CellIndex{9, 9}},
		{common.CellIndex{4, 0}, common.CellIndex{7, 1}},
		{common.CellIndex{8, 0}, common.CellIndex{9, 1}},
	}

	actualExtents := getExtents(cellToNodeCount, nodePerExtentThreshold, 0.0001, 0.0001)

	common.AssertContainsExactlyInAnyOrder(t, expectedExtents, actualExtents)
}

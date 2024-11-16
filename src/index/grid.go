package index

import "soq/common"

const GridIndexFolder = "grid-index"

type BaseGridIndex struct {
	TagIndex   *TagIndex
	CellWidth  float64
	CellHeight float64
	BaseFolder string
}

// GetCellIndexForCoordinate returns the cell index (i.e. position) for the given coordinate.
func (g *BaseGridIndex) GetCellIndexForCoordinate(x float64, y float64) common.CellIndex {
	return common.GetCellIndexForCoordinate(x, y, g.CellWidth, g.CellHeight)
}

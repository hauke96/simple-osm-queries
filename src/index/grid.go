package index

const GridIndexFolder = "grid-index"

type baseGridIndex struct {
	TagIndex   *TagIndex
	CellWidth  float64
	CellHeight float64
	BaseFolder string
}

// GetCellIndexForCoordinate returns the cell index (i.e. position) for the given coordinate.
func (g *baseGridIndex) GetCellIndexForCoordinate(x float64, y float64) CellIndex {
	return CellIndex{int(x / g.CellWidth), int(y / g.CellHeight)}
}

package common

import "github.com/paulmach/orb"

type CellIndex [2]int

func GetCellIndexForCoordinate(x float64, y float64, cellWidth float64, cellHeight float64) CellIndex {
	return CellIndex{int(x / cellWidth), int(y / cellHeight)}
}

func (c CellIndex) X() int { return c[0] }

func (c CellIndex) Y() int { return c[1] }

func (c CellIndex) isBelowOrLeftOf(other CellIndex) bool {
	return c.X() < other.X() || c.Y() < other.Y()
}

func (c CellIndex) isAboveOrRightOf(other CellIndex) bool {
	return c.X() > other.X() || c.Y() > other.Y()
}

func (c CellIndex) ToPoint(cellWidth float64, cellHeight float64) orb.Point {
	return orb.Point{float64(c[0]) * cellWidth, float64(c[1]) * cellHeight}
}

type CellExtent [2]CellIndex

func (c CellExtent) LowerLeftCell() CellIndex { return c[0] }

func (c CellExtent) UpperRightCell() CellIndex { return c[1] }

func (c CellExtent) Expand(cell CellIndex) CellExtent {
	if c.Contains(cell) {
		return c
	}

	minX := c.LowerLeftCell().X()
	minY := c.LowerLeftCell().Y()

	maxX := c.UpperRightCell().X()
	maxY := c.UpperRightCell().Y()

	if cell.X() < minX {
		minX = cell.X()
	}
	if cell.Y() < minY {
		minY = cell.Y()
	}

	if cell.X() > maxX {
		maxX = cell.X()
	}
	if cell.Y() > maxY {
		maxY = cell.Y()
	}

	return CellExtent{
		CellIndex{minX, minY},
		CellIndex{maxX, maxY},
	}
}

func (c CellExtent) Contains(cell CellIndex) bool {
	return !cell.isAboveOrRightOf(c.UpperRightCell()) && !cell.isBelowOrLeftOf(c.LowerLeftCell())
}

func (c CellExtent) ContainsLonLat(lon float64, lat float64, cellWidth float64, cellHeight float64) bool {
	x := int(lon / cellWidth)
	y := int(lat / cellHeight)

	return x >= c.LowerLeftCell().X() && y >= c.LowerLeftCell().Y() && x <= c.UpperRightCell().X() && y <= c.UpperRightCell().Y()
}

func (c CellExtent) ContainsAny(cells []CellIndex) bool {
	for _, cell := range cells {
		if c.Contains(cell) {
			return true
		}
	}
	return false
}

func (c CellExtent) ContainsAnyInMap(cells map[CellIndex]CellIndex) bool {
	for _, cell := range cells {
		if c.Contains(cell) {
			return true
		}
	}
	return false
}

func (c CellExtent) Subdivide(cellsX int, cellsY int) []CellExtent {
	var newExtents []CellExtent
	for x := c.LowerLeftCell().X(); x < c.UpperRightCell().X(); x += cellsX {
		for y := c.LowerLeftCell().Y(); y < c.UpperRightCell().Y(); y += cellsY {
			newExtents = append(newExtents, CellExtent{CellIndex{x, y}, CellIndex{x + cellsX - 1, y + cellsY - 1}})
		}
	}
	return newExtents
}

func (c CellExtent) GetCellIndices() []CellIndex {
	var indices []CellIndex

	for x := c.LowerLeftCell().X(); x <= c.UpperRightCell().X(); x++ {
		for y := c.LowerLeftCell().Y(); y <= c.UpperRightCell().Y(); y++ {
			indices = append(indices, CellIndex{x, y})
		}
	}

	return indices
}

func (c CellExtent) ToPolygon(cellWidth float64, cellHeight float64) orb.Polygon {
	lowerLeft := c[0].ToPoint(cellWidth, cellHeight)
	maxCell := CellIndex{c[1].X() + 1, c[1].Y() + 1}
	upperRight := maxCell.ToPoint(cellWidth, cellHeight)
	return orb.Polygon{
		orb.Ring{
			lowerLeft,
			orb.Point{upperRight.X(), lowerLeft.Y()},
			upperRight,
			orb.Point{lowerLeft.X(), upperRight.Y()},
			lowerLeft,
		},
	}
}

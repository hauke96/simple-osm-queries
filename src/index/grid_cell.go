package index

type CellIndex [2]int

func (c CellIndex) X() int { return c[0] }

func (c CellIndex) Y() int { return c[1] }

func (c CellIndex) isBelowLeftOf(other CellIndex) bool {
	return c.X() <= other.X() && c.Y() <= other.Y()
}

func (c CellIndex) isAboveRightOf(other CellIndex) bool {
	return c.X() >= other.X() && c.Y() >= other.Y()
}

type CellExtent [2]CellIndex

func (c CellExtent) LowerLeftCell() CellIndex { return c[0] }

func (c CellExtent) UpperRightCell() CellIndex { return c[1] }

func (c CellExtent) contains(cell CellIndex) bool {
	return cell.isAboveRightOf(c.LowerLeftCell()) && cell.isBelowLeftOf(c.UpperRightCell())
}

func (c CellExtent) containsAny(cells []CellIndex) bool {
	for _, cell := range cells {
		if c.contains(cell) {
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

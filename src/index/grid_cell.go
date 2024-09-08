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

func (c CellIndex) isWithin(lowerLeft CellIndex, upperRight CellIndex) bool {
	return c.isAboveRightOf(lowerLeft) && c.isBelowLeftOf(upperRight)
}

func isAnyWithin(cells []CellIndex, lowerLeft CellIndex, upperRight CellIndex) bool {
	for _, cell := range cells {
		if cell.isWithin(lowerLeft, upperRight) {
			return true
		}
	}
	return false
}

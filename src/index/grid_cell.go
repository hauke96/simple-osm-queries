package index

type CellIndex [2]int

func (c CellIndex) X() int { return c[0] }

func (c CellIndex) Y() int { return c[1] }

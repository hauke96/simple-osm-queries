package query

import "fmt"

type BinaryOperator int

const (
	BinOpInvalid BinaryOperator = iota
	BinOpEqual
	BinOpNotEqual
	BinOpGreater
	BinOpGreaterEqual
	BinOpLower
	BinOpLowerEqual
)

func (o BinaryOperator) string() string {
	switch o {
	case BinOpEqual:
		return "="
	case BinOpNotEqual:
		return "!="
	case BinOpGreater:
		return ">"
	case BinOpGreaterEqual:
		return ">="
	case BinOpLower:
		return "<"
	case BinOpLowerEqual:
		return "<="
	}
	return fmt.Sprintf("[!UNKNOWN BinaryOperator %d]", o)
}

// IsComparisonOperator returns true for operators >, >=, < and <=. The = and != operators are considered "equality" but
// not comparison operators.
func (o BinaryOperator) IsComparisonOperator() bool {
	return o == BinOpGreater || o == BinOpGreaterEqual || o == BinOpLower || o == BinOpLowerEqual
}

type LogicalOperator int

const (
	LogicOpAnd LogicalOperator = iota
	LogicOpOr
	LogicOpNot
)

func (o LogicalOperator) string() string {
	switch o {
	case LogicOpAnd:
		return "AND"
	case LogicOpOr:
		return "OR"
	case LogicOpNot:
		return "NOT"
	}
	return fmt.Sprintf("[!UNKNOWN LogicalOperator %d]", o)
}

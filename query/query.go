package query

type ObjectType int

const (
	Node ObjectType = iota
	Way
	Relation
)

type BinaryOperator int

const (
	Equal BinaryOperator = iota
	NotEqual
	Greater
	GreaterEqual
	Lower
	LowerEqual
)

type LogicalOperator int

const (
	And LogicalOperator = iota
	Or
	Not
)

type Query struct {
	topLevelStatements []Statement
}

/*
	Location expressions
*/

type LocationExpression interface {
	// TODO Function parameter
	IsWithin() bool
}

type BboxLocationExpression struct {
	coordinates [4]int
}

func (b *BboxLocationExpression) IsWithin() bool {
	// TODO Implement
	return true
}

/*
	Filter expressions
*/

type FilterExpression interface {
	// TODO Function parameter
	Applies() bool
}

type Statement struct {
	location   LocationExpression
	objectType ObjectType
	filter     FilterExpression
}

func (f Statement) Applies() bool {
	// TODO Implement
	return true
}

type NegatedStatement struct {
	baseExpression FilterExpression
	operator       LogicalOperator
}

func (f NegatedStatement) Applies() bool {
	// TODO Implement
	return true
}

type LogicalFilterExpression struct {
	statementA Statement
	statementB Statement
	operator   LogicalOperator
}

func (f LogicalFilterExpression) Applies() bool {
	// TODO Implement
	return true
}

type TagFilterExpression struct {
	key      int
	value    int
	operator BinaryOperator
}

func (f TagFilterExpression) Applies() bool {
	// TODO Implement
	return true
}

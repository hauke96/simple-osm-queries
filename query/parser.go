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
	Smaller
	SmallerEqual
)

type LogicalOperator int

const (
	And LogicalOperator = iota
	Or
)

type Query struct {
	topLevelStatements []Statement
}

type Location interface {
	// TODO Implement + function parameter
	IsWithin() bool
}

type FilterExpression interface {
	// TODO Function parameter
	Applies() bool
}

type Statement struct {
	location   Location
	objectType ObjectType
	filter     []FilterExpression
}

func (f Statement) Applies() bool {
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

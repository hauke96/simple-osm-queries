package query

import (
	"fmt"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"soq/index"
	"strings"
)

type ObjectType int

const (
	Node ObjectType = iota
	Way
	Relation
)

func (o ObjectType) string() string {
	switch o {
	case Node:
		return "Node"
	case Way:
		return "Way"
	case Relation:
		return "Relation"
	}
	return fmt.Sprintf("[!UNKNOWN ObjectType %d]", o)
}

type BinaryOperator int

const (
	Equal BinaryOperator = iota
	NotEqual
	Greater
	GreaterEqual
	Lower
	LowerEqual
)

func (o BinaryOperator) string() string {
	switch o {
	case Equal:
		return "="
	case NotEqual:
		return "!="
	case Greater:
		return ">"
	case GreaterEqual:
		return ">="
	case Lower:
		return "<"
	case LowerEqual:
		return "<="
	}
	return fmt.Sprintf("[!UNKNOWN BinaryOperator %d]", o)
}

type LogicalOperator int

const (
	And LogicalOperator = iota
	Or
	Not
)

func (o LogicalOperator) string() string {
	switch o {
	case And:
		return "AND"
	case Or:
		return "OR"
	case Not:
		return "NOT"
	}
	return fmt.Sprintf("[!UNKNOWN LogicalOperator %d]", o)
}

type Query struct {
	topLevelStatements []Statement
}

/*
	Location expressions
*/

type LocationExpression interface {
	IsWithin(feature *index.EncodedFeature) bool
	Print(indent int)
}

type BboxLocationExpression struct {
	bbox *orb.Bound
}

func (b *BboxLocationExpression) IsWithin(feature *index.EncodedFeature) bool {
	switch geometry := feature.Geometry.(type) {
	case orb.Point:
		return b.bbox.Contains(geometry)
	}
	return false
}

func (b *BboxLocationExpression) Print(indent int) {
	sigolo.Debugf("%s%s(%d, %d, %d, %d)", spacing(indent), "bbox", b.coordinates[0], b.coordinates[1], b.coordinates[2], b.coordinates[3])
}

/*
	Filter expressions
*/

type FilterExpression interface {
	// TODO Function parameter
	Applies() bool
	Print(indent int)
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

func (f Statement) Print(indent int) {
	sigolo.Debugf("%s%s", spacing(indent), "Statement")
	f.location.Print(indent + 2)
	sigolo.Debugf("%s%s", spacing(indent+2), f.objectType.string())
	f.filter.Print(indent + 2)
}

type NegatedStatement struct {
	baseExpression FilterExpression
	operator       LogicalOperator
}

func (f NegatedStatement) Applies() bool {
	// TODO Implement
	return true
}

func (f NegatedStatement) Print(indent int) {
	sigolo.Debugf("%s%s", spacing(indent), f.operator.string())
	f.baseExpression.Print(indent + 2)
}

type LogicalFilterExpression struct {
	statementA FilterExpression
	statementB FilterExpression
	operator   LogicalOperator
}

func (f LogicalFilterExpression) Applies() bool {
	// TODO Implement
	return true
}

func (f LogicalFilterExpression) Print(indent int) {
	sigolo.Debugf("%s%s", spacing(indent), f.operator.string())
	f.statementA.Print(indent + 2)
	f.statementB.Print(indent + 2)
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

func (f TagFilterExpression) Print(indent int) {
	sigolo.Debugf("%s%s: %d%s%d", spacing(indent), "TagFilterExpression", f.key, f.operator.string(), f.value)
}

func spacing(indent int) string {
	return strings.Repeat(" ", indent)
}

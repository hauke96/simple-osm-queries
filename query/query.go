package query

import (
	"fmt"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/pkg/errors"
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
	geometryIndex      index.GeometryIndex
}

func (q *Query) Execute() []index.EncodedFeature {
	var result []index.EncodedFeature

	for _, statement := range q.topLevelStatements {
		features := statement.GetFeatures(q.geometryIndex)

		for _, feature := range features {
			if statement.Applies(&feature) {
				result = append(features, feature)
			}
		}
	}

	return result
}

/*
	Statement
*/

type Statement struct {
	location   LocationExpression
	objectType ObjectType
	filter     FilterExpression
}

func (f Statement) GetFeatures(geometryIndex index.GeometryIndex) []index.EncodedFeature {
	return f.location.GetFeatures(geometryIndex)
}

func (f Statement) Applies(feature *index.EncodedFeature) bool {
	// TODO Respect object type
	return f.location.IsWithin(feature) && f.filter.Applies(feature)
}

func (f Statement) Print(indent int) {
	sigolo.Debugf("%s%s", spacing(indent), "Statement")
	f.location.Print(indent + 2)
	sigolo.Debugf("%s%s", spacing(indent+2), f.objectType.string())
	f.filter.Print(indent + 2)
}

/*
	Location expressions
*/

type LocationExpression interface {
	GetFeatures(geometryIndex index.GeometryIndex) []index.EncodedFeature
	IsWithin(feature *index.EncodedFeature) bool
	Print(indent int)
}

type BboxLocationExpression struct {
	bbox *orb.Bound
}

func (b *BboxLocationExpression) GetFeatures(geometryIndex index.GeometryIndex) []index.EncodedFeature {
	return geometryIndex.Get(b.bbox)
}

func (b *BboxLocationExpression) IsWithin(feature *index.EncodedFeature) bool {
	switch geometry := feature.Geometry.(type) {
	case orb.Point:
		return b.bbox.Contains(geometry)
	}
	return false
}

func (b *BboxLocationExpression) Print(indent int) {
	sigolo.Debugf("%s%s(%d, %d, %d, %d)", spacing(indent), "bbox", b.bbox.Min.Lon(), b.bbox.Min.Lat(), b.bbox.Max.Lon(), b.bbox.Max.Lat())
}

/*
	Filter expressions
*/

type FilterExpression interface {
	Applies(feature *index.EncodedFeature) bool
	Print(indent int)
}

type NegatedFilterExpression struct {
	baseExpression FilterExpression
	operator       LogicalOperator
}

func (f NegatedFilterExpression) Applies(feature *index.EncodedFeature) bool {
	return !f.baseExpression.Applies(feature)
}

func (f NegatedFilterExpression) Print(indent int) {
	sigolo.Debugf("%s%s", spacing(indent), f.operator.string())
	f.baseExpression.Print(indent + 2)
}

type LogicalFilterExpression struct {
	statementA FilterExpression
	statementB FilterExpression
	operator   LogicalOperator
}

func (f LogicalFilterExpression) Applies(feature *index.EncodedFeature) bool {
	switch f.operator {
	case Or:
		return f.statementA.Applies(feature) || f.statementB.Applies(feature)
	case And:
		return f.statementA.Applies(feature) && f.statementB.Applies(feature)
	}
	panic(errors.Errorf("Unknown or unsupported logical operator %d", f.operator))
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

func (f TagFilterExpression) Applies(feature *index.EncodedFeature) bool {
	return feature.HasTag(f.key, f.value)
}

func (f TagFilterExpression) Print(indent int) {
	sigolo.Debugf("%s%s: %d%s%d", spacing(indent), "TagFilterExpression", f.key, f.operator.string(), f.value)
}

func spacing(indent int) string {
	return strings.Repeat(" ", indent)
}

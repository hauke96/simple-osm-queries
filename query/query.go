package query

import (
	"fmt"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/pkg/errors"
	"soq/index"
	"strings"
	"time"
)

type OsmObjectType int

const (
	OsmObjNode OsmObjectType = iota
	OsmObjWay
	OsmObjRelation
)

func (o OsmObjectType) string() string {
	switch o {
	case OsmObjNode:
		return "node"
	case OsmObjWay:
		return "way"
	case OsmObjRelation:
		return "relation"
	}
	return fmt.Sprintf("[!UNKNOWN OsmObjectType %d]", o)
}

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

// isComparisonOperator returns true for operators >, >=, < and <=. The = and != operators are considered "equality" but
// not comparison operators.
func (o BinaryOperator) isComparisonOperator() bool {
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

type Query struct {
	topLevelStatements []Statement
	geometryIndex      index.GeometryIndex
}

func (q *Query) Execute() ([]*index.EncodedFeature, error) {
	sigolo.Info("Start query")
	queryStartTime := time.Now()

	var result []*index.EncodedFeature

	for _, statement := range q.topLevelStatements {
		statement.Print(0)

		featuresChannel, err := statement.GetFeatures(q.geometryIndex)
		if err != nil {
			return nil, err
		}

		for features := range featuresChannel {
			sigolo.Tracef("Received %d features", len(features))

			for _, feature := range features {
				sigolo.Trace("----- next feature -----")
				if feature != nil {
					feature.Print()
					if statement.Applies(feature) {
						result = append(result, feature)
					}
				}
			}
		}
	}

	queryDuration := time.Since(queryStartTime)
	sigolo.Infof("Executed query in %s", queryDuration)

	return result, nil
}

/*
	Statement
*/

type Statement struct {
	location   LocationExpression
	objectType OsmObjectType
	filter     FilterExpression
}

func (f Statement) GetFeatures(geometryIndex index.GeometryIndex) (chan []*index.EncodedFeature, error) {
	return f.location.GetFeatures(geometryIndex, f.objectType)
}

func (f Statement) Applies(feature *index.EncodedFeature) bool {
	// TODO Respect object type
	return f.location.IsWithin(feature) && f.filter.Applies(feature)
}

func (f Statement) Print(indent int) {
	sigolo.Debugf("%s%s", spacing(indent), "Statement")
	f.location.Print(indent + 2)
	sigolo.Debugf("%stype: %s", spacing(indent+2), f.objectType.string())
	f.filter.Print(indent + 2)
}

/*
	Location expressions
*/

type LocationExpression interface {
	GetFeatures(geometryIndex index.GeometryIndex, objectType OsmObjectType) (chan []*index.EncodedFeature, error)
	IsWithin(feature *index.EncodedFeature) bool
	Print(indent int)
}

type BboxLocationExpression struct {
	bbox *orb.Bound
}

func (b *BboxLocationExpression) GetFeatures(geometryIndex index.GeometryIndex, objectType OsmObjectType) (chan []*index.EncodedFeature, error) {
	// TODO Find a better solution than ".string()" for object types
	return geometryIndex.Get(b.bbox, objectType.string())
}

func (b *BboxLocationExpression) IsWithin(feature *index.EncodedFeature) bool {
	if sigolo.ShouldLogTrace() {
		sigolo.Tracef("BboxLocationExpression: IsWithin((%s), %v)", b.string(), feature.Geometry)
	}

	switch geometry := feature.Geometry.(type) {
	case *orb.Point:
		return b.bbox.Contains(*geometry)
	}

	panic(errors.Errorf("Unknown or unsupported geometry type %s", feature.Geometry.GeoJSONType()))
}

func (b *BboxLocationExpression) Print(indent int) {
	sigolo.Debugf("%slocation: %s(%s)", spacing(indent), "bbox", b.string())
}

func (b *BboxLocationExpression) string() string {
	return fmt.Sprintf("%f, %f, %f, %f", b.bbox.Min.Lon(), b.bbox.Min.Lat(), b.bbox.Max.Lon(), b.bbox.Max.Lat())
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
}

func (f NegatedFilterExpression) Applies(feature *index.EncodedFeature) bool {
	sigolo.Tracef("NegatedFilterExpression")
	return !f.baseExpression.Applies(feature)
}

func (f NegatedFilterExpression) Print(indent int) {
	sigolo.Debugf("%s%s", spacing(indent), LogicOpNot.string())
	f.baseExpression.Print(indent + 2)
}

type LogicalFilterExpression struct {
	statementA FilterExpression
	statementB FilterExpression
	operator   LogicalOperator
}

func (f LogicalFilterExpression) Applies(feature *index.EncodedFeature) bool {
	sigolo.Tracef("LogicalFilterExpression: Operator %d", f.operator)

	switch f.operator {
	case LogicOpOr:
		return f.statementA.Applies(feature) || f.statementB.Applies(feature)
	case LogicOpAnd:
		return f.statementA.Applies(feature) && f.statementB.Applies(feature)
	}

	panic(errors.Errorf("Unknown or unsupported logical operator %d", f.operator))
}

func (f LogicalFilterExpression) Print(indent int) {
	sigolo.Debugf("%sLogicalFilter:", spacing(indent))
	f.statementA.Print(indent + 2)
	sigolo.Debugf("%sAND", spacing(indent))
	f.statementB.Print(indent + 2)
}

type TagFilterExpression struct {
	key      int
	value    int
	operator BinaryOperator
}

func (f TagFilterExpression) Applies(feature *index.EncodedFeature) bool {
	if sigolo.ShouldLogTrace() {
		sigolo.Tracef("TagFilterExpression: %d%s%d", f.key, f.operator.string(), f.value)
	}

	if !feature.HasKey(f.key) {
		return false
	}

	switch f.operator {
	case BinOpEqual:
		return feature.HasTag(f.key, f.value)
	case BinOpNotEqual:
		return !feature.HasTag(f.key, f.value)
	case BinOpGreater:
		return feature.GetValueIndex(f.key) > f.value
	case BinOpGreaterEqual:
		return feature.GetValueIndex(f.key) >= f.value
	case BinOpLower:
		return feature.GetValueIndex(f.key) < f.value
	case BinOpLowerEqual:
		return feature.GetValueIndex(f.key) <= f.value
	default:
		return false
	}
}

func (f TagFilterExpression) Print(indent int) {
	sigolo.Debugf("%s%s: %d%s%d", spacing(indent), "TagFilterExpression", f.key, f.operator.string(), f.value)
}

type KeyFilterExpression struct {
	key         int
	shouldBeSet bool
}

func (f KeyFilterExpression) Applies(feature *index.EncodedFeature) bool {
	if sigolo.ShouldLogTrace() {
		sigolo.Tracef("TagFilterExpression: HasKey(%d)=%v?", f.key, f.shouldBeSet)
	}

	return feature.HasKey(f.key) == f.shouldBeSet
}

func (f KeyFilterExpression) Print(indent int) {
	sigolo.Debugf("%s%s: %d (souldBeSet=%v)", spacing(indent), "KeyFilterExpression", f.key, f.shouldBeSet)
}

func spacing(indent int) string {
	return strings.Repeat(" ", indent)
}

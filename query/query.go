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

var geometryIndex index.GeometryIndex

type Query struct {
	topLevelStatements []Statement
}

func (q *Query) Execute(geomIndex index.GeometryIndex) ([]index.EncodedFeature, error) {
	// TODO Refactor this, since this is just a quick and dirty way to make sub-statement access the geometry index.
	geometryIndex = geomIndex

	sigolo.Info("Start query")
	queryStartTime := time.Now()

	var result []index.EncodedFeature

	for _, statement := range q.topLevelStatements {
		statementResult, err := statement.Execute(nil)
		if err != nil {
			return nil, err
		}
		result = append(result, statementResult...)
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

func (s Statement) GetFeatures(context index.EncodedFeature) (chan []index.EncodedFeature, error) {
	return s.location.GetFeatures(geometryIndex, context, s.objectType)
}

func (s Statement) Applies(feature index.EncodedFeature) (bool, error) {
	// TODO Respect object type (this should also not be necessary, should it?)

	// TODO Is the "IsWithin" call necessary? It should always be true since we only got features that fulfill this requirement.
	isWithin, err := s.location.IsWithin(feature)
	if err != nil {
		return false, err
	}

	applies, err := s.filter.Applies(feature)
	if err != nil {
		return false, err
	}

	return isWithin && applies, nil
}

func (s Statement) Execute(context index.EncodedFeature) ([]index.EncodedFeature, error) {
	s.Print(0)

	featuresChannel, err := s.GetFeatures(context)
	if err != nil {
		return nil, err
	}

	var result []index.EncodedFeature

	for features := range featuresChannel {
		sigolo.Tracef("Received %d features", len(features))

		for _, feature := range features {
			sigolo.Trace("----- next feature -----")
			if feature != nil {
				feature.Print()

				applies, err := s.Applies(feature)
				if err != nil {
					return nil, err
				}

				if applies {
					result = append(result, feature)
				}
			}
		}
	}

	return result, nil
}

func (s Statement) Print(indent int) {
	sigolo.Debugf("%s%s", spacing(indent), "Statement")
	s.location.Print(indent + 2)
	sigolo.Debugf("%stype: %s", spacing(indent+2), s.objectType.string())
	s.filter.Print(indent + 2)
}

/*
	Location expressions
*/

type LocationExpression interface {
	GetFeatures(geometryIndex index.GeometryIndex, context index.EncodedFeature, objectType OsmObjectType) (chan []index.EncodedFeature, error)
	IsWithin(feature index.EncodedFeature) (bool, error)
	Print(indent int)
}

type BboxLocationExpression struct {
	bbox *orb.Bound
}

func (b *BboxLocationExpression) GetFeatures(geometryIndex index.GeometryIndex, context index.EncodedFeature, objectType OsmObjectType) (chan []index.EncodedFeature, error) {
	// TODO Find a better solution than ".string()" for object types
	return geometryIndex.Get(b.bbox, objectType.string())
}

func (b *BboxLocationExpression) IsWithin(feature index.EncodedFeature) (bool, error) {
	if sigolo.ShouldLogTrace() {
		sigolo.Tracef("BboxLocationExpression: IsWithin((%s), %v)", b.string(), feature.GetGeometry())
	}

	switch geometry := feature.GetGeometry().(type) {
	case *orb.Point:
		return b.bbox.Contains(*geometry), nil
	}

	return false, errors.Errorf("Unknown or unsupported geometry type %s", feature.GetGeometry().GeoJSONType())
}

func (b *BboxLocationExpression) Print(indent int) {
	sigolo.Debugf("%slocation: %s(%s)", spacing(indent), "bbox", b.string())
}

func (b *BboxLocationExpression) string() string {
	return fmt.Sprintf("%f, %f, %f, %f", b.bbox.Min.Lon(), b.bbox.Min.Lat(), b.bbox.Max.Lon(), b.bbox.Max.Lat())
}

type ContextAwareLocationExpression struct {
}

func (e *ContextAwareLocationExpression) GetFeatures(geometryIndex index.GeometryIndex, context index.EncodedFeature, objectType OsmObjectType) (chan []index.EncodedFeature, error) {
	/*
		Supported expressions for nodes    :    -   .ways .relations
		Supported expressions for ways     : .nodes   -   .relations
		Supported expressions for relations: .nodes .ways .relations
	*/

	// TODO request the feature for the given context and objectType
	return nil, errors.New("Not yet implemented")
}

func (e *ContextAwareLocationExpression) IsWithin(feature index.EncodedFeature) (bool, error) {
	panic("Not yet implemented")
}

func (e *ContextAwareLocationExpression) Print(indent int) {
	sigolo.Debugf("%sContextAwareLocationExpression", spacing(indent))
}

/*
	Filter expressions
*/

type FilterExpression interface {
	Applies(feature index.EncodedFeature) (bool, error)
	Print(indent int)
}

type NegatedFilterExpression struct {
	baseExpression FilterExpression
}

func (f NegatedFilterExpression) Applies(feature index.EncodedFeature) (bool, error) {
	sigolo.Tracef("NegatedFilterExpression")
	applies, err := f.baseExpression.Applies(feature)
	if err != nil {
		return false, err
	}
	return !applies, nil
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

func (f LogicalFilterExpression) Applies(feature index.EncodedFeature) (bool, error) {
	sigolo.Tracef("LogicalFilterExpression: Operator %d", f.operator)

	if f.operator == LogicOpOr || f.operator == LogicOpAnd {
		aApplies, err := f.statementA.Applies(feature)
		if err != nil {
			return false, err
		}
		bApplies, err := f.statementB.Applies(feature)
		if err != nil {
			return false, err
		}

		if f.operator == LogicOpOr {
			return aApplies || bApplies, nil
		}
		return aApplies && bApplies, nil
	}

	return false, errors.Errorf("Operator %d not supported in LogicalFilterExpression", f.operator)
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

func (f TagFilterExpression) Applies(feature index.EncodedFeature) (bool, error) {
	if sigolo.ShouldLogTrace() {
		sigolo.Tracef("TagFilterExpression: %d%s%d", f.key, f.operator.string(), f.value)
	}

	if !feature.HasKey(f.key) {
		return false, nil
	}

	switch f.operator {
	case BinOpEqual:
		return feature.HasTag(f.key, f.value), nil
	case BinOpNotEqual:
		return !feature.HasTag(f.key, f.value), nil
	case BinOpGreater:
		return feature.GetValueIndex(f.key) > f.value, nil
	case BinOpGreaterEqual:
		return feature.GetValueIndex(f.key) >= f.value, nil
	case BinOpLower:
		return feature.GetValueIndex(f.key) < f.value, nil
	case BinOpLowerEqual:
		return feature.GetValueIndex(f.key) <= f.value, nil
	default:
		return false, errors.Errorf("Operator %d not supported in TagFilterExpression", f.operator)
	}
}

func (f TagFilterExpression) Print(indent int) {
	sigolo.Debugf("%s%s: %d%s%d", spacing(indent), "TagFilterExpression", f.key, f.operator.string(), f.value)
}

type KeyFilterExpression struct {
	key         int
	shouldBeSet bool
}

func (f KeyFilterExpression) Applies(feature index.EncodedFeature) (bool, error) {
	if sigolo.ShouldLogTrace() {
		sigolo.Tracef("TagFilterExpression: HasKey(%d)=%v?", f.key, f.shouldBeSet)
	}

	return feature.HasKey(f.key) == f.shouldBeSet, nil
}

func (f KeyFilterExpression) Print(indent int) {
	sigolo.Debugf("%s%s: %d (souldBeSet=%v)", spacing(indent), "KeyFilterExpression", f.key, f.shouldBeSet)
}

type SubStatementFilterExpression struct {
	statement *Statement
}

func (f SubStatementFilterExpression) Applies(feature index.EncodedFeature) (bool, error) {
	if sigolo.ShouldLogTrace() {
		sigolo.Tracef("SubStatementFilterExpression for object %d?", feature.GetID())
	}

	result, err := f.statement.Execute(feature)
	if err != nil {
		return false, err
	}
	return len(result) > 0, nil
}

func (f SubStatementFilterExpression) Print(indent int) {
	sigolo.Debugf("%s%s", spacing(indent), "SubStatementFilterExpression")
	f.statement.Print(indent + 2)
}

func spacing(indent int) string {
	return strings.Repeat(" ", indent)
}

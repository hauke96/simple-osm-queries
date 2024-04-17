package query

import (
	"fmt"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/pkg/errors"
	"reflect"
	"soq/feature"
	"soq/index"
	"strings"
	"time"
)

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

func (q *Query) Execute(geomIndex index.GeometryIndex) ([]feature.EncodedFeature, error) {
	// TODO Refactor this, since this is just a quick and dirty way to make sub-statement access the geometry index.
	geometryIndex = geomIndex

	sigolo.Info("Start query")
	queryStartTime := time.Now()

	var result []feature.EncodedFeature

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
	objectType feature.OsmObjectType
	filter     FilterExpression
}

func (s Statement) GetFeatures(context feature.EncodedFeature) (chan *index.GetFeaturesResult, error) {
	return s.location.GetFeatures(geometryIndex, context, s.objectType)
}

func (s Statement) Applies(feature feature.EncodedFeature, context feature.EncodedFeature) (bool, error) {
	// TODO Respect object type (this should also not be necessary, should it?)

	applies, err := s.filter.Applies(feature, context)
	if err != nil {
		return false, err
	}

	return applies, nil
}

func (s Statement) Execute(context feature.EncodedFeature) ([]feature.EncodedFeature, error) {
	s.Print(0)

	featuresChannel, err := s.GetFeatures(context)
	if err != nil {
		return nil, err
	}

	var result []feature.EncodedFeature

	for getFeatureResult := range featuresChannel {
		sigolo.Tracef("Received %d features from cell %v", len(getFeatureResult.Features), getFeatureResult.Cell)

		for _, feature := range getFeatureResult.Features {
			sigolo.Trace("----- next feature -----")
			if feature != nil {
				feature.Print()

				applies, err := s.Applies(feature, context)
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
	sigolo.Debugf("%stype: %s", spacing(indent+2), s.objectType.String())
	s.filter.Print(indent + 2)
}

/*
	Location expressions
*/

type LocationExpression interface {
	GetFeatures(geometryIndex index.GeometryIndex, context feature.EncodedFeature, objectType feature.OsmObjectType) (chan *index.GetFeaturesResult, error)
	IsWithin(feature feature.EncodedFeature, context feature.EncodedFeature) (bool, error)
	Print(indent int)
}

type BboxLocationExpression struct {
	bbox *orb.Bound
}

func (b *BboxLocationExpression) GetFeatures(geometryIndex index.GeometryIndex, context feature.EncodedFeature, objectType feature.OsmObjectType) (chan *index.GetFeaturesResult, error) {
	// TODO Find a better solution than ".String()" for object types
	return geometryIndex.Get(b.bbox, objectType.String())
}

func (b *BboxLocationExpression) IsWithin(feature feature.EncodedFeature, context feature.EncodedFeature) (bool, error) {
	if sigolo.ShouldLogTrace() {
		sigolo.Tracef("BboxLocationExpression: IsWithin((%s), %v)", b.string(), feature.GetGeometry())
	}

	switch geometry := feature.GetGeometry().(type) {
	case *orb.Point:
		return b.bbox.Contains(*geometry), nil
	case *orb.LineString:
		return b.bbox.Intersects(geometry.Bound()), nil // TODO Use a more accurate check?
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
	bbox BboxLocationExpression
}

func (e *ContextAwareLocationExpression) GetFeatures(geometryIndex index.GeometryIndex, context feature.EncodedFeature, objectType feature.OsmObjectType) (chan *index.GetFeaturesResult, error) {
	/*
		Supported expressions for nodes    :    -   .ways .relations
		Supported expressions for ways     : .nodes   -   .relations
		Supported expressions for relations: .nodes .ways .relations
	*/

	switch encodedFeature := context.(type) {
	case *feature.EncodedWayFeature:
		switch objectType {
		case feature.OsmObjNode:
			return geometryIndex.GetNodes(encodedFeature.GetNodes())
		}
		return nil, errors.Errorf("Unsupported object type %s for context-aware query of way", objectType.String())
	}

	return nil, errors.Errorf("Encoded feature type '%s' of context object not supported", reflect.TypeOf(context).String())
}

func (e *ContextAwareLocationExpression) IsWithin(feature feature.EncodedFeature, context feature.EncodedFeature) (bool, error) {
	return context.GetGeometry().Bound().Intersects(feature.GetGeometry().Bound()), nil
}

func (e *ContextAwareLocationExpression) Print(indent int) {
	sigolo.Debugf("%sContextAwareLocationExpression", spacing(indent))
}

/*
	Filter expressions
*/

type FilterExpression interface {
	Applies(feature feature.EncodedFeature, context feature.EncodedFeature) (bool, error)
	Print(indent int)
}

type NegatedFilterExpression struct {
	baseExpression FilterExpression
}

func (f NegatedFilterExpression) Applies(feature feature.EncodedFeature, context feature.EncodedFeature) (bool, error) {
	sigolo.Tracef("NegatedFilterExpression")
	applies, err := f.baseExpression.Applies(feature, nil)
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

func (f LogicalFilterExpression) Applies(feature feature.EncodedFeature, context feature.EncodedFeature) (bool, error) {
	sigolo.Tracef("LogicalFilterExpression: Operator %d", f.operator)

	if f.operator == LogicOpOr || f.operator == LogicOpAnd {
		aApplies, err := f.statementA.Applies(feature, context)
		if err != nil {
			return false, err
		}
		bApplies, err := f.statementB.Applies(feature, context)
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

func (f TagFilterExpression) Applies(feature feature.EncodedFeature, context feature.EncodedFeature) (bool, error) {
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

func (f KeyFilterExpression) Applies(feature feature.EncodedFeature, context feature.EncodedFeature) (bool, error) {
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

func (f SubStatementFilterExpression) Applies(feature feature.EncodedFeature, context feature.EncodedFeature) (bool, error) {
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

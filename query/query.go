package query

import (
	"fmt"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/pkg/errors"
	"reflect"
	"soq/feature"
	"soq/index"
	"soq/util"
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

var geometryIndex index.GeometryIndex

type Query struct {
	topLevelStatements []Statement
}

func NewQuery(topLevelStatements []Statement) *Query {
	return &Query{topLevelStatements: topLevelStatements}
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

func NewStatement(locationExpression LocationExpression, objectType feature.OsmObjectType, filterExpression FilterExpression) *Statement {
	return &Statement{
		location:   locationExpression,
		objectType: objectType,
		filter:     filterExpression,
	}
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
	GetFeaturesForCells(geometryIndex index.GeometryIndex, cells []index.CellIndex, objectType feature.OsmObjectType) (chan *index.GetFeaturesResult, error)
	IsWithin(feature feature.EncodedFeature, context feature.EncodedFeature) (bool, error)
	Print(indent int)
}

type BboxLocationExpression struct {
	bbox *orb.Bound
}

func NewBboxLocationExpression(bbox *orb.Bound) *BboxLocationExpression {
	return &BboxLocationExpression{bbox: bbox}
}

func (b *BboxLocationExpression) GetFeatures(geometryIndex index.GeometryIndex, context feature.EncodedFeature, objectType feature.OsmObjectType) (chan *index.GetFeaturesResult, error) {
	// TODO Find a better solution than ".String()" for object types
	return geometryIndex.Get(b.bbox, objectType.String())
}

func (b *BboxLocationExpression) GetFeaturesForCells(geometryIndex index.GeometryIndex, cells []index.CellIndex, objectType feature.OsmObjectType) (chan *index.GetFeaturesResult, error) {
	return geometryIndex.GetFeaturesForCells(cells, objectType.String()), nil
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

func NewContextAwareLocationExpression(bbox BboxLocationExpression) *ContextAwareLocationExpression {
	return &ContextAwareLocationExpression{bbox: bbox}
}

func (e *ContextAwareLocationExpression) GetFeatures(geometryIndex index.GeometryIndex, context feature.EncodedFeature, objectType feature.OsmObjectType) (chan *index.GetFeaturesResult, error) {
	/*
		Supported expressions for nodes    :    -   .ways .relations
		Supported expressions for ways     : .nodes   -   .relations
		Supported expressions for relations: .nodes .ways .relations
	*/
	if context == nil {
		return nil, errors.Errorf("Context feature must not be 'nil'")
	}

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

func (e *ContextAwareLocationExpression) GetFeaturesForCells(geometryIndex index.GeometryIndex, cells []index.CellIndex, objectType feature.OsmObjectType) (chan *index.GetFeaturesResult, error) {
	return geometryIndex.GetFeaturesForCells(cells, objectType.String()), nil
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

func NewNegatedFilterExpression(baseExpression FilterExpression) *NegatedFilterExpression {
	return &NegatedFilterExpression{baseExpression: baseExpression}
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

func NewLogicalFilterExpression(statementA FilterExpression, statementB FilterExpression, operator LogicalOperator) *LogicalFilterExpression {
	return &LogicalFilterExpression{
		statementA: statementA,
		statementB: statementB,
		operator:   operator,
	}
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

func NewTagFilterExpression(key int, value int, operator BinaryOperator) *TagFilterExpression {
	return &TagFilterExpression{
		key:      key,
		value:    value,
		operator: operator,
	}
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

func NewKeyFilterExpression(key int, shouldBeSet bool) *KeyFilterExpression {
	return &KeyFilterExpression{
		key:         key,
		shouldBeSet: shouldBeSet,
	}
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
	statement   *Statement
	cachedCells []index.CellIndex
	idCache     map[uint64]uint64
}

func NewSubStatementFilterExpression(statement *Statement) *SubStatementFilterExpression {
	return &SubStatementFilterExpression{
		statement:   statement,
		cachedCells: []index.CellIndex{},
		idCache:     make(map[uint64]uint64),
	}
}

func (f *SubStatementFilterExpression) Applies(featureToCheck feature.EncodedFeature, context feature.EncodedFeature) (bool, error) {
	if sigolo.ShouldLogTrace() {
		sigolo.Tracef("SubStatementFilterExpression for object %d?", featureToCheck.GetID())
	}

	context = featureToCheck

	var err error
	var featuresChannel chan *index.GetFeaturesResult
	cells := map[index.CellIndex]index.CellIndex{} // Map instead of array to have quick lookups

	switch contextFeature := context.(type) {
	case *feature.EncodedWayFeature:
		for _, node := range contextFeature.Nodes {
			cell := geometryIndex.GetCellIdForCoordinate(node.Lon, node.Lat)
			if _, ok := cells[cell]; !ok {
				cells[cell] = cell
			}
		}
	}
	if len(cells) == 0 {
		return false, errors.Errorf("No cells found for context feature %d", context.GetID())
	}

	// Get those cells that are not in the cache
	var cellsToFetch []index.CellIndex
	for _, cell := range cells {
		if !util.Contains(f.cachedCells, cell) {
			cellsToFetch = append(cellsToFetch, cell)
		}
	}

	// Fetch data only of those cells needed
	if len(cellsToFetch) != 0 {
		featuresChannel, err = f.statement.location.GetFeaturesForCells(geometryIndex, cellsToFetch, f.statement.objectType)
		if err != nil {
			return false, err
		}

		for getFeatureResult := range featuresChannel {
			sigolo.Tracef("Received %d features from cell %v", len(getFeatureResult.Features), getFeatureResult.Cell)

			for _, foundFeature := range getFeatureResult.Features {
				sigolo.Trace("----- next feature -----")
				if foundFeature != nil {
					foundFeature.Print()

					applies, err := f.statement.Applies(foundFeature, context)
					if err != nil {
						return false, err
					}

					if applies {
						f.idCache[foundFeature.GetID()] = foundFeature.GetID()
					}
				}
			}
		}

		f.cachedCells = append(f.cachedCells, cellsToFetch...)
	}

	// Check whether at least one sub-feature of the context is within the list of IDs that fulfill the sub-statement.
	switch contextFeature := context.(type) {
	case *feature.EncodedWayFeature:
		for _, node := range contextFeature.Nodes {
			if _, ok := f.idCache[uint64(node.ID)]; ok {
				return true, nil
			}
		}
	}

	return false, nil
}

func (f *SubStatementFilterExpression) Print(indent int) {
	sigolo.Debugf("%s%s", spacing(indent), "SubStatementFilterExpression")
	f.statement.Print(indent + 2)
}

func spacing(indent int) string {
	return strings.Repeat(" ", indent)
}

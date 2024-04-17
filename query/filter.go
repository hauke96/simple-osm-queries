package query

import (
	"github.com/hauke96/sigolo/v2"
	"github.com/pkg/errors"
	"soq/feature"
	"soq/index"
	"soq/util"
	"strings"
)

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

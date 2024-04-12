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
	return false
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
	sigolo.Tracef("TagFilterExpression: HasTag(%d, %d)?", f.key, f.value)
	return feature.HasTag(f.key, f.value)
}

func (f TagFilterExpression) Print(indent int) {
	sigolo.Debugf("%s%s: %d%s%d", spacing(indent), "TagFilterExpression", f.key, f.operator.string(), f.value)
}

func spacing(indent int) string {
	return strings.Repeat(" ", indent)
}

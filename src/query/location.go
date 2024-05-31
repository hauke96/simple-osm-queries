package query

import (
	"fmt"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/pkg/errors"
	"reflect"
	"soq/feature"
	"soq/index"
)

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

func (b *BboxLocationExpression) GetBbox() *orb.Bound {
	return b.bbox
}

func (b *BboxLocationExpression) string() string {
	return fmt.Sprintf("%f, %f, %f, %f", b.bbox.Min.Lon(), b.bbox.Min.Lat(), b.bbox.Max.Lon(), b.bbox.Max.Lat())
}

type ContextAwareLocationExpression struct {
}

func NewContextAwareLocationExpression() *ContextAwareLocationExpression {
	return &ContextAwareLocationExpression{}
}

func (e *ContextAwareLocationExpression) GetFeatures(geometryIndex index.GeometryIndex, context feature.EncodedFeature, objectType feature.OsmObjectType) (chan *index.GetFeaturesResult, error) {
	// TODO Should never been called since the SubStatementFilterExpression itself queries the features and does some caching.
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
		sigolo.Debug("Get Features context way")
		switch objectType {
		case feature.OsmObjNode:
			return geometryIndex.GetNodes(encodedFeature.GetNodes())
		}
		return nil, errors.Errorf("Unsupported object type %s for context-aware expression of way", objectType.String())
	default:
		sigolo.Debugf("Unsupported context %s", reflect.TypeOf(context).String())
		return nil, errors.Errorf("Unsupported feature type %s for context-aware expression", reflect.TypeOf(context).String())
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

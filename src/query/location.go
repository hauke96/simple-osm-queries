package query

import (
	"fmt"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/pkg/errors"
	"soq/common"
	"soq/feature"
	"soq/index"
	ownOsm "soq/osm"
)

type LocationExpression interface {
	GetFeatures(geometryIndex index.GeometryIndex, context feature.Feature, objectType ownOsm.OsmObjectType) (chan *index.GetFeaturesResult, error)
	GetFeaturesForCells(geometryIndex index.GeometryIndex, cells []common.CellIndex, objectType ownOsm.OsmObjectType) (chan *index.GetFeaturesResult, error)
	IsWithin(feature feature.Feature, context feature.Feature) (bool, error)
	Print(indent int)
}

type BboxLocationExpression struct {
	bbox *orb.Bound
}

func NewBboxLocationExpression(bbox *orb.Bound) *BboxLocationExpression {
	return &BboxLocationExpression{bbox: bbox}
}

func (b *BboxLocationExpression) GetFeatures(geometryIndex index.GeometryIndex, context feature.Feature, objectType ownOsm.OsmObjectType) (chan *index.GetFeaturesResult, error) {
	return geometryIndex.Get(b.bbox, objectType)
}

func (b *BboxLocationExpression) GetFeaturesForCells(geometryIndex index.GeometryIndex, cells []common.CellIndex, objectType ownOsm.OsmObjectType) (chan *index.GetFeaturesResult, error) {
	return geometryIndex.GetFeaturesForCells(cells, objectType), nil
}

func (b *BboxLocationExpression) IsWithin(feature feature.Feature, context feature.Feature) (bool, error) {
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

func (e *ContextAwareLocationExpression) GetFeatures(geometryIndex index.GeometryIndex, context feature.Feature, objectType ownOsm.OsmObjectType) (chan *index.GetFeaturesResult, error) {
	// Should never been called since the SubStatementFilterExpression itself queries the features and does some caching.
	panic("THe GetFeatures function of a ContextAwareLocationExpression should never been called. This is a bug.")
}

func (e *ContextAwareLocationExpression) GetFeaturesForCells(geometryIndex index.GeometryIndex, cells []common.CellIndex, objectType ownOsm.OsmObjectType) (chan *index.GetFeaturesResult, error) {
	return geometryIndex.GetFeaturesForCells(cells, objectType), nil
}

func (e *ContextAwareLocationExpression) IsWithin(feature feature.Feature, context feature.Feature) (bool, error) {
	return context.GetGeometry().Bound().Intersects(feature.GetGeometry().Bound()), nil
}

func (e *ContextAwareLocationExpression) Print(indent int) {
	sigolo.Debugf("%sContextAwareLocationExpression", spacing(indent))
}

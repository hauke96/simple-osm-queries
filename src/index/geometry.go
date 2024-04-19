package index

import (
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"soq/feature"
)

type GetFeaturesResult struct {
	Cell     CellIndex
	Features []feature.EncodedFeature
}

type GeometryIndex interface {
	Import(filename string) error
	Get(bbox *orb.Bound, objectType string) (chan *GetFeaturesResult, error)
	GetFeaturesForCells(cells []CellIndex, objectType string) chan *GetFeaturesResult
	GetNodes(nodes osm.WayNodes) (chan *GetFeaturesResult, error)
	GetCellIndexForCoordinate(x float64, y float64) CellIndex
}

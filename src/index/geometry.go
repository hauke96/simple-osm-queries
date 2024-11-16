package index

import (
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"soq/common"
	"soq/feature"
	ownOsm "soq/osm"
)

type GetFeaturesResult struct {
	Cell     common.CellIndex
	Features []feature.Feature
}

type GeometryIndex interface {
	Get(bbox *orb.Bound, objectType ownOsm.OsmObjectType) (chan *GetFeaturesResult, error)
	GetFeaturesForCells(cells []common.CellIndex, objectType ownOsm.OsmObjectType) chan *GetFeaturesResult
	GetNodes(nodes osm.WayNodes) (chan *GetFeaturesResult, error)
	GetCellIndexForCoordinate(x float64, y float64) common.CellIndex
}

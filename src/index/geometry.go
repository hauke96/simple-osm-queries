package index

import (
	"soq/common"
	"soq/feature"
	ownOsm "soq/osm"

	"github.com/paulmach/orb"
)

type GetFeaturesResult struct {
	Cell     common.CellExtent
	Features []feature.Feature
}

type GeometryIndex interface {
	Get(bbox *orb.Bound, objectType ownOsm.OsmObjectType) (chan *GetFeaturesResult, error)
	GetFeaturesForCells(cells []common.CellIndex, objectType ownOsm.OsmObjectType) chan *GetFeaturesResult
	GetCellIndexForCoordinate(x float64, y float64) common.CellIndex
}

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
	GetNodes(nodes osm.WayNodes) (chan *GetFeaturesResult, error)
}

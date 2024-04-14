package index

import (
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"soq/feature"
)

type GeometryIndex interface {
	Import(filename string) error
	Get(bbox *orb.Bound, objectType string) (chan []feature.EncodedFeature, error)
	GetNodes(nodes osm.WayNodes) (chan []feature.EncodedFeature, error)
}

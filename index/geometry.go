package index

import (
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
)

type GeometryIndex interface {
	Import(filename string) error
	Get(bbox *orb.Bound, objectType string) (chan []EncodedFeature, error)
	GetNodes(nodes osm.WayNodes) (chan []EncodedFeature, error)
}

package index

import "github.com/paulmach/orb"

type GeometryIndex interface {
	Import(filename string) error
	Get(bbox *orb.Bound, objectType string) (chan []*EncodedFeature, error)
}

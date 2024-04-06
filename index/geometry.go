package index

import "github.com/paulmach/orb"

type GeometryIndex interface {
	Import(filename string) error
	Get(bbox *orb.Bound, objectType string) ([]EncodedFeature, error) // TODO Maybe returning a channel might be a good idea (to filter things cell by cell)
}

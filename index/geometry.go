package index

type GeometryIndex interface {
	Import(filename string) (GeometryIndex, error)
	SaveToFile(filename string) error
	LoadFromFile(filename string) (GeometryIndex, error)

	Get(bbox BBOX) chan []EncodedFeature
}

type BBOX struct {
	X1, Y1 int // lower-left corner
	X2, Y2 int // upper-right corner
}

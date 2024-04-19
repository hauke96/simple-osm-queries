package feature

import "fmt"

type OsmObjectType int

const (
	OsmObjNode OsmObjectType = iota
	OsmObjWay
	OsmObjRelation
)

func (o OsmObjectType) String() string {
	switch o {
	case OsmObjNode:
		return "node"
	case OsmObjWay:
		return "way"
	case OsmObjRelation:
		return "relation"
	}
	return fmt.Sprintf("[!UNKNOWN OsmObjectType %d]", o)
}

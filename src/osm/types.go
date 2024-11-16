package osm

import (
	"fmt"
)

// OsmObjectType is an enum for all the three existing object types in OpenStreetMap.
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
	panic(fmt.Sprintf("[!UNKNOWN OsmObjectType %d]", o))
}

// OsmQueryType is similar to OsmObjectType but contains all possible object types that can be queried, which at least
// contains the two directions in relations (child and parent relation memberships).
type OsmQueryType int

const (
	OsmQueryNode OsmQueryType = iota
	OsmQueryWay
	OsmQueryRelation
	OsmQueryChildRelation
)

func (o OsmQueryType) String() string {
	switch o {
	case OsmQueryNode:
		return "nodes"
	case OsmQueryWay:
		return "ways"
	case OsmQueryRelation:
		return "relations"
	case OsmQueryChildRelation:
		return "child_relations"
	}
	panic(fmt.Sprintf("[!UNKNOWN OsmQueryectType %d]", o))
}

func (o OsmQueryType) GetObjectType() OsmObjectType {
	switch o {
	case OsmQueryNode:
		return OsmObjNode
	case OsmQueryWay:
		return OsmObjWay
	case OsmQueryRelation:
		return OsmObjRelation
	case OsmQueryChildRelation:
		return OsmObjRelation
	}
	panic(fmt.Sprintf("[!UNKNOWN OsmQueryectType %d]", o))
}

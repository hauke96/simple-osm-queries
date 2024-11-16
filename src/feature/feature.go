package feature

import (
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
)

type Feature interface {
	// TODO Refactor these functions, only keep those that are needed
	GetID() uint64
	GetGeometry() orb.Geometry
	GetKeys() []byte
	GetValues() []int

	HasKey(keyIndex int) bool
	GetValueIndex(keyIndex int) int
	HasTag(keyIndex int, valueIndex int) bool
	Print()
}

type NodeFeature interface {
	Feature
	GetLon() float64
	GetLat() float64
	GetWayIds() []osm.WayID
	SetWayIds(wayIds []osm.WayID)
	GetRelationIds() []osm.RelationID
	SetRelationIds(relationIds []osm.RelationID)
}

type WayFeature interface {
	Feature
	GetNodes() osm.WayNodes
	GetRelationIds() []osm.RelationID
	SetRelationIds(relationIds []osm.RelationID)
}

type RelationFeature interface {
	Feature
	GetNodeIds() []osm.NodeID
	GetWayIds() []osm.WayID
	GetChildRelationIds() []osm.RelationID
	GetParentRelationIds() []osm.RelationID
	SetParentRelationIds(relationIds []osm.RelationID)
	SetGeometry(geometry orb.Geometry)
}

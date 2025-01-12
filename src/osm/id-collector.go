package osm

import (
	"github.com/paulmach/osm"
	"soq/common"
)

type ReverseIdCollector struct {
	NodeIdsInRelations map[osm.NodeID]common.Void
	WayIdsInRelations  map[osm.WayID]common.Void
}

func NewReverseIdCollector() *ReverseIdCollector {
	return &ReverseIdCollector{}
}

func (c *ReverseIdCollector) Name() string {
	return "ReverseIdCollector"
}

func (c *ReverseIdCollector) Init() error {
	c.NodeIdsInRelations = map[osm.NodeID]common.Void{}
	c.WayIdsInRelations = map[osm.WayID]common.Void{}
	return nil
}

func (c *ReverseIdCollector) HandleNode(node *osm.Node) error {
	return nil
}

func (c *ReverseIdCollector) HandleWay(way *osm.Way) error {
	return nil
}

func (c *ReverseIdCollector) HandleRelation(relation *osm.Relation) error {
	for _, member := range relation.Members {
		switch member.Type {
		case osm.TypeNode:
			c.NodeIdsInRelations[osm.NodeID(member.Ref)] = common.Void{}
		case osm.TypeWay:
			c.WayIdsInRelations[osm.WayID(member.Ref)] = common.Void{}
		}
	}
	return nil
}

func (c *ReverseIdCollector) Done() error {
	return nil
}

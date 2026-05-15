package importing

import (
	"math"
	"soq/common"
	"soq/index"
	indexCommon "soq/index/common"
	"soq/index/storage"
	"soq/profiler"

	"github.com/paulmach/osm"
)

type OsmToRawFeaturesImporter struct {
	tagIndex               *index.TagIndex
	tagIndexTempValueArray []int
	featureStorageWriter   *storage.FeatureStorageWriter
	cellExtents            []common.CellExtent
	cellWidth              float64
	cellHeight             float64
}

func NewOsmToRawFeaturesImporter(tagIndex *index.TagIndex, featureStorageWriter *storage.FeatureStorageWriter, cellExtents []common.CellExtent, cellWidth float64, cellHeight float64) *OsmToRawFeaturesImporter {
	return &OsmToRawFeaturesImporter{
		tagIndex:               tagIndex,
		tagIndexTempValueArray: make([]int, 0),
		featureStorageWriter:   featureStorageWriter,
		cellExtents:            cellExtents,
		cellWidth:              cellWidth,
		cellHeight:             cellHeight,
	}
}

func (i *OsmToRawFeaturesImporter) Name() string {
	return "OsmToRawFeaturesImporter"
}

func (i *OsmToRawFeaturesImporter) Init() error {
	return nil
}

func (i *OsmToRawFeaturesImporter) HandleNode(node *osm.Node) error {
	key := profiler.StartMeasurement()
	defer profiler.EndMeasurement(key)

	encodedKeys, encodedValues := i.tagIndex.EncodeTags(node.Tags, i.tagIndexTempValueArray)
	encodedFeature := &indexCommon.EncodedNodeFeature{
		AbstractEncodedFeature: indexCommon.AbstractEncodedFeature{
			ID:     uint64(node.ID),
			Keys:   encodedKeys,
			Values: encodedValues,
		},
	}

	for _, cellExtent := range i.cellExtents {
		if cellExtent.ContainsLonLat(node.Lon, node.Lat, i.cellWidth, i.cellHeight) {
			err := i.featureStorageWriter.WriteNodeFeature(encodedFeature, cellExtent, true)
			if err != nil {
				return err
			}
			break
		}
	}

	return nil
}

func (i *OsmToRawFeaturesImporter) HandleWay(way *osm.Way) error {
	key := profiler.StartMeasurement()
	defer profiler.EndMeasurement(key)

	encodedKeys, encodedValues := i.tagIndex.EncodeTags(way.Tags, i.tagIndexTempValueArray)
	encodedFeature := &indexCommon.EncodedWayFeature{
		AbstractEncodedFeature: indexCommon.AbstractEncodedFeature{
			ID:     uint64(way.ID),
			Keys:   encodedKeys,
			Values: encodedValues,
		},
		Nodes: way.Nodes,
	}

	for _, cellExtent := range i.cellExtents {
		for _, node := range way.Nodes {
			if cellExtent.ContainsLonLat(node.Lon, node.Lat, i.cellWidth, i.cellHeight) {
				err := i.featureStorageWriter.WriteWayFeature(encodedFeature, cellExtent, true)
				if err != nil {
					return err
				}
				break
			}
		}
	}

	return nil
}

func (i *OsmToRawFeaturesImporter) HandleRelation(relation *osm.Relation) error {
	key := profiler.StartMeasurement()
	defer profiler.EndMeasurement(key)

	var nodeIds []osm.NodeID
	var wayIds []osm.WayID
	var childRelationIds []osm.RelationID

	for _, member := range relation.Members {
		switch member.Type {
		case osm.TypeNode:
			nodeId := osm.NodeID(member.Ref)
			nodeIds = append(nodeIds, nodeId)
		case osm.TypeWay:
			wayId := osm.WayID(member.Ref)
			wayIds = append(wayIds, wayId)
		case osm.TypeRelation:
			relId := osm.RelationID(member.Ref)
			childRelationIds = append(childRelationIds, relId)
		}
	}

	encodedKeys, encodedValues := i.tagIndex.EncodeTags(relation.Tags, i.tagIndexTempValueArray)
	encodedFeature := &indexCommon.EncodedRelationFeature{
		AbstractEncodedFeature: indexCommon.AbstractEncodedFeature{
			ID:     uint64(relation.ID),
			Keys:   encodedKeys,
			Values: encodedValues,
		},
		NodeIds:          nodeIds,
		WayIds:           wayIds,
		ChildRelationIds: childRelationIds,
	}

	// TODO is minValue a proper value to show "doesn't have a cell yet"?
	return i.featureStorageWriter.WriteRelationFeature(encodedFeature, common.CellExtent{common.CellIndex{math.MinInt32, math.MinInt32}, common.CellIndex{math.MinInt32, math.MinInt32}}, true)
}

func (i *OsmToRawFeaturesImporter) Done() error {
	key := profiler.StartMeasurement()
	defer profiler.EndMeasurement(key)

	return i.featureStorageWriter.FlushCaches()
}

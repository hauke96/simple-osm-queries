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
	scaledCellExtents      [][4]float64
	cellWidth              float64
	cellHeight             float64
	rawRelationsCellExtent common.CellExtent // Raw relations don't have bounds or geometries, so we first write them to this extent and later add coordinates.
}

func NewOsmToRawFeaturesImporter(tagIndex *index.TagIndex, featureStorageWriter *storage.FeatureStorageWriter, cellExtents []common.CellExtent, cellWidth float64, cellHeight float64) *OsmToRawFeaturesImporter {
	scaledCellExtents := make([][4]float64, len(cellExtents))
	for i := 0; i < len(cellExtents); i++ {
		scaledCellExtents[i] = [4]float64{
			float64(cellExtents[i].LowerLeftCell().X()) * cellWidth,
			float64(cellExtents[i].LowerLeftCell().Y()) * cellHeight,
			// +1 because the cells are inclusive, i.e. a right border at cell x=5 includes all coordinates <6. To
			// allow this inclusion, we use +1 and the "<" (instead of "<=") operator in the condition.
			float64(cellExtents[i].UpperRightCell().X()+1) * cellWidth,
			float64(cellExtents[i].UpperRightCell().Y()+1) * cellHeight,
		}
	}

	return &OsmToRawFeaturesImporter{
		tagIndex:               tagIndex,
		tagIndexTempValueArray: tagIndex.NewTempEncodedValueArray(),
		featureStorageWriter:   featureStorageWriter,
		cellExtents:            cellExtents,
		scaledCellExtents:      scaledCellExtents,
		cellWidth:              cellWidth,
		cellHeight:             cellHeight,
		rawRelationsCellExtent: common.CellExtent{common.CellIndex{math.MinInt32, math.MinInt32}, common.CellIndex{math.MinInt32, math.MinInt32}},
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
	point := node.Point()
	encodedFeature := &indexCommon.EncodedNodeFeature{
		AbstractEncodedFeature: indexCommon.AbstractEncodedFeature{
			ID:       uint64(node.ID),
			Geometry: &point,
			Keys:     encodedKeys,
			Values:   encodedValues,
		},
	}

	for j, scaledExtent := range i.scaledCellExtents {
		if i.ContainsLonLat(node.Lon, node.Lat, scaledExtent) {
			err := i.featureStorageWriter.WriteNodeFeature(encodedFeature, i.cellExtents[j])
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

	for j, scaledExtent := range i.scaledCellExtents {
		for _, node := range way.Nodes {
			if i.ContainsLonLat(node.Lon, node.Lat, scaledExtent) {
				err := i.featureStorageWriter.WriteWayFeature(encodedFeature, i.cellExtents[j])
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
	return i.featureStorageWriter.WriteRelationFeature(encodedFeature, i.rawRelationsCellExtent)
}

func (i *OsmToRawFeaturesImporter) Done() error {
	key := profiler.StartMeasurement()
	defer profiler.EndMeasurement(key)

	return i.featureStorageWriter.FlushData()
}

func (i *OsmToRawFeaturesImporter) ContainsLonLat(lon float64, lat float64, scaledExtent [4]float64) bool {
	return lon >= scaledExtent[0] &&
		lat >= scaledExtent[1] &&
		// "<" operator because elements [2] and [3] are one cell width/height larger than the bound. This allows the
		// check to include the right and upper cells. E.g. a coordinate falling into cell "5.5" (i.e. in the middle of
		// cell 5) is included when the extent ends at cell 5, because the scaledExtent value is 6 and with "<6" we then
		// include all values that fall into the cell 5.
		lon < scaledExtent[2] &&
		lat < scaledExtent[3]
}

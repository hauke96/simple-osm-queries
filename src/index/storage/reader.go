package storage

import (
	"bufio"
	"encoding/json"
	"os"
	"soq/common"
	"soq/feature"

	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/osm"
	"github.com/pkg/errors"
)

type FeatureStorageReader struct {
	indexFileReader *bufio.Reader
	indexMetadata   *indexMetadata
}

func NewFeatureStorageReader(baseFolder string) *FeatureStorageReader {
	metadataFileName := baseFolder + "/metadata.json"

	metadataFileContent, err := os.ReadFile(metadataFileName)
	sigolo.FatalCheck(errors.Wrapf(err, "Unable to read metadata file %s", metadataFileName))

	metadata := &indexMetadata{}
	err = json.Unmarshal(metadataFileContent, metadata)
	sigolo.FatalCheck(errors.Wrapf(err, "Unable to unmarshal content of metadata file %s", metadataFileName))

	var file *os.File
	indexFileName := baseFolder + "/index"
	file, err = os.OpenFile(indexFileName, os.O_RDONLY, 0666)
	sigolo.FatalCheck(errors.Wrapf(err, "Unable to open index file %s", indexFileName))

	return &FeatureStorageReader{
		indexFileReader: bufio.NewReader(file),
		indexMetadata:   metadata,
	}
}

func (r FeatureStorageReader) readNodes(cellExtent common.CellExtent) []feature.NodeFeature {
	features := []feature.NodeFeature{}

	// TODO read from disk

	return features
}

func (r FeatureStorageReader) readWays(cellExtent common.CellExtent) ([]feature.WayFeature, map[osm.NodeID][]osm.WayID) {
	features := []feature.WayFeature{}

	// TODO read from disk

	var nodeToWayMapping map[osm.NodeID][]osm.WayID

	for _, way := range features {
		for _, node := range way.GetNodes() {
			nodeToWayMapping[node.ID] = append(nodeToWayMapping[node.ID], osm.WayID(way.GetID()))
		}
	}

	return features, nodeToWayMapping
}

func (r FeatureStorageReader) readRelations(cellExtent common.CellExtent) ([]feature.RelationFeature, map[osm.NodeID][]osm.RelationID, map[osm.WayID][]osm.RelationID, map[osm.RelationID][]osm.RelationID) {
	features := []feature.RelationFeature{}

	// TODO read from disk

	var nodeToRelationMapping map[osm.NodeID][]osm.RelationID
	var wayToRelationMapping map[osm.WayID][]osm.RelationID
	var relationToRelationMapping map[osm.RelationID][]osm.RelationID

	for _, relation := range features {
		for _, nodeId := range relation.GetNodeIds() {
			nodeToRelationMapping[nodeId] = append(nodeToRelationMapping[nodeId], osm.RelationID(relation.GetID()))
		}
		for _, wayId := range relation.GetWayIds() {
			wayToRelationMapping[wayId] = append(wayToRelationMapping[wayId], osm.RelationID(relation.GetID()))
		}
		for _, relationId := range relation.GetChildRelationIds() {
			relationToRelationMapping[relationId] = append(relationToRelationMapping[relationId], osm.RelationID(relation.GetID()))
		}
	}

	return features, nodeToRelationMapping, wayToRelationMapping, relationToRelationMapping
}

package storage

import (
	"bufio"
	"encoding/json"
	"os"
	"soq/common"
	indexCommon "soq/index/common"

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

func (r FeatureStorageReader) readNodes(cellExtent common.CellExtent) []indexCommon.EncodedNodeFeature {
	features := []indexCommon.EncodedNodeFeature{}

	// TODO read from disk

	return features
}

func (r FeatureStorageReader) readWays(cellExtent common.CellExtent) ([]indexCommon.EncodedWayFeature, map[osm.NodeID][]osm.WayID) {
	features := []indexCommon.EncodedWayFeature{}

	// TODO read from disk

	var nodeToWayMapping map[osm.NodeID][]osm.WayID

	for _, way := range features {
		for _, node := range way.Nodes {
			nodeToWayMapping[node.ID] = append(nodeToWayMapping[node.ID], osm.WayID(way.ID))
		}
	}

	return features, nodeToWayMapping
}

func (r FeatureStorageReader) readRelations(cellExtent common.CellExtent) ([]indexCommon.EncodedRelationFeature, map[osm.NodeID][]osm.RelationID, map[osm.WayID][]osm.RelationID, map[osm.RelationID][]osm.RelationID) {
	features := []indexCommon.EncodedRelationFeature{}

	// TODO read from disk

	var nodeToRelationMapping map[osm.NodeID][]osm.RelationID
	var wayToRelationMapping map[osm.WayID][]osm.RelationID
	var relationToRelationMapping map[osm.RelationID][]osm.RelationID

	for _, relation := range features {
		for _, nodeId := range relation.NodeIds {
			nodeToRelationMapping[nodeId] = append(nodeToRelationMapping[nodeId], osm.RelationID(relation.ID))
		}
		for _, wayId := range relation.WayIds {
			wayToRelationMapping[wayId] = append(wayToRelationMapping[wayId], osm.RelationID(relation.ID))
		}
		for _, relationId := range relation.ChildRelationIds {
			relationToRelationMapping[relationId] = append(relationToRelationMapping[relationId], osm.RelationID(relation.ID))
		}
	}

	return features, nodeToRelationMapping, wayToRelationMapping, relationToRelationMapping
}

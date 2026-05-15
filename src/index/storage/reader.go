package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"os"
	"soq/common"
	indexCommon "soq/index/common"

	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/osm"
	"github.com/pkg/errors"
)

type FeatureStorageReader struct {
	indexFile       *os.File
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
		indexFile:       file,
		indexFileReader: bufio.NewReader(file),
		indexMetadata:   metadata,
	}
}

func (r FeatureStorageReader) readRawNodes(cellExtent common.CellExtent) []*indexCommon.RawEncodedNodeFeature {
	features := []*indexCommon.RawEncodedNodeFeature{}

	cellOffsets := r.indexMetadata.getCellMetadata(cellExtent).NodeOffsets
	data := r.read(cellOffsets)

	// Storage format see FeatureStorageWriter::writeNodeData
	for pos := 0; pos < len(data); pos++ {
		id := int64(binary.LittleEndian.Uint64(data[pos:]))

		numWayIds := int(binary.LittleEndian.Uint16(data[pos+20:]))
		if numWayIds != 0 {
			sigolo.Fatalf("Expected number of ways on raw node %d to be 0 but was %d", id, numWayIds)
		}

		numRelationIds := int(binary.LittleEndian.Uint16(data[pos+22:]))
		if numRelationIds != 0 {
			sigolo.Fatalf("Expected number of relations on raw node %d to be 0 but was %d", id, numRelationIds)
		}

		numEncodedKeyBytes := int(binary.LittleEndian.Uint16(data[pos+16:]))
		numValues := int(binary.LittleEndian.Uint16(data[pos+18:]))

		numberOfBytes := 8 + 4 + 4 + 2 + 2 + 2 + 2 + numEncodedKeyBytes + numValues*3

		rawEncodedNode := &indexCommon.RawEncodedNodeFeature{
			Data:        data[pos : pos+numberOfBytes],
			WayIds:      make([]osm.WayID, 0),
			RelationIds: make([]osm.RelationID, 0),
		}
		features = append(features, rawEncodedNode)
	}

	return features
}

func (r FeatureStorageReader) readRawWays(cellExtent common.CellExtent) ([]*indexCommon.RawEncodedWayFeature, map[osm.NodeID][]osm.WayID) {
	features := []*indexCommon.RawEncodedWayFeature{}

	cellOffsets := r.indexMetadata.getCellMetadata(cellExtent).WayOffsets
	data := r.read(cellOffsets)

	// Storage format see FeatureStorageWriter::writeWayData
	for pos := 0; pos < len(data); pos++ {
		id := int64(binary.LittleEndian.Uint64(data[pos:]))

		numRelationIds := int(binary.LittleEndian.Uint16(data[pos+14:]))
		if numRelationIds != 0 {
			sigolo.Fatalf("Expected number of relations on raw way %d to be 0 but was %d", id, numRelationIds)
		}

		numEncodedKeyBytes := int(binary.LittleEndian.Uint16(data[pos+8:]))
		numValues := int(binary.LittleEndian.Uint16(data[pos+10:]))

		numberOfBytes := 8 + 2 + 2 + 2 + 2 + numEncodedKeyBytes + numValues*3

		rawEncodedWay := &indexCommon.RawEncodedWayFeature{
			Data:        data[pos : pos+numberOfBytes],
			RelationIds: make([]osm.RelationID, 0),
		}
		features = append(features, rawEncodedWay)
	}

	var nodeToWayMapping map[osm.NodeID][]osm.WayID

	for _, way := range features {
		for _, node := range way.GetNodes() {
			nodeToWayMapping[node.ID] = append(nodeToWayMapping[node.ID], osm.WayID(way.GetID()))
		}
	}

	return features, nodeToWayMapping
}

func (r FeatureStorageReader) readRelations(cellExtent common.CellExtent) ([]*indexCommon.RawEncodedRelationFeature, map[osm.NodeID][]osm.RelationID, map[osm.WayID][]osm.RelationID, map[osm.RelationID][]osm.RelationID) {
	features := []*indexCommon.RawEncodedRelationFeature{}

	cellOffsets := r.indexMetadata.getCellMetadata(cellExtent).RelationOffsets
	data := r.read(cellOffsets)

	// Storage format see FeatureStorageWriter::writeRelationData
	for pos := 0; pos < len(data); pos++ {
		id := int64(binary.LittleEndian.Uint64(data[pos:]))

		numRelationIds := int(binary.LittleEndian.Uint16(data[pos+34:]))
		if numRelationIds != 0 {
			sigolo.Fatalf("Expected number of parent-relations on raw relation %d to be 0 but was %d", id, numRelationIds)
		}

		numEncodedKeyBytes := int(binary.LittleEndian.Uint16(data[pos+24:]))
		numValues := int(binary.LittleEndian.Uint16(data[pos+26:]))

		numberOfBytes := 8 + 16 + 2 + 2 + 2 + 2 + 2 + 2 + numEncodedKeyBytes + numValues*3

		rawEncodedRelation := &indexCommon.RawEncodedRelationFeature{
			Data:              data[pos : pos+numberOfBytes],
			ParentRelationIds: make([]osm.RelationID, 0),
		}
		features = append(features, rawEncodedRelation)
	}

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

func (r FeatureStorageReader) read(cellOffsets []indexCellOffset) []byte {
	numBytes := int64(0)
	for _, cellOffset := range cellOffsets {
		numBytes += cellOffset.EndIndex - cellOffset.StartIndex
	}

	buffer := make([]byte, numBytes)
	posInBuffer := int64(0)

	for _, cellOffset := range cellOffsets {
		numBytesToRead := cellOffset.EndIndex - cellOffset.StartIndex

		numBytesActuallyRead, err := r.indexFile.ReadAt(buffer[posInBuffer:numBytesToRead], cellOffset.StartIndex)
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to read bytes %d to %d from index file", cellOffset.StartIndex, cellOffset.EndIndex))
		if int64(numBytesActuallyRead) != numBytesToRead {
			sigolo.Fatalf("Error reading bytes %d to %d from index file. Expected to read %d bytes but actually read %d bytes.", cellOffset.StartIndex, cellOffset.EndIndex, numBytesToRead, numBytesActuallyRead)
		}

		posInBuffer += numBytesToRead
	}

	return buffer
}

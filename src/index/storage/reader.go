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

func NewFeatureStorageReader(baseFolder string, filename string) *FeatureStorageReader {
	metadataFileName := baseFolder + "/metadata.json"

	metadataFileContent, err := os.ReadFile(metadataFileName)
	sigolo.FatalCheck(errors.Wrapf(err, "Unable to read metadata file %s", metadataFileName))

	metadata := &indexMetadata{}
	err = json.Unmarshal(metadataFileContent, metadata)
	sigolo.FatalCheck(errors.Wrapf(err, "Unable to unmarshal content of metadata file %s", metadataFileName))

	var file *os.File
	indexFileName := baseFolder + "/" + filename
	file, err = os.OpenFile(indexFileName, os.O_RDONLY, 0666)
	sigolo.FatalCheck(errors.Wrapf(err, "Unable to open index file %s", indexFileName))

	return &FeatureStorageReader{
		indexFile:       file,
		indexFileReader: bufio.NewReader(file),
		indexMetadata:   metadata,
	}
}

func (r FeatureStorageReader) ReadRawDataWithParentIds(cellExtent common.CellExtent) (
	[]*indexCommon.RawEncodedNodeFeature,
	[]*indexCommon.RawEncodedWayFeature,
	[]*indexCommon.RawEncodedRelationFeature,
) {
	nodes := r.readRawNodes(cellExtent)

	ways, nodeToWayMapping := r.readRawWays(cellExtent)
	for _, node := range nodes {
		node.SetWayIds(nodeToWayMapping[osm.NodeID(node.GetID())])
	}

	relations, nodeToRelationMapping, wayToRelationMapping, relationToRelationMapping := r.readRelations(cellExtent)
	for _, node := range nodes {
		node.SetRelationIds(nodeToRelationMapping[osm.NodeID(node.GetID())])
	}
	for _, way := range ways {
		way.SetRelationIds(wayToRelationMapping[osm.WayID(way.GetID())])
	}
	for _, relation := range relations {
		relation.SetParentRelationIds(relationToRelationMapping[osm.RelationID(relation.GetID())])
	}

	return nodes, ways, relations
}

func (r FeatureStorageReader) readRawNodes(cellExtent common.CellExtent) []*indexCommon.RawEncodedNodeFeature {
	features := []*indexCommon.RawEncodedNodeFeature{}

	cellOffsets := r.indexMetadata.getCellMetadata(cellExtent).NodeOffsets
	data := r.read(cellOffsets)

	// Storage format see FeatureStorageWriter::writeNodeData
	for pos := 0; pos < len(data); {
		id := int64(binary.LittleEndian.Uint64(data[pos:]))

		numWayIds := int(binary.LittleEndian.Uint16(data[pos+20:]))
		if numWayIds != 0 {
			sigolo.Fatalf("Expected number of ways on raw node %d to be 0 but was %d", id, numWayIds)
		}

		numRelationIds := int(binary.LittleEndian.Uint16(data[pos+22:]))
		if numRelationIds != 0 {
			sigolo.Fatalf("Expected number of relations on raw node %d (post=%d) to be 0 but was %d", id, pos, numRelationIds)
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

		pos += numberOfBytes
	}

	return features
}

func (r FeatureStorageReader) readRawWays(cellExtent common.CellExtent) ([]*indexCommon.RawEncodedWayFeature, map[osm.NodeID][]osm.WayID) {
	features := []*indexCommon.RawEncodedWayFeature{}
	nodeToWayMapping := map[osm.NodeID][]osm.WayID{}

	cellOffsets := r.indexMetadata.getCellMetadata(cellExtent).WayOffsets
	data := r.read(cellOffsets)

	// Storage format see FeatureStorageWriter::writeWayData
	for pos := 0; pos < len(data); {
		id := osm.WayID(binary.LittleEndian.Uint64(data[pos:]))

		numRelationIds := int(binary.LittleEndian.Uint16(data[pos+14:]))
		if numRelationIds != 0 {
			sigolo.Fatalf("Expected number of relations on raw way %d (post=%d) to be 0 but was %d", id, pos, numRelationIds)
		}

		numEncodedKeyBytes := int(binary.LittleEndian.Uint16(data[pos+8:]))
		numValues := int(binary.LittleEndian.Uint16(data[pos+10:]))
		numNodeIds := int(binary.LittleEndian.Uint16(data[pos+12:]))

		numHeaderBytes := 8 + 2 + 2 + 2 + 2 + numEncodedKeyBytes + numValues*3
		numberOfBytes := numHeaderBytes + numNodeIds*16

		for i := 0; i < numNodeIds; i++ {
			nodeId := osm.NodeID(binary.LittleEndian.Uint64(data[pos+numHeaderBytes+i*16:]))
			nodeToWayMapping[nodeId] = append(nodeToWayMapping[nodeId], id)
		}

		rawEncodedWay := &indexCommon.RawEncodedWayFeature{
			Data:        data[pos : pos+numberOfBytes],
			RelationIds: make([]osm.RelationID, 0),
		}
		features = append(features, rawEncodedWay)

		pos += numberOfBytes
	}

	return features, nodeToWayMapping
}

func (r FeatureStorageReader) readRelations(cellExtent common.CellExtent) ([]*indexCommon.RawEncodedRelationFeature, map[osm.NodeID][]osm.RelationID, map[osm.WayID][]osm.RelationID, map[osm.RelationID][]osm.RelationID) {
	features := []*indexCommon.RawEncodedRelationFeature{}
	nodeToRelationMapping := map[osm.NodeID][]osm.RelationID{}
	wayToRelationMapping := map[osm.WayID][]osm.RelationID{}
	relationToRelationMapping := map[osm.RelationID][]osm.RelationID{}

	cellOffsets := r.indexMetadata.getCellMetadata(cellExtent).RelationOffsets
	data := r.read(cellOffsets)

	// Storage format see FeatureStorageWriter::writeRelationData
	for pos := 0; pos < len(data); {
		id := osm.RelationID(binary.LittleEndian.Uint64(data[pos:]))

		numParentRelationIds := int(binary.LittleEndian.Uint16(data[pos+34:]))
		if numParentRelationIds != 0 {
			sigolo.Fatalf("Expected number of parent-relations on raw relation %d to be 0 but was %d", id, numParentRelationIds)
		}

		numEncodedKeyBytes := int(binary.LittleEndian.Uint16(data[pos+24:]))
		numValues := int(binary.LittleEndian.Uint16(data[pos+26:]))
		numNodeIds := int(binary.LittleEndian.Uint16(data[pos+28:]))
		numWayIds := int(binary.LittleEndian.Uint16(data[pos+30:]))
		numChildRelationIds := int(binary.LittleEndian.Uint16(data[pos+32:]))

		numHeaderBytes := 8 + 16 + 2 + 2 + 2 + 2 + 2 + 2 + numEncodedKeyBytes + numValues*3
		numberOfBytes := numHeaderBytes + numNodeIds*8 + numWayIds*8 + numChildRelationIds*8

		for i := 0; i < numNodeIds; i++ {
			nodeId := osm.NodeID(binary.LittleEndian.Uint64(data[pos+numHeaderBytes+i*8:]))
			nodeToRelationMapping[nodeId] = append(nodeToRelationMapping[nodeId], id)
		}
		for i := 0; i < numWayIds; i++ {
			wayId := osm.WayID(binary.LittleEndian.Uint64(data[pos+numHeaderBytes+numNodeIds*8+i*8:]))
			wayToRelationMapping[wayId] = append(wayToRelationMapping[wayId], id)
		}
		for i := 0; i < numChildRelationIds; i++ {
			relationId := osm.RelationID(binary.LittleEndian.Uint64(data[pos+numHeaderBytes+numNodeIds*8+numWayIds*8+i*8:]))
			relationToRelationMapping[relationId] = append(relationToRelationMapping[relationId], id)
		}

		rawEncodedRelation := &indexCommon.RawEncodedRelationFeature{
			Data:              data[pos : pos+numberOfBytes],
			ParentRelationIds: make([]osm.RelationID, 0),
		}
		features = append(features, rawEncodedRelation)

		pos += numberOfBytes
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

		numBytesActuallyRead, err := r.indexFile.ReadAt(buffer[posInBuffer:posInBuffer+numBytesToRead], cellOffset.StartIndex)
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to read bytes %d to %d from index file", cellOffset.StartIndex, cellOffset.EndIndex))
		if int64(numBytesActuallyRead) != numBytesToRead {
			sigolo.Fatalf("Error reading bytes %d to %d from index file. Expected to read %d bytes but actually read %d bytes.", cellOffset.StartIndex, cellOffset.EndIndex, numBytesToRead, numBytesActuallyRead)
		}

		posInBuffer += numBytesToRead
	}

	return buffer
}

package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"soq/common"
	"soq/feature"
	indexCommon "soq/index/common"
	ownOsm "soq/osm"

	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"github.com/pkg/errors"
)

type FeatureStorageReader struct {
	indexFile       *os.File
	indexFileReader *bufio.Reader
	indexMetadata   *indexMetadata
	cellCache       featureCache
}

// TODO return error
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
		cellCache:       newLruCache(10), // TODO make this max-size parameter configurable
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

	relations, nodeToRelationMapping, wayToRelationMapping, relationToRelationMapping := r.readRawRelations(common.CellExtent{common.CellIndex{math.MinInt32, math.MinInt32}, common.CellIndex{math.MinInt32, math.MinInt32}})
	relationMap := map[uint64]*indexCommon.RawEncodedRelationFeature{}

	// Map to store the bound of the relation within this cellExtent. The relation might be larger but currently we only
	// consider data within this cell extent.
	relationMinXMap := map[osm.RelationID]float32{}
	relationMaxXMap := map[osm.RelationID]float32{}
	relationMinYMap := map[osm.RelationID]float32{}
	relationMaxYMap := map[osm.RelationID]float32{}
	for _, relation := range relations {
		relationMap[relation.GetID()] = relation
	}

	for _, node := range nodes {
		relationIDsOfNode := nodeToRelationMapping[osm.NodeID(node.GetID())]
		node.SetRelationIds(relationIDsOfNode)
		for _, relationId := range relationIDsOfNode {
			lon := float32(node.GetLon())
			lat := float32(node.GetLat())
			if val, ok := relationMinXMap[relationId]; !ok || val > lon {
				relationMinXMap[relationId] = lon
			}
			if val, ok := relationMaxXMap[relationId]; !ok || val < lon {
				relationMaxXMap[relationId] = lon
			}
			if val, ok := relationMinYMap[relationId]; !ok || val > lat {
				relationMinYMap[relationId] = lat
			}
			if val, ok := relationMaxYMap[relationId]; !ok || val < lat {
				relationMaxYMap[relationId] = lat
			}
		}
	}
	for _, way := range ways {
		relationIDsOfWay := wayToRelationMapping[osm.WayID(way.GetID())]
		way.SetRelationIds(relationIDsOfWay)
		longitudes, latitudes := way.GetNodeCoordinates()
		for i, _ := range longitudes {
			for _, relationId := range relationIDsOfWay {
				lon := longitudes[i]
				lat := latitudes[i]
				if val, ok := relationMinXMap[relationId]; !ok || val > lon {
					relationMinXMap[relationId] = lon
				}
				if val, ok := relationMaxXMap[relationId]; !ok || val < lon {
					relationMaxXMap[relationId] = lon
				}
				if val, ok := relationMinYMap[relationId]; !ok || val > lat {
					relationMinYMap[relationId] = lat
				}
				if val, ok := relationMaxYMap[relationId]; !ok || val < lat {
					relationMaxYMap[relationId] = lat
				}
			}
		}
	}
	for _, relation := range relations {
		relationId := osm.RelationID(relation.GetID())
		relation.SetParentRelationIds(relationToRelationMapping[relationId])
		if _, relationHasCoordinates := relationMinXMap[relationId]; relationHasCoordinates {
			relation.SetBounds(orb.Bound{
				Min: orb.Point{float64(relationMinXMap[relationId]), float64(relationMinYMap[relationId])},
				Max: orb.Point{float64(relationMaxXMap[relationId]), float64(relationMaxYMap[relationId])},
			})
		}
	}

	// TODO filter relations by their bounds so that only the ones in this extent are returned. Relations had no bound before. Also consider reading them once and then holding them in memory.

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

func (r FeatureStorageReader) ReadNodes(cellExtent common.CellExtent) ([]feature.Feature, error) {
	result := []feature.Feature{}

	cellMetadata := r.indexMetadata.getCellMetadata(cellExtent)
	if cellMetadata == nil {
		// No data in this cell
		return result, nil
	}

	cachedFeatures, entryIsNew, err := r.cellCache.getOrInsert(cellMetadata.Extent, ownOsm.OsmObjNode)
	if err != nil {
		return nil, err
	}
	// Ignore new and empty caches. Empty caches might not be actually empty but not yet filled. This might happen when
	// the same cell file is read by multiple goroutines at the same time.
	if !entryIsNew && len(cachedFeatures) > 0 {
		sigolo.Tracef("Use features from cache for cell extent %v", cellExtent)
		return cachedFeatures, nil
	}

	cellOffsets := cellMetadata.NodeOffsets
	data := r.read(cellOffsets)

	// Storage format see FeatureStorageWriter::writeNodeData
	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeNodeData".

		/*
			Read header fields
		*/
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		lon := math.Float32frombits(binary.LittleEndian.Uint32(data[pos+8:]))
		lat := math.Float32frombits(binary.LittleEndian.Uint32(data[pos+12:]))
		numEncodedKeyBytes := int(binary.LittleEndian.Uint16(data[pos+16:]))
		numValues := int(binary.LittleEndian.Uint16(data[pos+18:]))
		numWayIds := int(binary.LittleEndian.Uint16(data[pos+20:]))
		numRelationIds := int(binary.LittleEndian.Uint16(data[pos+22:]))

		headerBytesCount := 8 + 4 + 4 + 2 + 2 + 2 + 2 // = 24

		sigolo.Tracef("Read feature pos=%d, id=%d, lon=%f, lat=%f, numKeys=%d, numValues=%d", pos, osmId, lon, lat, numEncodedKeyBytes, numValues)

		pos += headerBytesCount

		/*
			Read keys
		*/
		encodedKeys := make([]byte, numEncodedKeyBytes)
		encodedValues := make([]int, numValues)
		copy(encodedKeys[:], data[pos:])
		pos += numEncodedKeyBytes

		/*
			Read values
		*/
		for i := 0; i < numValues; i++ {
			encodedValues[i] = int(uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16)
			pos += 3
		}

		/*
			Read way-IDs
		*/
		wayIds := make([]osm.WayID, numWayIds)
		for i := 0; i < numWayIds; i++ {
			wayIds[i] = osm.WayID(binary.LittleEndian.Uint64(data[pos:]))
			pos += 8
		}

		/*
			Read relation-IDs
		*/
		relationIds := make([]osm.RelationID, numRelationIds)
		for i := 0; i < numRelationIds; i++ {
			relationIds[i] = osm.RelationID(binary.LittleEndian.Uint64(data[pos:]))
			pos += 8
		}

		/*
			Create encoded feature from raw data
		*/
		encodedFeature := &indexCommon.EncodedNodeFeature{
			AbstractEncodedFeature: indexCommon.AbstractEncodedFeature{
				ID:       osmId,
				Geometry: &orb.Point{float64(lon), float64(lat)},
				Keys:     encodedKeys,
				Values:   encodedValues,
			},
			WayIds:      wayIds,
			RelationIds: relationIds,
		}

		// TODO
		//if g.checkFeatureValidity {
		//	sigolo.Debugf("Check validity of feature %d", encodedFeature.ID)
		//	g.checkValidity(encodedFeature)
		//}

		result = append(result, encodedFeature)
	}

	r.cellCache.insertOrAppend(cellMetadata.Extent, ownOsm.OsmObjNode, result)

	return result, nil
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

func (r FeatureStorageReader) ReadWays(cellExtent common.CellExtent) ([]feature.Feature, error) {
	result := []feature.Feature{}

	cellMetadata := r.indexMetadata.getCellMetadata(cellExtent)
	if cellMetadata == nil {
		// No data in this cell
		return result, nil
	}

	cachedFeatures, entryIsNew, err := r.cellCache.getOrInsert(cellMetadata.Extent, ownOsm.OsmObjWay)
	if err != nil {
		return nil, err
	}
	// Ignore new and empty caches. Empty caches might not be actually empty but not yet filled. This might happen when
	// the same cell file is read by multiple goroutines at the same time.
	if !entryIsNew && len(cachedFeatures) > 0 {
		sigolo.Tracef("Use features from cache for cell extent %v", cellExtent)
		return cachedFeatures, nil
	}

	cellOffsets := cellMetadata.WayOffsets
	data := r.read(cellOffsets)

	// Storage format see FeatureStorageWriter::writeWayData
	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeWayData".

		/*
			Read header fields
		*/
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		numEncodedKeyBytes := int(binary.LittleEndian.Uint16(data[pos+8:]))
		numValues := int(binary.LittleEndian.Uint16(data[pos+10:]))
		numNodes := int(binary.LittleEndian.Uint16(data[pos+12:]))
		numRelationIds := int(binary.LittleEndian.Uint16(data[pos+14:]))

		headerBytesCount := 8 + 2 + 2 + 2 + 2

		sigolo.Tracef("Read feature pos=%d, id=%d, numKeys=%d, numValues=%d", pos, osmId, numEncodedKeyBytes, numValues)

		pos += headerBytesCount

		/*
			Read keys
		*/
		encodedKeys := make([]byte, numEncodedKeyBytes)
		encodedValues := make([]int, numValues)
		copy(encodedKeys[:], data[pos:])
		pos += numEncodedKeyBytes

		/*
			Read values
		*/
		for i := 0; i < numValues; i++ {
			encodedValues[i] = int(uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16)
			pos += 3
		}

		/*
			Read node-IDs
		*/
		nodes := make([]osm.WayNode, numNodes)
		for i := 0; i < numNodes; i++ {
			nodes[i] = osm.WayNode{
				ID:  osm.NodeID(binary.LittleEndian.Uint64(data[pos:])),
				Lon: float64(math.Float32frombits(binary.LittleEndian.Uint32(data[(pos + 8):]))),
				Lat: float64(math.Float32frombits(binary.LittleEndian.Uint32(data[(pos + 12):]))),
			}
			pos += 16
		}

		/*
			Read relation-IDs
		*/
		var relationIds []osm.RelationID
		for i := 0; i < numRelationIds; i++ {
			relationIds = append(relationIds, osm.RelationID(binary.LittleEndian.Uint64(data[pos:])))
			pos += 8
		}

		/*
			Create encoded feature from raw data
		*/
		lineString := make(orb.LineString, len(nodes))
		for i, node := range nodes {
			lineString[i] = orb.Point{node.Lon, node.Lat}
		}

		encodedFeature := &indexCommon.EncodedWayFeature{
			AbstractEncodedFeature: indexCommon.AbstractEncodedFeature{
				ID:       osmId,
				Keys:     encodedKeys,
				Values:   encodedValues,
				Geometry: &lineString,
			},
			Nodes:       nodes,
			RelationIds: relationIds,
		}

		// TODO
		//if g.checkFeatureValidity {
		//	sigolo.Debugf("Check validity of feature %d", encodedFeature.ID)
		//	g.checkValidity(&encodedFeature)
		//}

		result = append(result, encodedFeature)
	}

	r.cellCache.insertOrAppend(cellMetadata.Extent, ownOsm.OsmObjWay, result)

	return result, nil
}

func (r FeatureStorageReader) readRawRelations(cellExtent common.CellExtent) ([]*indexCommon.RawEncodedRelationFeature, map[osm.NodeID][]osm.RelationID, map[osm.WayID][]osm.RelationID, map[osm.RelationID][]osm.RelationID) {
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

func (r FeatureStorageReader) ReadRelations(cellExtent common.CellExtent) ([]feature.Feature, error) {
	result := []feature.Feature{}

	cellMetadata := r.indexMetadata.getCellMetadata(cellExtent)
	if cellMetadata == nil {
		// No data in this cell
		return result, nil
	}

	cachedFeatures, entryIsNew, err := r.cellCache.getOrInsert(cellMetadata.Extent, ownOsm.OsmObjRelation)
	if err != nil {
		return nil, err
	}
	// Ignore new and empty caches. Empty caches might not be actually empty but not yet filled. This might happen when
	// the same cell file is read by multiple goroutines at the same time.
	if !entryIsNew && len(cachedFeatures) > 0 {
		sigolo.Tracef("Use features from cache for cell extent %v", cellExtent)
		return cachedFeatures, nil
	}

	cellOffsets := cellMetadata.RelationOffsets
	data := r.read(cellOffsets)

	// Storage format see FeatureStorageWriter::writeRelationData
	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeRelationData".

		/*
			Read header fields
		*/
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		minLon := math.Float32frombits(binary.LittleEndian.Uint32(data[pos+8:]))
		minLat := math.Float32frombits(binary.LittleEndian.Uint32(data[pos+12:]))
		maxLon := math.Float32frombits(binary.LittleEndian.Uint32(data[pos+16:]))
		maxLat := math.Float32frombits(binary.LittleEndian.Uint32(data[pos+20:]))
		numEncodedKeyBytes := int(binary.LittleEndian.Uint16(data[pos+24:]))
		numValues := int(binary.LittleEndian.Uint16(data[pos+26:]))
		numNodeIds := int(binary.LittleEndian.Uint16(data[pos+28:]))
		numWayIds := int(binary.LittleEndian.Uint16(data[pos+30:]))
		numChildRelationIds := int(binary.LittleEndian.Uint16(data[pos+32:]))
		numParentRelationIds := int(binary.LittleEndian.Uint16(data[pos+34:]))

		bbox := orb.Bound{
			Min: orb.Point{float64(minLon), float64(minLat)},
			Max: orb.Point{float64(maxLon), float64(maxLat)},
		}

		headerBytesCount := 8 + 16 + 2 + 2 + 2 + 2 + 2 + 2 // = 36

		sigolo.Tracef("Read feature pos=%d, id=%d, bbox=%v, numKeys=%d, numValues=%d", pos, osmId, bbox, numEncodedKeyBytes, numValues)

		pos += headerBytesCount

		/*
			Read keys
		*/
		encodedKeys := make([]byte, numEncodedKeyBytes)
		encodedValues := make([]int, numValues)
		copy(encodedKeys[:], data[pos:])
		pos += numEncodedKeyBytes

		/*
			Read values
		*/
		for i := 0; i < numValues; i++ {
			encodedValues[i] = int(uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16)
			pos += 3
		}

		/*
			Read node-IDs
		*/
		nodeIds := make([]osm.NodeID, numNodeIds)
		for i := 0; i < numNodeIds; i++ {
			nodeIds[i] = osm.NodeID(binary.LittleEndian.Uint64(data[pos:]))
			pos += 8
		}

		/*
			Read way-IDs
		*/
		wayIds := make([]osm.WayID, numWayIds)
		for i := 0; i < numWayIds; i++ {
			wayIds[i] = osm.WayID(binary.LittleEndian.Uint64(data[pos:]))
			pos += 8
		}

		/*
			Read child relation-IDs
		*/
		childRelationIds := make([]osm.RelationID, numChildRelationIds)
		for i := 0; i < numChildRelationIds; i++ {
			childRelationIds[i] = osm.RelationID(binary.LittleEndian.Uint64(data[pos:]))
			pos += 8
		}

		/*
			Read relation-IDs
		*/
		parentRelationIds := make([]osm.RelationID, numParentRelationIds)
		for i := 0; i < numParentRelationIds; i++ {
			parentRelationIds[i] = osm.RelationID(binary.LittleEndian.Uint64(data[pos:]))
			pos += 8
		}

		/*
			Create encoded feature from raw data
		*/
		bboxPolygon := bbox.ToPolygon()
		encodedFeature := &indexCommon.EncodedRelationFeature{
			AbstractEncodedFeature: indexCommon.AbstractEncodedFeature{
				ID:       osmId,
				Geometry: &bboxPolygon, // This is probably temporary until the real geometry collection is stored
				Keys:     encodedKeys,
				Values:   encodedValues,
			},
			NodeIds:           nodeIds,
			WayIds:            wayIds,
			ChildRelationIds:  childRelationIds,
			ParentRelationIds: parentRelationIds,
		}

		// TODO
		//if g.checkFeatureValidity {
		//	sigolo.Debugf("Check validity of feature %d", encodedFeature.ID)
		//	g.checkValidity(encodedFeature)
		//}

		result = append(result, encodedFeature)
	}

	r.cellCache.insertOrAppend(cellMetadata.Extent, ownOsm.OsmObjRelation, result)

	return result, nil
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

func (r FeatureStorageReader) GetExtentsForCells(cells []common.CellIndex) []common.CellExtent {
	result := []common.CellExtent{}

	for _, cellMetadata := range r.indexMetadata.Cells {
		for _, cell := range cells {
			if cellMetadata.Extent.Contains(cell) {
				result = append(result, cellMetadata.Extent)
				break
			}
		}
	}

	return result
}

func (r FeatureStorageReader) GetExtentsForCellBounds(bounds common.CellExtent) []common.CellExtent {
	result := []common.CellExtent{}

	for _, cellMetadata := range r.indexMetadata.Cells {
		if cellMetadata.Extent.Intersects(bounds) {
			result = append(result, cellMetadata.Extent)
		}
	}

	return result
}

package index

import (
	"encoding/binary"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"github.com/pkg/errors"
	"math"
	"os"
	"path"
	"soq/feature"
	"strconv"
	"sync"
)

type GridIndexReader struct {
	baseGridIndex

	checkFeatureValidity bool
	cellCache            featureCache
}

func LoadGridIndex(indexBaseFolder string, cellWidth float64, cellHeight float64, checkFeatureValidity bool, tagIndex *TagIndex) *GridIndexReader {
	return &GridIndexReader{
		baseGridIndex: baseGridIndex{
			TagIndex:   tagIndex,
			CellWidth:  cellWidth,
			CellHeight: cellHeight,
			BaseFolder: path.Join(indexBaseFolder, GridIndexFolder),
		},
		checkFeatureValidity: checkFeatureValidity,
		cellCache:            newLruCache(10), // TODO make this max-size parameter configurable
	}
}

func (g *GridIndexReader) Get(bbox *orb.Bound, objectType feature.OsmObjectType) (chan *GetFeaturesResult, error) {
	sigolo.Debugf("Get feature from bbox=%#v", bbox)
	minCell := g.GetCellIndexForCoordinate(bbox.Min.Lon(), bbox.Min.Lat())
	maxCell := g.GetCellIndexForCoordinate(bbox.Max.Lon(), bbox.Max.Lat())

	resultChannel := make(chan *GetFeaturesResult)

	go func() {
		numThreads := 3

		// Group the cells into columns of equal size so that each goroutine below can handle on column.
		cellColumns := maxCell.X() - minCell.X() + 1 // min and max are inclusive, therefore +1
		if cellColumns < numThreads {
			// To prevent that two threads are fetching the same columns
			numThreads = cellColumns
		}
		threadColumns := cellColumns / numThreads

		var wg sync.WaitGroup
		wg.Add(numThreads)

		for i := 0; i < numThreads; i++ {
			minColX := minCell.X() + i*threadColumns
			maxColX := minCell.X() + (i+1)*threadColumns - 1 // -1 to prevent overlapping columns
			if i == numThreads-1 {
				// Last column: Make sure it goes til the requested end
				maxColX = maxCell.X()
			}

			go g.getFeaturesForCellsWithBbox(resultChannel, &wg, bbox, minColX, maxColX, minCell.Y(), maxCell.Y(), objectType)
		}

		wg.Wait()
		close(resultChannel)

		sigolo.Debugf("Done reading %s features for area minCell=%v to maxCell=%v", objectType, minCell, maxCell)
	}()

	return resultChannel, nil // Remove error from return, since it doesn't make any sense here
}

func (g *GridIndexReader) GetNodes(nodes osm.WayNodes) (chan *GetFeaturesResult, error) {
	cells := map[CellIndex][]uint64{}            // just a lookup table to quickly see if a cell has already been collected
	innerCellBounds := map[CellIndex]orb.Bound{} // just a lookup table to quickly see if a cell has already been collected
	for _, node := range nodes {
		cell := g.GetCellIndexForCoordinate(node.Lon, node.Lat)
		if _, ok := cells[cell]; !ok {
			// New cell -> Create it and add first node ID
			cells[cell] = []uint64{uint64(node.ID)}
			innerCellBounds[cell] = node.Point().Bound()
		} else {
			// Cell has been seen before -> just add the new node ID
			cells[cell] = append(cells[cell], uint64(node.ID))
			innerCellBounds[cell] = innerCellBounds[cell].Union(node.Point().Bound())
		}
	}

	var resultChannel = make(chan *GetFeaturesResult, 10)

	if len(cells) == 0 {
		close(resultChannel)
		return resultChannel, nil
	}

	go func() {
		for cell, nodeIds := range cells {
			innerCellBound := innerCellBounds[cell]
			outputBuffer := []feature.EncodedFeature{}

			unfilteredFeatures, err := g.readFeaturesFromCellFile(cell[0], cell[1], feature.OsmObjNode)
			sigolo.FatalCheck(err)

			for i := 0; i < len(unfilteredFeatures); i++ {
				encodedFeature := unfilteredFeatures[i]
				if encodedFeature != nil {
					// TODO Getting the bound, geometry and filtering takes quite long. Try to optimize this (maybe with a "encodedFeature.IsWithin()" func?)
					featureBound := encodedFeature.GetGeometry().Bound()
					notWithinCellContent := (innerCellBound.Max[0] < featureBound.Min[0]) ||
						(innerCellBound.Min[0] > featureBound.Max[0]) ||
						(innerCellBound.Max[1] < featureBound.Min[1]) ||
						(innerCellBound.Min[1] > featureBound.Max[1])
					if notWithinCellContent {
						continue
					}

					for j := 0; j < len(nodeIds); j++ {
						if encodedFeature.GetID() == nodeIds[j] {
							outputBuffer = append(outputBuffer, encodedFeature)
							break
						}
					}
				}
			}

			resultChannel <- &GetFeaturesResult{
				Cell:     cell,
				Features: outputBuffer,
			}
		}
		close(resultChannel)
	}()

	return resultChannel, nil
}

func (g *GridIndexReader) GetFeaturesForCells(cells []CellIndex, objectType feature.OsmObjectType) chan *GetFeaturesResult {
	resultChannel := make(chan *GetFeaturesResult)

	go func() {
		for _, cell := range cells {
			featuresInCell := &GetFeaturesResult{
				Cell:     cell,
				Features: []feature.EncodedFeature{},
			}

			encodedFeatures, err := g.readFeaturesFromCellFile(cell[0], cell[1], objectType)
			sigolo.FatalCheck(err)
			featuresInCell.Features = encodedFeatures

			resultChannel <- featuresInCell
		}
		close(resultChannel)
	}()

	return resultChannel
}

func (g *GridIndexReader) getFeaturesForCellsWithBbox(output chan *GetFeaturesResult, wg *sync.WaitGroup, bbox *orb.Bound, minCellX int, maxCellX int, minCellY int, maxCellY int, objectType feature.OsmObjectType) {
	sigolo.Debugf("Get %s features for cells minX=%d, maxX=%d / minY=%d, maxY=%d", objectType.String(), minCellX, maxCellX, minCellY, maxCellY)
	for cellX := minCellX; cellX <= maxCellX; cellX++ {
		for cellY := minCellY; cellY <= maxCellY; cellY++ {
			sigolo.Debugf("Get %s features for cell X=%d, Y=%d", objectType.String(), minCellX, maxCellX)

			featuresInBbox := &GetFeaturesResult{
				Cell:     CellIndex{cellX, cellY},
				Features: []feature.EncodedFeature{},
			}

			encodedFeatures, err := g.readFeaturesFromCellFile(cellX, cellY, objectType)
			sigolo.FatalCheck(err)

			for i := 0; i < len(encodedFeatures); i++ {
				if encodedFeatures[i] != nil && bbox.Intersects(encodedFeatures[i].GetGeometry().Bound()) {
					featuresInBbox.Features = append(featuresInBbox.Features, encodedFeatures[i])
				}
			}

			output <- featuresInBbox
		}
	}
	wg.Done()
	sigolo.Debugf("Finished getting %s features for cells minX=%d, maxX=%d / minY=%d, maxY=%d", objectType, minCellX, maxCellX, minCellY, maxCellY)
}

// readFeaturesFromCellFile reads all features from the specified cell and writes them periodically to the output channel.
func (g *GridIndexReader) readFeaturesFromCellFile(cellX int, cellY int, objectType feature.OsmObjectType) ([]feature.EncodedFeature, error) {
	cellFolderName := path.Join(g.BaseFolder, objectType.String(), strconv.Itoa(cellX))
	cellFileName := path.Join(cellFolderName, strconv.Itoa(cellY)+".cell")

	if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
		sigolo.Tracef("Cell file %s does not exist, I'll return an empty feature list", cellFileName)
		return nil, nil
	} else if err != nil {
		return nil, errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	cachedFeatures, entryIsNew, err := g.cellCache.getOrInsert(cellFileName)
	// Ignore new and empty caches. Empty caches might not be actually empty but not yet filled. This might happen when
	// the same cell file is read by multiple goroutines at the same time.
	if !entryIsNew && len(cachedFeatures) > 0 {
		sigolo.Tracef("Use features from cache for cell file %s", cellFileName)
		return cachedFeatures, nil
	}

	sigolo.Tracef("Read cell file %s", cellFileName)
	data, err := os.ReadFile(cellFileName)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, objectType)
	}

	readFeatureChannel := make(chan []feature.EncodedFeature)
	featureCachedWaitGroup := &sync.WaitGroup{}
	featureCachedWaitGroup.Add(1)
	go func() {
		for readFeatures := range readFeatureChannel {
			// TODO not-null check needed for the features?
			cachedFeatures = append(cachedFeatures, readFeatures...)
		}
		featureCachedWaitGroup.Done()
	}()

	switch objectType {
	case feature.OsmObjNode:
		g.readNodesFromCellData(readFeatureChannel, data)
	case feature.OsmObjWay:
		g.readWaysFromCellData(readFeatureChannel, data)
	case feature.OsmObjRelation:
		g.readRelationsFromCellData(readFeatureChannel, data)
	default:
		panic("Unsupported object type to read: " + objectType.String())
	}

	close(readFeatureChannel)
	featureCachedWaitGroup.Wait()

	g.cellCache.insertOrAppend(cellFileName, cachedFeatures)

	return cachedFeatures, nil
}

func (g *GridIndexReader) readNodesFromCellData(output chan []feature.EncodedFeature, data []byte) {
	outputBuffer := make([]feature.EncodedFeature, 1000)
	currentBufferPos := 0

	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeNodeData".

		/*
			Read header fields
		*/
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		lon := math.Float32frombits(binary.LittleEndian.Uint32(data[pos+8:]))
		lat := math.Float32frombits(binary.LittleEndian.Uint32(data[pos+12:]))
		numEncodedKeyBytes := int(binary.LittleEndian.Uint32(data[pos+16:]))
		numValues := int(binary.LittleEndian.Uint32(data[pos+20:]))
		numWayIds := int(binary.LittleEndian.Uint16(data[pos+24:]))
		numRelationIds := int(binary.LittleEndian.Uint16(data[pos+26:]))

		headerBytesCount := 8 + 4 + 4 + 4 + 4 + 2 + 2 // = 28

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
		encodedFeature := &feature.EncodedNodeFeature{
			AbstractEncodedFeature: feature.AbstractEncodedFeature{
				ID:       osmId,
				Geometry: &orb.Point{float64(lon), float64(lat)},
				Keys:     encodedKeys,
				Values:   encodedValues,
			},
			WayIds:      wayIds,
			RelationIds: relationIds,
		}
		if g.checkFeatureValidity {
			sigolo.Debugf("Check validity of feature %d", encodedFeature.ID)
			g.checkValidity(encodedFeature)
		}

		outputBuffer[currentBufferPos] = encodedFeature
		currentBufferPos++

		if currentBufferPos == len(outputBuffer)-1 {
			output <- outputBuffer
			outputBuffer = make([]feature.EncodedFeature, len(outputBuffer))
			currentBufferPos = 0
		}
	}

	output <- outputBuffer
}

func (g *GridIndexReader) readWaysFromCellData(output chan []feature.EncodedFeature, data []byte) {
	outputBuffer := make([]feature.EncodedFeature, 1000)
	currentBufferPos := 0
	totalReadFeatures := 0

	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeWayData".

		/*
			Read header fields
		*/
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		numEncodedKeyBytes := int(binary.LittleEndian.Uint32(data[pos+8:]))
		numValues := int(binary.LittleEndian.Uint32(data[pos+12:]))
		numNodes := int(binary.LittleEndian.Uint16(data[pos+16:]))
		numRelationIDs := int(binary.LittleEndian.Uint16(data[pos+18:]))

		headerBytesCount := 8 + 4 + 4 + 2 + 2

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
		var nodes osm.WayNodes
		for i := 0; i < numNodes; i++ {
			nodes = append(nodes, osm.WayNode{
				ID:  osm.NodeID(binary.LittleEndian.Uint64(data[pos:])),
				Lon: float64(math.Float32frombits(binary.LittleEndian.Uint32(data[(pos + 8):]))),
				Lat: float64(math.Float32frombits(binary.LittleEndian.Uint32(data[(pos + 12):]))),
			})
			pos += 16
		}

		/*
			Read relation-IDs
		*/
		var relationIds []osm.RelationID
		for i := 0; i < numRelationIDs; i++ {
			relationIds = append(relationIds, osm.RelationID(binary.LittleEndian.Uint64(data[pos:])))
			pos += 8
		}

		/*
			Create encoded feature from raw data
		*/
		var lineString orb.LineString
		for _, node := range nodes {
			lineString = append(lineString, orb.Point{node.Lon, node.Lat})
		}
		encodedFeature := feature.EncodedWayFeature{
			AbstractEncodedFeature: feature.AbstractEncodedFeature{
				ID:       osmId,
				Keys:     encodedKeys,
				Values:   encodedValues,
				Geometry: &lineString,
			},
			Nodes:       nodes,
			RelationIds: relationIds,
		}
		if g.checkFeatureValidity {
			sigolo.Debugf("Check validity of feature %d", encodedFeature.ID)
			g.checkValidity(&encodedFeature)
		}

		outputBuffer[currentBufferPos] = &encodedFeature
		currentBufferPos++

		if currentBufferPos == len(outputBuffer)-1 {
			output <- outputBuffer
			outputBuffer = make([]feature.EncodedFeature, len(outputBuffer))
			currentBufferPos = 0
		}

		totalReadFeatures++
	}

	output <- outputBuffer
}

func (g *GridIndexReader) readRelationsFromCellData(output chan []feature.EncodedFeature, data []byte) {
	outputBuffer := make([]feature.EncodedFeature, 1000)
	currentBufferPos := 0

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
		numEncodedKeyBytes := int(binary.LittleEndian.Uint32(data[pos+24:]))
		numValues := int(binary.LittleEndian.Uint32(data[pos+28:]))
		numNodeIds := int(binary.LittleEndian.Uint16(data[pos+32:]))
		numWayIds := int(binary.LittleEndian.Uint16(data[pos+34:]))
		numChildRelationIds := int(binary.LittleEndian.Uint16(data[pos+36:]))
		numParentRelationIds := int(binary.LittleEndian.Uint16(data[pos+38:]))

		bbox := orb.Bound{
			Min: orb.Point{float64(minLon), float64(minLat)},
			Max: orb.Point{float64(maxLon), float64(maxLat)},
		}

		headerBytesCount := 8 + 16 + 4 + 4 + 2 + 2 + 2 + 2 // = 40

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
		encodedFeature := &feature.EncodedRelationFeature{
			AbstractEncodedFeature: feature.AbstractEncodedFeature{
				ID:       osmId,
				Geometry: &bboxPolygon, // This is probably temporary until the real geometry collection is stored
				Keys:     encodedKeys,
				Values:   encodedValues,
			},
			NodeIDs:           nodeIds,
			WayIDs:            wayIds,
			ChildRelationIDs:  childRelationIds,
			ParentRelationIDs: parentRelationIds,
		}
		if g.checkFeatureValidity {
			sigolo.Debugf("Check validity of feature %d", encodedFeature.ID)
			g.checkValidity(encodedFeature)
		}

		outputBuffer[currentBufferPos] = encodedFeature
		currentBufferPos++

		if currentBufferPos == len(outputBuffer)-1 {
			output <- outputBuffer
			outputBuffer = make([]feature.EncodedFeature, len(outputBuffer))
			currentBufferPos = 0
		}
	}

	output <- outputBuffer
}

// readNodeToWayMappingFromCellData is a simplified version of the general way-reading function. It returns a mapping of
// node-ID to way-IDs for the given cell file. Therefore, it can be used to determine which ways a node belongs to,
// without reading whole encoded features.
func (g *GridIndexReader) readNodeToWayMappingFromCellData(cellX int, cellY int) (map[uint64][]osm.WayID, error) {
	cellFolderName := path.Join(g.BaseFolder, feature.OsmObjWay.String(), strconv.Itoa(cellX))
	cellFileName := path.Join(cellFolderName, strconv.Itoa(cellY)+".cell")

	if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
		sigolo.Tracef("Cell file %s does not exist, I'll return an empty feature list", cellFileName)
		return nil, nil
	} else if err != nil {
		return nil, errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	sigolo.Tracef("Read cell file %s", cellFileName)
	data, err := os.ReadFile(cellFileName)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, feature.OsmObjWay.String())
	}

	nodeToWays := map[uint64][]osm.WayID{}

	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeWayData".

		/*
			Read general information of the feature
		*/
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		numEncodedKeyBytes := int(binary.LittleEndian.Uint32(data[pos+8:]))
		numValues := int(binary.LittleEndian.Uint32(data[pos+12:]))
		encodedValuesBytes := numValues * 3 // Multiplication since each value is an int with 3 bytes
		numNodes := int(binary.LittleEndian.Uint16(data[pos+16:]))
		nodeBytes := numNodes * 16
		numRelationIDs := int(binary.LittleEndian.Uint16(data[pos+18:]))
		relationIDBytes := numRelationIDs * 8

		headerBytesCount := 8 + 4 + 4 + 2 + 2

		sigolo.Tracef("Read feature pos=%d, id=%d, numKeys=%d, numValues=%d", pos, osmId, numEncodedKeyBytes, numValues)

		encodedValuesStartIndex := pos + headerBytesCount + numEncodedKeyBytes

		/*
			Read the node-IDs of the way
		*/
		nodesStartIndex := encodedValuesStartIndex + encodedValuesBytes
		for i := 0; i < numNodes; i++ {
			nodeIdIndex := nodesStartIndex + i*16
			nodeId := binary.LittleEndian.Uint64(data[nodeIdIndex:])

			if _, ok := nodeToWays[nodeId]; !ok {
				nodeToWays[nodeId] = []osm.WayID{osm.WayID(osmId)}
			} else {
				nodeToWays[nodeId] = append(nodeToWays[nodeId], osm.WayID(osmId))
			}
		}

		pos += headerBytesCount + numEncodedKeyBytes + encodedValuesBytes + nodeBytes + relationIDBytes
	}

	return nodeToWays, nil
}

// readNodeToRelationMappingFromCellData is a simplified version of the general relations-reading function. It returns
// a mapping of node-ID to relation-IDs for the given cell file. Therefore, it can be used to determine which relations
// a node belongs to, without reading whole encoded features.
func (g *GridIndexReader) readNodeToRelationMappingFromCellData(cellX int, cellY int) (map[uint64][]osm.RelationID, error) {
	cellFolderName := path.Join(g.BaseFolder, feature.OsmObjRelation.String(), strconv.Itoa(cellX))
	cellFileName := path.Join(cellFolderName, strconv.Itoa(cellY)+".cell")

	if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
		sigolo.Tracef("Cell file %s does not exist, I'll return an empty feature list", cellFileName)
		return nil, nil
	} else if err != nil {
		return nil, errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	sigolo.Tracef("Read cell file %s", cellFileName)
	data, err := os.ReadFile(cellFileName)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, feature.OsmObjWay.String())
	}

	nodeToRelations := map[uint64][]osm.RelationID{}

	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeRelationData".

		/*
			Read general information of the feature
		*/
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		numEncodedKeyBytes := int(binary.LittleEndian.Uint32(data[pos+24:]))
		numValues := int(binary.LittleEndian.Uint32(data[pos+28:]))
		encodedValuesBytes := numValues * 3 // Multiplication since each value is an int with 3 bytes
		numNodeIds := int(binary.LittleEndian.Uint16(data[pos+32:]))
		nodeBytes := numNodeIds * 8
		numWayIds := int(binary.LittleEndian.Uint16(data[pos+34:]))
		wayBytes := numWayIds * 8
		numChildRelationIds := int(binary.LittleEndian.Uint16(data[pos+36:]))
		childRelationBytes := numChildRelationIds * 8
		numParentRelationIds := int(binary.LittleEndian.Uint16(data[pos+38:]))
		parentRelationBytes := numParentRelationIds * 8

		headerBytesCount := 8 + 16 + 4 + 4 + 2 + 2 + 2 + 2 // = 40

		sigolo.Tracef("Read feature pos=%d, id=%d, numKeys=%d, numValues=%d", pos, osmId, numEncodedKeyBytes, numValues)

		/*
			Read the node-IDs of the relation
		*/
		nodesStartIndex := pos + headerBytesCount + numEncodedKeyBytes + encodedValuesBytes
		for i := 0; i < numNodeIds; i++ {
			nodeIdIndex := nodesStartIndex + i*8
			nodeId := binary.LittleEndian.Uint64(data[nodeIdIndex:])

			if _, ok := nodeToRelations[nodeId]; !ok {
				nodeToRelations[nodeId] = []osm.RelationID{osm.RelationID(osmId)}
			} else {
				nodeToRelations[nodeId] = append(nodeToRelations[nodeId], osm.RelationID(osmId))
			}
		}

		pos += headerBytesCount + numEncodedKeyBytes + encodedValuesBytes + nodeBytes + wayBytes + childRelationBytes + parentRelationBytes
	}

	return nodeToRelations, nil
}

// readWayToRelationMappingFromCellData is a simplified version of the general relations-reading function. It returns
// a mapping of way-ID to relation-IDs for the given cell file. Therefore, it can be used to determine which relations
// a way belongs to, without reading whole encoded features.
func (g *GridIndexReader) readWayToRelationMappingFromCellData(cellX int, cellY int) (map[uint64][]osm.RelationID, error) {
	cellFolderName := path.Join(g.BaseFolder, feature.OsmObjRelation.String(), strconv.Itoa(cellX))
	cellFileName := path.Join(cellFolderName, strconv.Itoa(cellY)+".cell")

	if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
		sigolo.Tracef("Cell file %s does not exist, I'll return an empty feature list", cellFileName)
		return nil, nil
	} else if err != nil {
		return nil, errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	sigolo.Tracef("Read cell file %s", cellFileName)
	data, err := os.ReadFile(cellFileName)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, feature.OsmObjWay.String())
	}

	wayToRelations := map[uint64][]osm.RelationID{}

	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeRelationData".

		/*
			Read general information of the feature
		*/
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		numEncodedKeyBytes := int(binary.LittleEndian.Uint32(data[pos+24:]))
		numValues := int(binary.LittleEndian.Uint32(data[pos+28:]))
		encodedValuesBytes := numValues * 3 // Multiplication since each value is an int with 3 bytes
		numNodeIds := int(binary.LittleEndian.Uint16(data[pos+32:]))
		nodeBytes := numNodeIds * 8
		numWayIds := int(binary.LittleEndian.Uint16(data[pos+34:]))
		wayBytes := numWayIds * 8
		numRelationIds := int(binary.LittleEndian.Uint16(data[pos+36:]))
		relationBytes := numRelationIds * 8
		numParentRelationIds := int(binary.LittleEndian.Uint16(data[pos+38:]))
		parentRelationBytes := numParentRelationIds * 8

		headerBytesCount := 8 + 16 + 4 + 4 + 2 + 2 + 2 + 2 // = 40

		sigolo.Tracef("Read feature pos=%d, id=%d, numKeys=%d, numValues=%d", pos, osmId, numEncodedKeyBytes, numValues)

		/*
			Read the way-IDs of the relation
		*/
		waysStartIndex := pos + headerBytesCount + numEncodedKeyBytes + encodedValuesBytes + nodeBytes
		for i := 0; i < numWayIds; i++ {
			wayIdIndex := waysStartIndex + i*8
			wayId := binary.LittleEndian.Uint64(data[wayIdIndex:])

			if _, ok := wayToRelations[wayId]; !ok {
				wayToRelations[wayId] = []osm.RelationID{osm.RelationID(osmId)}
			} else {
				wayToRelations[wayId] = append(wayToRelations[wayId], osm.RelationID(osmId))
			}
		}

		pos += headerBytesCount + numEncodedKeyBytes + encodedValuesBytes + nodeBytes + wayBytes + relationBytes + parentRelationBytes
	}

	return wayToRelations, nil
}

// readRelationToParentRelationMappingFromCellData is a simplified version of the general relations-reading function. It returns
// a mapping of relation-ID to parent-relation-IDs for the given cell file. Therefore, it can be used to determine which relations
// another relation belongs to, without reading whole encoded features.
func (g *GridIndexReader) readRelationToParentRelationMappingFromCellData(cellX int, cellY int) (map[uint64][]osm.RelationID, error) {
	cellFolderName := path.Join(g.BaseFolder, feature.OsmObjRelation.String(), strconv.Itoa(cellX))
	cellFileName := path.Join(cellFolderName, strconv.Itoa(cellY)+".cell")

	if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
		sigolo.Tracef("Cell file %s does not exist, I'll return an empty feature list", cellFileName)
		return nil, nil
	} else if err != nil {
		return nil, errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	sigolo.Tracef("Read cell file %s", cellFileName)
	data, err := os.ReadFile(cellFileName)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, feature.OsmObjWay.String())
	}

	relationToParentRelations := map[uint64][]osm.RelationID{}

	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeRelationData".

		/*
			Read general information of the feature
		*/
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		numEncodedKeyBytes := int(binary.LittleEndian.Uint32(data[pos+24:]))
		numValues := int(binary.LittleEndian.Uint32(data[pos+28:]))
		encodedValuesBytes := numValues * 3 // Multiplication since each value is an int with 3 bytes
		numNodeIds := int(binary.LittleEndian.Uint16(data[pos+32:]))
		nodeBytes := numNodeIds * 8
		numWayIds := int(binary.LittleEndian.Uint16(data[pos+34:]))
		wayBytes := numWayIds * 8
		numChildRelationIds := int(binary.LittleEndian.Uint16(data[pos+36:]))
		childRelationBytes := numChildRelationIds * 8
		numParentRelationIds := int(binary.LittleEndian.Uint16(data[pos+38:]))
		parentRelationBytes := numParentRelationIds * 8

		headerBytesCount := 8 + 16 + 4 + 4 + 2 + 2 + 2 + 2 // = 40

		sigolo.Tracef("Read feature pos=%d, id=%d, numKeys=%d, numValues=%d", pos, osmId, numEncodedKeyBytes, numValues)

		/*
			Read the relation-IDs of the given child-relation
		*/
		relationsStartIndex := pos + headerBytesCount + numEncodedKeyBytes + encodedValuesBytes + nodeBytes + wayBytes
		for i := 0; i < numChildRelationIds; i++ {
			relationIdIndex := relationsStartIndex + i*8
			relationId := binary.LittleEndian.Uint64(data[relationIdIndex:])

			if _, ok := relationToParentRelations[relationId]; !ok {
				relationToParentRelations[relationId] = []osm.RelationID{osm.RelationID(osmId)}
			} else {
				relationToParentRelations[relationId] = append(relationToParentRelations[relationId], osm.RelationID(osmId))
			}
		}

		pos += headerBytesCount + numEncodedKeyBytes + encodedValuesBytes + nodeBytes + wayBytes + childRelationBytes + parentRelationBytes
	}

	return relationToParentRelations, nil
}

func (g *GridIndexReader) checkValidity(encodedFeature feature.EncodedFeature) {
	// Check keys
	if len(encodedFeature.GetKeys()) > len(g.TagIndex.keyMap) {
		sigolo.Fatalf("Invalid length of keys in feature %d: Expected less than %d but found %d", encodedFeature.GetID(), len(g.TagIndex.keyMap), len(encodedFeature.GetKeys()))
	}

	// Check values
	numberOfSetKeys := 0
	for keyIndex := 0; keyIndex < len(encodedFeature.GetKeys())*8; keyIndex++ {
		if encodedFeature.HasKey(keyIndex) {
			valueIndex := encodedFeature.GetValueIndex(keyIndex)
			if valueIndex > len(g.TagIndex.valueMap[keyIndex])-1 {
				sigolo.Fatalf("Invalid key value found in feature %d: keyIndex=%d, valueIndex=%d, allowedMaxValueIndex=%d", encodedFeature.GetID(), keyIndex, valueIndex, len(g.TagIndex.valueMap[keyIndex])-1)
			}
			numberOfSetKeys++
		}
	}

	if numberOfSetKeys > len(encodedFeature.GetValues()) {
		sigolo.Fatalf("Invalid number of value indices found in feature %d: Expected %d values but found %d", encodedFeature.GetID(), len(encodedFeature.GetValues())-1, numberOfSetKeys)
	}
}

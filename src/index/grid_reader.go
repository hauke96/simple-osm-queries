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

func (g *GridIndex) Get(bbox *orb.Bound, objectType string) (chan *GetFeaturesResult, error) {
	sigolo.Debugf("Get feature from bbox=%#v", bbox)
	minCell := g.GetCellIndexForCoordinate(bbox.Min.Lon(), bbox.Min.Lat())
	maxCell := g.GetCellIndexForCoordinate(bbox.Max.Lon(), bbox.Max.Lat())

	resultChannel := make(chan *GetFeaturesResult, 10)

	numThreads := 3
	var wg sync.WaitGroup
	wg.Add(numThreads)
	go func() {
		// Group the cells into columns of equal size so that each goroutine below can handle on column.
		cellColumns := maxCell.X() - minCell.X() + 1 // min and max are inclusive, therefore +1
		threadColumns := cellColumns / numThreads

		for i := 0; i < numThreads; i++ {
			minColX := minCell.X() + i*threadColumns
			maxColX := minCell.X() + (i+1)*threadColumns
			if i != 0 {
				// To prevent overlapping columns
				minColX++
			}
			if i == numThreads-1 {
				// Last column: Make sure it goes til the requested end
				maxColX = maxCell.X()
			}

			go g.getFeaturesForCellsWithBbox(resultChannel, &wg, bbox, minColX, maxColX, minCell.Y(), maxCell.Y(), objectType)
		}
	}()

	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	return resultChannel, nil // Remove error from return, since it doesn't make any sense here
}

func (g *GridIndex) GetNodes(nodes osm.WayNodes) (chan *GetFeaturesResult, error) {
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

			unfilteredFeatures, err := g.readFeaturesFromCellFile(cell[0], cell[1], feature.OsmObjNode.String())
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

// GetCellIndexForCoordinate returns the cell index (i.e. position) for the given coordinate.
func (g *GridIndex) GetCellIndexForCoordinate(x float64, y float64) CellIndex {
	return CellIndex{int(x / g.CellWidth), int(y / g.CellHeight)}
}

func (g *GridIndex) GetFeaturesForCells(cells []CellIndex, objectType string) chan *GetFeaturesResult {
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

func (g *GridIndex) getFeaturesForCellsWithBbox(output chan *GetFeaturesResult, wg *sync.WaitGroup, bbox *orb.Bound, minCellX int, maxCellX int, minCellY int, maxCellY int, objectType string) {
	sigolo.Tracef("Get feature for cell column from=%d, to=%d", minCellX, maxCellX)
	for cellX := minCellX; cellX <= maxCellX; cellX++ {
		for cellY := minCellY; cellY <= maxCellY; cellY++ {
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
}

// readFeaturesFromCellFile reads all features from the specified cell and writes them periodically to the output channel.
func (g *GridIndex) readFeaturesFromCellFile(cellX int, cellY int, objectType string) ([]feature.EncodedFeature, error) {
	cellFolderName := path.Join(g.BaseFolder, objectType, strconv.Itoa(cellX))
	cellFileName := path.Join(cellFolderName, strconv.Itoa(cellY)+".cell")

	if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
		sigolo.Tracef("Cell file %s does not exist, I'll return an empty feature list", cellFileName)
		return nil, nil
	} else if err != nil {
		return nil, errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	g.featureCacheMutex.Lock()
	if cachedFeatures, ok := g.featureCache[cellFileName]; ok {
		sigolo.Tracef("Use features from cache for cell file %s", cellFileName)
		g.featureCacheMutex.Unlock()
		return cachedFeatures, nil
	}
	g.featureCache[cellFileName] = []feature.EncodedFeature{}
	g.featureCacheMutex.Unlock()

	sigolo.Tracef("Read cell file %s", cellFileName)
	data, err := os.ReadFile(cellFileName)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, objectType)
	}

	readFeatureChannel := make(chan []feature.EncodedFeature)
	go func() {
		for readFeatures := range readFeatureChannel {
			// TODO not-null check needed for the features?
			g.featureCacheMutex.Lock()
			g.featureCache[cellFileName] = append(g.featureCache[cellFileName], readFeatures...)
			g.featureCacheMutex.Unlock()
		}
	}()

	switch objectType {
	case "node":
		g.readNodesFromCellData(readFeatureChannel, data)
	case "way":
		g.readWaysFromCellData(readFeatureChannel, data)
	default:
		panic("Unsupported object type to read: " + objectType)
	}

	close(readFeatureChannel)

	return g.featureCache[cellFileName], nil
}

func (g *GridIndex) readNodesFromCellData(output chan []feature.EncodedFeature, data []byte) {
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

func (g *GridIndex) readWaysFromCellData(output chan []feature.EncodedFeature, data []byte) {
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

// readNodeToWayMappingFromCellData is a simplified version of the general way-reading function. It returns a mapping of
// node-ID to way-IDs for the given cell file. Therefore, it can be used to determine which ways a node belongs to,
// without reading whole encoded features.
func (g *GridIndex) readNodeToWayMappingFromCellData(cellX int, cellY int) (map[uint64][]osm.WayID, error) {
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

func (g *GridIndex) checkValidity(encodedFeature feature.EncodedFeature) {
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

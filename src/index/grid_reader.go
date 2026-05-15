package index

import (
	"encoding/binary"
	"os"
	"path"
	"soq/common"
	"soq/feature"
	"soq/index/storage"
	ownOsm "soq/osm"
	"strconv"
	"sync"

	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"github.com/pkg/errors"
)

type GridIndexReader struct {
	BaseGridIndex

	featureReader        *storage.FeatureStorageReader
	checkFeatureValidity bool
}

func LoadGridIndex(indexBaseFolder string, cellWidth float64, cellHeight float64, checkFeatureValidity bool, tagIndex *TagIndex, featureReader *storage.FeatureStorageReader) *GridIndexReader {
	return &GridIndexReader{
		BaseGridIndex: BaseGridIndex{
			TagIndex:   tagIndex,
			CellWidth:  cellWidth,
			CellHeight: cellHeight,
			BaseFolder: path.Join(indexBaseFolder, GridIndexFolder),
		},
		featureReader:        featureReader,
		checkFeatureValidity: checkFeatureValidity,
	}
}

func (g *GridIndexReader) Get(bbox *orb.Bound, objectType ownOsm.OsmObjectType) (chan *GetFeaturesResult, error) {
	sigolo.Debugf("Get feature from bbox=%#v", bbox)
	minCell := g.GetCellIndexForCoordinate(bbox.Min.Lon(), bbox.Min.Lat())
	maxCell := g.GetCellIndexForCoordinate(bbox.Max.Lon(), bbox.Max.Lat())

	resultChannel := make(chan *GetFeaturesResult)

	// TODO determine CellExtents here and distribute them across the threads
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

func (g *GridIndexReader) GetFeaturesForCells(cells []common.CellIndex, objectType ownOsm.OsmObjectType) chan *GetFeaturesResult {
	resultChannel := make(chan *GetFeaturesResult)

	cellExtents := g.featureReader.GetExtentsForCells(cells)

	go func() {
		for _, cellExtent := range cellExtents {
			featuresInCell := &GetFeaturesResult{
				Cell:     cellExtent,
				Features: []feature.Feature{},
			}

			encodedFeatures, err := g.readFeatures(cellExtent, objectType)
			sigolo.FatalCheck(err)
			featuresInCell.Features = encodedFeatures

			resultChannel <- featuresInCell
		}
		close(resultChannel)
	}()

	return resultChannel
}

func (g *GridIndexReader) getFeaturesForCellsWithBbox(output chan *GetFeaturesResult, wg *sync.WaitGroup, bbox *orb.Bound, minCellX int, maxCellX int, minCellY int, maxCellY int, objectType ownOsm.OsmObjectType) {
	sigolo.Debugf("Get %s features for cells minX=%d, minY=%d / maxX=%d, maxY=%d", objectType.String(), minCellX, minCellY, maxCellX, maxCellY)

	var cells []common.CellIndex

	for cellX := minCellX; cellX <= maxCellX; cellX++ {
		for cellY := minCellY; cellY <= maxCellY; cellY++ {
			cells = append(cells, common.CellIndex{cellX, cellY})
		}
	}

	cellExtents := g.featureReader.GetExtentsForCells(cells)

	for _, cellExtent := range cellExtents {
		sigolo.Debugf("Get %s features for cell extent %v", objectType.String(), cellExtent)

		featuresInBbox := &GetFeaturesResult{
			Cell:     cellExtent,
			Features: []feature.Feature{},
		}

		encodedFeatures, err := g.readFeatures(cellExtent, objectType)
		sigolo.FatalCheck(err)

		for i := 0; i < len(encodedFeatures); i++ {
			encodedFeature := encodedFeatures[i]
			if encodedFeature.GetID() == 108782320 {
				sigolo.Debug("Found")
			}
			if encodedFeature != nil && bbox.Intersects(encodedFeature.GetGeometry().Bound()) {
				featuresInBbox.Features = append(featuresInBbox.Features, encodedFeature)
			}
		}

		output <- featuresInBbox
	}

	wg.Done()

	sigolo.Debugf("Finished getting %s features for cells minX=%d, maxX=%d / minY=%d, maxY=%d", objectType, minCellX, maxCellX, minCellY, maxCellY)
}

// readFeatures reads all features from the specified cell and writes them periodically to the output channel.
func (g *GridIndexReader) readFeatures(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType) ([]feature.Feature, error) {
	switch objectType {
	case ownOsm.OsmObjNode:
		return g.featureReader.ReadNodes(cellExtent)
	case ownOsm.OsmObjWay:
		return g.featureReader.ReadWays(cellExtent)
	case ownOsm.OsmObjRelation:
		return g.featureReader.ReadRelations(cellExtent)
	default:
		panic("Unsupported object type to read: " + objectType.String())
	}
}

// readNodeToWayMappingFromCellData is a simplified version of the general way-reading function. It returns a mapping of
// node-ID to way-IDs for the given cell file. Therefore, it can be used to determine which ways a node belongs to,
// without reading whole encoded features.
func (g *GridIndexReader) readNodeToWayMappingFromCellData(cellX int, cellY int) (map[uint64][]osm.WayID, error) {
	cellFolderName := path.Join(g.BaseFolder, ownOsm.OsmObjWay.String(), strconv.Itoa(cellX))
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
		return nil, errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, ownOsm.OsmObjWay.String())
	}

	nodeToWays := map[uint64][]osm.WayID{}

	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeWayData".

		/*
			Read general information of the feature
		*/
		osmId := binary.LittleEndian.Uint64(data[pos+0:])
		numEncodedKeyBytes := int(binary.LittleEndian.Uint16(data[pos+8:]))
		numValues := int(binary.LittleEndian.Uint16(data[pos+10:]))
		encodedValuesBytes := numValues * 3 // Multiplication since each value is an int with 3 bytes
		numNodes := int(binary.LittleEndian.Uint16(data[pos+12:]))
		nodeBytes := numNodes * 16
		numRelationIds := int(binary.LittleEndian.Uint16(data[pos+14:]))
		relationIdBytes := numRelationIds * 8

		headerBytesCount := 8 + 2 + 2 + 2 + 2

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

		pos += headerBytesCount + numEncodedKeyBytes + encodedValuesBytes + nodeBytes + relationIdBytes
	}

	return nodeToWays, nil
}

// readObjectsToRelationMappingFromCellData is a simplified version of the general relations-reading function. It returns
// a mapping of node-ID to relation-IDs for the given cell file. Therefore, it can be used to determine which relations
// a node belongs to, without reading whole encoded features.
func (g *GridIndexReader) readObjectsToRelationMappingFromCellData(cellX int, cellY int) (map[uint64][]osm.RelationID, map[uint64][]osm.RelationID, map[uint64][]osm.RelationID, error) {
	cellFolderName := path.Join(g.BaseFolder, ownOsm.OsmObjRelation.String(), strconv.Itoa(cellX))
	cellFileName := path.Join(cellFolderName, strconv.Itoa(cellY)+".cell")

	if _, err := os.Stat(cellFileName); errors.Is(err, os.ErrNotExist) {
		sigolo.Tracef("Cell file %s does not exist, I'll return an empty feature list", cellFileName)
		return nil, nil, nil, nil
	} else if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "Unable to get existance status of cell file %s", cellFileName)
	}

	sigolo.Tracef("Read cell file %s", cellFileName)
	data, err := os.ReadFile(cellFileName)
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "Unable to read cell x=%d, y=%d, type=%s", cellX, cellY, ownOsm.OsmObjWay.String())
	}

	nodeToRelations := make(map[uint64][]osm.RelationID)
	wayToRelations := make(map[uint64][]osm.RelationID)
	relationToParentRelations := make(map[uint64][]osm.RelationID)

	for pos := 0; pos < len(data); {
		// See format details (bit position, field sizes, etc.) in function "writeRelationData".

		/*
			Read general information of the feature
		*/
		relationId := osm.RelationID(binary.LittleEndian.Uint64(data[pos+0:]))
		numEncodedKeyBytes := int(binary.LittleEndian.Uint16(data[pos+24:]))
		numValues := int(binary.LittleEndian.Uint16(data[pos+16:]))
		encodedValuesBytes := numValues * 3 // Multiplication since each value is an int with 3 bytes
		numNodeIds := int(binary.LittleEndian.Uint16(data[pos+28:]))
		numWayIds := int(binary.LittleEndian.Uint16(data[pos+30:]))
		numChildRelationIds := int(binary.LittleEndian.Uint16(data[pos+32:]))
		numParentRelationIds := int(binary.LittleEndian.Uint16(data[pos+34:]))
		parentRelationBytes := numParentRelationIds * 8

		headerBytesCount := 8 + 16 + 2 + 2 + 2 + 2 + 2 + 2 // = 36

		sigolo.Tracef("Read feature pos=%d, id=%d, numKeys=%d, numValues=%d", pos, relationId, numEncodedKeyBytes, numValues)

		/*
			Read the node-IDs of the relation
		*/
		pos += headerBytesCount + numEncodedKeyBytes + encodedValuesBytes
		for i := 0; i < numNodeIds; i++ {
			nodeId := binary.LittleEndian.Uint64(data[pos:])
			nodeToRelations[nodeId] = append(nodeToRelations[nodeId], relationId)
			pos += 8
		}

		/*
			Read the way-IDs of the relation
		*/
		for i := 0; i < numWayIds; i++ {
			wayId := binary.LittleEndian.Uint64(data[pos:])
			wayToRelations[wayId] = append(wayToRelations[wayId], relationId)
			pos += 8
		}

		/*
			Read the child relation-IDs of the current relation to create the inverse mapping
		*/
		for i := 0; i < numChildRelationIds; i++ {
			childRelationId := binary.LittleEndian.Uint64(data[pos:])
			relationToParentRelations[childRelationId] = append(relationToParentRelations[childRelationId], relationId)
			pos += 8
		}

		pos += parentRelationBytes
	}

	return nodeToRelations, wayToRelations, relationToParentRelations, nil
}

func (g *GridIndexReader) checkValidity(encodedFeature feature.Feature) {
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

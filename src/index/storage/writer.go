package storage

import (
	"bufio"
	"encoding/binary"
	"math"
	os "os"
	"soq/common"
	"soq/feature"
	"soq/profiler"

	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/osm"
	"github.com/pkg/errors"
)

// TODO move to separate namespace to hide internal functions
type FeatureStorageWriter struct {
	indexFileWriter     *bufio.Writer
	indexFileCursorByte int64 // Byte at which point the next data will be written. Is initially 0.
	indexMetadata       *indexMetadata

	nodeCache     map[common.CellExtent][]feature.NodeFeature
	wayCache      map[common.CellExtent][]feature.WayFeature
	relationCache map[common.CellExtent][]feature.RelationFeature
	maxCacheSize  int // Number of features. If exceeded, the cache (i.e. the list of node features for a certain cell extent) is written to disk.

	nodeToWayMapping          map[osm.NodeID][]osm.WayID
	nodeToRelationMapping     map[osm.NodeID][]osm.RelationID
	wayToRelationMapping      map[osm.WayID][]osm.RelationID
	relationToRelationMapping map[osm.RelationID][]osm.RelationID
}

func NewFeatureStorageWriter(baseFolder string) *FeatureStorageWriter {
	var file *os.File

	indexFileName := baseFolder + "/index"

	// Ensure the folder exists
	if _, err := os.Stat(baseFolder); os.IsNotExist(err) {
		sigolo.Tracef("Index folder %s doesn't exist, I'll create it", baseFolder)
		err = os.MkdirAll(baseFolder, os.ModePerm)
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to create index folder %s", baseFolder))
	}

	if _, err := os.Stat(indexFileName); err == nil {
		// Index file does exist -> open it
		sigolo.Tracef("Index file %s already exist but is not cached, I'll open it", indexFileName)
		file, err = os.OpenFile(indexFileName, os.O_RDWR, 0666)
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to open index file %s", indexFileName))
	} else if errors.Is(err, os.ErrNotExist) {
		// Index file does NOT exist -> create its folder (if needed) and the file itself

		// Create index file
		sigolo.Tracef("Index file %s does not exist, I'll create it", indexFileName)
		file, err = os.Create(indexFileName)
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to create new index file %s", indexFileName))
	} else {
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to get existance status of index file %s", indexFileName))
	}

	return &FeatureStorageWriter{
		indexFileWriter:           bufio.NewWriter(file),
		indexFileCursorByte:       0,
		indexMetadata:             &indexMetadata{},
		nodeCache:                 make(map[common.CellExtent][]feature.NodeFeature),
		wayCache:                  make(map[common.CellExtent][]feature.WayFeature),
		relationCache:             make(map[common.CellExtent][]feature.RelationFeature),
		maxCacheSize:              1000, // TODO make this configurable
		nodeToWayMapping:          make(map[osm.NodeID][]osm.WayID),
		nodeToRelationMapping:     make(map[osm.NodeID][]osm.RelationID),
		wayToRelationMapping:      make(map[osm.WayID][]osm.RelationID),
		relationToRelationMapping: make(map[osm.RelationID][]osm.RelationID),
	}
}

// WriteNodeFeature writes the given feature to the internal cache, which is eventually flushed to disk. This method not
// necessarily performs any I/O operations.
//
// The firstPass argument specifies whether parent-IDs (i.e. the way-IDs of
// a given node) should be collected from the given encoded features or written to them. Call this method first with
// "true" as argument to collect the parent IDs. Call this method again with "false" as argument to write the data
// including their parent IDs into the final index file.
func (w *FeatureStorageWriter) WriteNodeFeature(feature feature.NodeFeature, cellExtent common.CellExtent, firstPass bool) error {
	key := profiler.StartMeasurement()
	defer profiler.EndMeasurement(key)

	id := osm.NodeID(feature.GetID())

	if !firstPass {
		if wayIds, ok := w.nodeToWayMapping[id]; ok {
			feature.SetWayIds(wayIds)
		}
		if relationIds, ok := w.nodeToRelationMapping[id]; ok {
			feature.SetRelationIds(relationIds)
		}
	}

	w.nodeCache[cellExtent] = append(w.nodeCache[cellExtent], feature)

	return w.flushCachesIfNeeded()
}

// WriteWayFeature writes the given feature to the internal cache, which is eventually flushed to disk. This method not
// necessarily performs any I/O operations.
//
// The firstPass argument specifies whether parent-IDs (i.e. the way-IDs of
// a given node) should be collected from the given encoded features or written to them. Call this method first with
// "true" as argument to collect the parent IDs. Call this method again with "false" as argument to write the data
// including their parent IDs into the final index file.
func (w *FeatureStorageWriter) WriteWayFeature(feature feature.WayFeature, cellExtent common.CellExtent, firstPass bool) error {
	key := profiler.StartMeasurement()
	defer profiler.EndMeasurement(key)

	id := osm.WayID(feature.GetID())

	if firstPass {
		for _, node := range feature.GetNodes() {
			w.nodeToWayMapping[node.ID] = append(w.nodeToWayMapping[node.ID], id)
		}
	} else {
		if relationIds, ok := w.wayToRelationMapping[id]; ok {
			feature.SetRelationIds(relationIds)
		}
	}

	w.wayCache[cellExtent] = append(w.wayCache[cellExtent], feature)

	return w.flushCachesIfNeeded()
}

// WriteRelationFeature writes the given feature to the internal cache, which is eventually flushed to disk. This method not
// necessarily performs any I/O operations.
//
// The firstPass argument specifies whether parent-IDs (i.e. the way-IDs of
// a given node) should be collected from the given encoded features or written to them. Call this method first with
// "true" as argument to collect the parent IDs. Call this method again with "false" as argument to write the data
// including their parent IDs into the final index file.
func (w *FeatureStorageWriter) WriteRelationFeature(feature feature.RelationFeature, cellExtent common.CellExtent, firstPass bool) error {
	key := profiler.StartMeasurement()
	defer profiler.EndMeasurement(key)

	id := osm.RelationID(feature.GetID())

	if firstPass {
		for _, nodeId := range feature.GetNodeIds() {
			w.nodeToRelationMapping[nodeId] = append(w.nodeToRelationMapping[nodeId], id)
		}
		for _, wayId := range feature.GetWayIds() {
			w.wayToRelationMapping[wayId] = append(w.wayToRelationMapping[wayId], id)
		}
		for _, relationId := range feature.GetChildRelationIds() {
			w.relationToRelationMapping[relationId] = append(w.relationToRelationMapping[relationId], id)
		}
	} else {
		if relationIds, ok := w.relationToRelationMapping[id]; ok {
			feature.SetParentRelationIds(relationIds)
		}
	}

	w.relationCache[cellExtent] = append(w.relationCache[cellExtent], feature)

	return w.flushCachesIfNeeded()
}

// flushCachesIfNeeded writes full caches, i.e. caches above the size threshold, to disk.
func (w *FeatureStorageWriter) flushCachesIfNeeded() error {
	// TODO mutex needed?

	for cellExtent, encodedFeatures := range w.nodeCache {
		if len(encodedFeatures) > w.maxCacheSize {
			metadata := w.indexMetadata.getCellMetadata(cellExtent)
			metadata.NodeOffsets = append(metadata.NodeOffsets, w.indexFileCursorByte)

			for _, encodedFeature := range encodedFeatures {
				err := w.writeNodeData(encodedFeature)
				if err != nil {
					return err
				}
			}
		}
	}

	for cellExtent, encodedFeatures := range w.wayCache {
		if len(encodedFeatures) > w.maxCacheSize {
			metadata := w.indexMetadata.getCellMetadata(cellExtent)
			metadata.WayOffsets = append(metadata.WayOffsets, w.indexFileCursorByte)

			for _, encodedFeature := range encodedFeatures {
				err := w.writeWayData(encodedFeature)
				if err != nil {
					return err
				}
			}
		}
	}

	for cellExtent, encodedFeatures := range w.relationCache {
		if len(encodedFeatures) > w.maxCacheSize {
			metadata := w.indexMetadata.getCellMetadata(cellExtent)
			metadata.RelationOffsets = append(metadata.NodeOffsets, w.indexFileCursorByte)

			for _, encodedFeature := range encodedFeatures {
				err := w.writeRelationData(encodedFeature)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (w *FeatureStorageWriter) writeNodeData(encodedFeature feature.NodeFeature) error {
	/*
		Entry format:
		// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes).

		Names: | osmId | lon | lat | num. keys | num. values | num. ways | num. rels |   encodedKeys   |   encodedValues   |     way IDs     |   relation IDs  |
		Bytes: |   8   |  4  |  4  |     2     |      2      |     2     |     2     | <num. keys> / 8 | <num. values> * 3 | <num. ways> * 8 | <num. rels> * 8 |

		The encodedKeys is a bit-string (each key 1 bit), that why the division by 8 happens. The stored value is the
		number of bytes in the keys array of the feature (i.e. "len(encodedFeature.GetKeys())"). The encodedValue part, however,
		is an int-array, therefore, we need the multiplication with 4.
	*/

	// The number of key-bins to store is determined by the bin with the highest index that is not empty (i.e. all 0s).
	// If only the first bin contains some 1s (i.e. keys that are set on the feature) and the next 100 bins are empty,
	// then there's no reason to store those empty bins. This reduced the cell-file size for hamburg-latest (45 MB PBF)
	// by a factor of ten!
	numKeys := 0
	for i := 0; i < len(encodedFeature.GetKeys()); i++ {
		if encodedFeature.GetKeys()[i] != 0 {
			numKeys = i + 1
		}
	}

	encodedKeyBytes := numKeys                                  // Is already a byte-array -> no division by 8 needed
	encodedValueBytes := len(encodedFeature.GetValues()) * 3    // Int array and int = 4 bytes
	wayIdBytes := len(encodedFeature.GetWayIds()) * 8           // IDs are all 64-bit integers
	relationIdBytes := len(encodedFeature.GetRelationIds()) * 8 // IDs are all 64-bit integers

	headerBytesCount := 8 + 4 + 4 + 2 + 2 + 2 + 2 // = 24
	byteCount := headerBytesCount
	byteCount += encodedKeyBytes
	byteCount += encodedValueBytes
	byteCount += wayIdBytes
	byteCount += relationIdBytes

	data := make([]byte, byteCount)

	binary.LittleEndian.PutUint64(data[0:], encodedFeature.GetID())
	binary.LittleEndian.PutUint32(data[8:], math.Float32bits(float32(encodedFeature.GetLon())))
	binary.LittleEndian.PutUint32(data[12:], math.Float32bits(float32(encodedFeature.GetLat())))
	binary.LittleEndian.PutUint16(data[16:], uint16(numKeys))
	binary.LittleEndian.PutUint16(data[18:], uint16(len(encodedFeature.GetValues())))
	binary.LittleEndian.PutUint16(data[20:], uint16(len(encodedFeature.GetWayIds())))
	binary.LittleEndian.PutUint16(data[22:], uint16(len(encodedFeature.GetRelationIds())))

	pos := headerBytesCount

	/*
		Write keys
	*/
	copy(data[pos:], encodedFeature.GetKeys()[0:numKeys])
	pos += numKeys

	/*
		Write values
	*/
	for _, v := range encodedFeature.GetValues() {
		data[pos] = byte(v)
		data[pos+1] = byte(v >> 8)
		data[pos+2] = byte(v >> 16)
		pos += 3
	}

	/*
		Write way-IDs
	*/
	for _, wayId := range encodedFeature.GetWayIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(wayId))
		pos += 8
	}

	/*
		Write relation-IDs
	*/
	for _, relationId := range encodedFeature.GetRelationIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	writtenBytes, err := w.indexFileWriter.Write(data)
	if err != nil {
		return err
	}

	w.indexFileCursorByte += int64(writtenBytes)

	return nil
}

func (w *FeatureStorageWriter) writeWayData(encodedFeature feature.WayFeature) error {
	/*
		Entry format:
		// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes).

		Names: | osmId | num. keys | num. values | num. nodes | num. rels |   encodedKeys   |   encodedValues   |       nodes       |       rels      |
		Bytes: |   8   |     2     |      2      |      2     |     2     | <num. keys> / 8 | <num. values> * 3 | <num. nodes> * 16 | <num. rels> * 8 |

		The encodedKeys is a bit-string (each key 1 bit), that why the division by 8 happens. The stored value is the
		number of bytes in the keys array of the feature (i.e. "len(encodedFeature.GetKeys())"). The encodedValue part, however,
		is an int-array, therefore, we need the multiplication with 4.

		The nodes section contains all nodes, not only the ones within this cell. This enables geometric checks, even
		in cases where no way-node is within this cell. The nodes are stores in the following way:
		<id (64-bit)><lon (32-bit)><lat (23-bit)>
	*/
	// The number of key-bins to store is determined by the bin with the highest index that is not empty (i.e. all 0s).
	// If only the first bin contains some 1s (i.e. keys that are set on the feature) and the next 100 bins are empty,
	// then there's no reason to store those empty bins. This reduced the cell-file size for hamburg-latest (45 MB PBF)
	// by a factor of ten!
	numEncodedKeyBytes := 0
	for i := 0; i < len(encodedFeature.GetKeys()); i++ {
		if encodedFeature.GetKeys()[i] != 0 {
			numEncodedKeyBytes = i + 1
		}
	}

	numEncodedValueBytes := len(encodedFeature.GetValues()) * 3 // Int array and int = 4 bytes
	nodeIdBytes := len(encodedFeature.GetNodes()) * 16          // Each ID is a 64-bit int + 2*4 bytes for lat/lon
	relationIdBytes := len(encodedFeature.GetRelationIds()) * 8 // Each ID is a 64-bit int

	headerByteCount := 8 + 2 + 2 + 2 + 2
	byteCount := headerByteCount
	byteCount += numEncodedKeyBytes
	byteCount += numEncodedValueBytes
	byteCount += nodeIdBytes
	byteCount += relationIdBytes

	data := make([]byte, byteCount)

	/*
		Write header
	*/
	binary.LittleEndian.PutUint64(data[0:], encodedFeature.GetID())
	binary.LittleEndian.PutUint16(data[8:], uint16(numEncodedKeyBytes))
	binary.LittleEndian.PutUint16(data[10:], uint16(len(encodedFeature.GetValues())))
	binary.LittleEndian.PutUint16(data[12:], uint16(len(encodedFeature.GetNodes())))
	binary.LittleEndian.PutUint16(data[14:], uint16(len(encodedFeature.GetRelationIds())))

	pos := headerByteCount

	/*
		Write keys
	*/
	copy(data[pos:], encodedFeature.GetKeys()[0:numEncodedKeyBytes])
	pos += numEncodedKeyBytes

	/*
		Write value
	*/
	for _, v := range encodedFeature.GetValues() {
		data[pos] = byte(v)
		data[pos+1] = byte(v >> 8)
		data[pos+2] = byte(v >> 16)
		pos += 3
	}

	/*
		Write nodes
	*/
	for _, node := range encodedFeature.GetNodes() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(node.ID))
		binary.LittleEndian.PutUint32(data[pos+8:], math.Float32bits(float32(node.Lon)))
		binary.LittleEndian.PutUint32(data[pos+12:], math.Float32bits(float32(node.Lat)))
		pos += 16
	}

	/*
		Write relation-IDs
	*/
	for _, relationId := range encodedFeature.GetRelationIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	writtenBytes, err := w.indexFileWriter.Write(data)
	if err != nil {
		return err
	}

	w.indexFileCursorByte += int64(writtenBytes)

	return nil
}

func (w *FeatureStorageWriter) writeRelationData(encodedFeature feature.RelationFeature) error {
	/*
		Entry format:
		// TODO Globally the "name" key has more than 2^24 values (max. number that can be represented with 3 bytes).

		Names: | osmId | bbox | num. keys | num. values | num. nodes | num. ways | num. child rels | num. parent rels |   encodedKeys   |   encodedValues   |     node IDs     |     way IDs     |    child rel. IDs     |    parent rel. IDs     |
		Bytes: |   8   |  16  |     2     |      2      |      2     |     2     |        2        |         2        | <num. keys> / 8 | <num. values> * 3 | <num. nodes> * 8 | <num. ways> * 8 | <num. child rels> * 8 | <num. parent rels> * 8 |

		The encodedKeys is a bit-string (each key 1 bit), that why the division by 8 happens. The stored value is the
		number of bytes in the keys array of the feature (i.e. "len(encodedFeature.GetKeys())"). The encodedValue part, however,
		is an int-array, therefore, we need the multiplication with 4.

		The "bbox" field are 4 32-bit floats for the min-lon, min-lat, max-lon and max-lat values.

		// TODO store real geometry. Including geometry of sub-relations?
	*/
	numKeys := 0
	for i := 0; i < len(encodedFeature.GetKeys()); i++ {
		if encodedFeature.GetKeys()[i] != 0 {
			numKeys = i + 1
		}
	}

	encodedKeyBytes := numKeys                                              // Is already a byte-array -> no division by 8 needed
	encodedValueBytes := len(encodedFeature.GetValues()) * 3                // Int array and int = 4 bytes
	nodeIdBytes := len(encodedFeature.GetNodeIds()) * 8                     // IDs are all 64-bit integers
	wayIdBytes := len(encodedFeature.GetWayIds()) * 8                       // IDs are all 64-bit integers
	childRelationIdBytes := len(encodedFeature.GetChildRelationIds()) * 8   // IDs are all 64-bit integers
	parentRelationIdBytes := len(encodedFeature.GetParentRelationIds()) * 8 // IDs are all 64-bit integers

	headerBytesCount := 8 + 16 + 2 + 2 + 2 + 2 + 2 + 2 // = 36
	byteCount := headerBytesCount
	byteCount += encodedKeyBytes
	byteCount += encodedValueBytes
	byteCount += nodeIdBytes
	byteCount += wayIdBytes
	byteCount += childRelationIdBytes
	byteCount += parentRelationIdBytes

	data := make([]byte, byteCount)

	bbox := encodedFeature.GetGeometry().Bound()

	binary.LittleEndian.PutUint64(data[0:], encodedFeature.GetID())
	binary.LittleEndian.PutUint32(data[8:], math.Float32bits(float32(bbox.Min.Lon())))
	binary.LittleEndian.PutUint32(data[12:], math.Float32bits(float32(bbox.Min.Lat())))
	binary.LittleEndian.PutUint32(data[16:], math.Float32bits(float32(bbox.Max.Lon())))
	binary.LittleEndian.PutUint32(data[20:], math.Float32bits(float32(bbox.Max.Lat())))
	binary.LittleEndian.PutUint16(data[24:], uint16(numKeys))
	binary.LittleEndian.PutUint16(data[26:], uint16(len(encodedFeature.GetValues())))
	binary.LittleEndian.PutUint16(data[28:], uint16(len(encodedFeature.GetNodeIds())))
	binary.LittleEndian.PutUint16(data[30:], uint16(len(encodedFeature.GetWayIds())))
	binary.LittleEndian.PutUint16(data[32:], uint16(len(encodedFeature.GetChildRelationIds())))
	binary.LittleEndian.PutUint16(data[34:], uint16(len(encodedFeature.GetParentRelationIds())))

	pos := headerBytesCount

	/*
		Write keys
	*/
	copy(data[pos:], encodedFeature.GetKeys()[0:numKeys])
	pos += numKeys

	/*
		Write values
	*/
	for _, v := range encodedFeature.GetValues() {
		data[pos] = byte(v)
		data[pos+1] = byte(v >> 8)
		data[pos+2] = byte(v >> 16)
		pos += 3
	}

	/*
		Write node-IDs
	*/
	for _, nodeId := range encodedFeature.GetNodeIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(nodeId))
		pos += 8
	}

	/*
		Write way-IDs
	*/
	for _, wayId := range encodedFeature.GetWayIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(wayId))
		pos += 8
	}

	/*
		Write child relation-IDs
	*/
	for _, relationId := range encodedFeature.GetChildRelationIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	/*
		Write parent relation-IDs
	*/
	for _, relationId := range encodedFeature.GetParentRelationIds() {
		binary.LittleEndian.PutUint64(data[pos:], uint64(relationId))
		pos += 8
	}

	/*
		Write data to disk
	*/
	writtenBytes, err := w.indexFileWriter.Write(data)
	if err != nil {
		return err
	}

	w.indexFileCursorByte += int64(writtenBytes)

	return nil
}

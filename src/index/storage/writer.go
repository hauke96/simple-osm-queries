package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"math"
	os "os"
	"soq/common"
	"soq/feature"
	indexCommon "soq/index/common"
	"soq/profiler"

	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/pkg/errors"
)

type FeatureStorageWriter struct {
	indexFileWriter       *bufio.Writer
	indexFileCursorByte   int64 // Byte at which point the next data will be written. Is initially 0.
	indexMetadata         *indexMetadata
	indexMetadataFileName string

	nodeCache     map[common.CellExtent][]feature.NodeFeature
	wayCache      map[common.CellExtent][]feature.WayFeature
	relationCache map[common.CellExtent][]feature.RelationFeature
	maxCacheSize  int // Number of features. If exceeded, the cache (i.e. the list of node features for a certain cell extent) is written to disk.
}

func NewFeatureStorageWriter(baseFolder string, filename string) *FeatureStorageWriter {
	var file *os.File

	indexFileName := baseFolder + "/" + filename
	sigolo.Debugf("Use index file %s for writer", indexFileName)

	// Ensure the folder exists
	if _, err := os.Stat(baseFolder); os.IsNotExist(err) {
		sigolo.Tracef("Index folder %s doesn't exist, I'll create it", baseFolder)
		err = os.MkdirAll(baseFolder, os.ModePerm)
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to create index folder %s", baseFolder))
	}

	if _, err := os.Stat(indexFileName); err == nil {
		// Index file DOES exist -> open it
		sigolo.Tracef("Index file %s already exist but is not cached, I'll open it", indexFileName)
		file, err = os.OpenFile(indexFileName, os.O_RDWR, 0666)
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to open index file %s", indexFileName))
	} else if errors.Is(err, os.ErrNotExist) {
		// Index file does NOT exist -> create new index file
		sigolo.Tracef("Index file %s does not exist, I'll create it", indexFileName)
		file, err = os.Create(indexFileName)
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to create new index file %s", indexFileName))
	} else {
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to get existance status of index file %s", indexFileName))
	}

	return &FeatureStorageWriter{
		indexFileWriter:       bufio.NewWriter(file),
		indexFileCursorByte:   0,
		indexMetadata:         &indexMetadata{},
		indexMetadataFileName: baseFolder + "/metadata.json",
		nodeCache:             make(map[common.CellExtent][]feature.NodeFeature),
		wayCache:              make(map[common.CellExtent][]feature.WayFeature),
		relationCache:         make(map[common.CellExtent][]feature.RelationFeature),
		maxCacheSize:          1_000_000, // TODO make this configurable
	}
}

// WriteNodeFeature writes the given feature to the internal cache, which is eventually flushed to disk. This method not
// necessarily performs any I/O operations.
//
// The firstPass argument specifies whether parent-IDs (i.e. the way-IDs of
// a given node) should be collected from the given encoded features or written to them. Call this method first with
// "true" as argument to collect the parent IDs. Call this method again with "false" as argument to write the data
// including their parent IDs into the final index file.
func (w *FeatureStorageWriter) WriteNodeFeature(feature feature.NodeFeature, cellExtent common.CellExtent) error {
	key := profiler.StartMeasurement()
	defer profiler.EndMeasurement(key)

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
func (w *FeatureStorageWriter) WriteWayFeature(feature feature.WayFeature, cellExtent common.CellExtent) error {
	key := profiler.StartMeasurement()
	defer profiler.EndMeasurement(key)

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
func (w *FeatureStorageWriter) WriteRelationFeature(feature feature.RelationFeature, cellExtent common.CellExtent) error {
	key := profiler.StartMeasurement()
	defer profiler.EndMeasurement(key)

	w.relationCache[cellExtent] = append(w.relationCache[cellExtent], feature)

	return w.flushCachesIfNeeded()
}

// flushCachesIfNeeded writes full caches, i.e. caches above the size threshold, to disk.
func (w *FeatureStorageWriter) flushCachesIfNeeded() error {
	// TODO mutex needed?

	for cellExtent, nodeFeatures := range w.nodeCache {
		if len(nodeFeatures) > w.maxCacheSize {
			metadata := w.indexMetadata.getCellMetadata(cellExtent)
			startIndex := w.indexFileCursorByte

			sigolo.Debugf("Flush node cache for extent %v to disk starting at index %d", cellExtent, startIndex)

			for _, nodeFeature := range nodeFeatures {
				switch encodedFeature := nodeFeature.(type) {
				case *indexCommon.EncodedNodeFeature:
					err := w.writeNodeData(encodedFeature)
					if err != nil {
						return err
					}
				case *indexCommon.RawEncodedNodeFeature:
					err := w.writeRawNodeData(encodedFeature)
					if err != nil {
						return err
					}

				}
			}

			metadata.NodeOffsets = append(metadata.NodeOffsets, indexCellOffset{StartIndex: startIndex, EndIndex: w.indexFileCursorByte})
			w.nodeCache[cellExtent] = make([]feature.NodeFeature, 0)
		}
	}

	for cellExtent, wayFeatures := range w.wayCache {
		if len(wayFeatures) > w.maxCacheSize {
			metadata := w.indexMetadata.getCellMetadata(cellExtent)
			startIndex := w.indexFileCursorByte

			sigolo.Debugf("Flush way cache for extent %v to disk starting at index %d", cellExtent, startIndex)

			for _, wayFeature := range wayFeatures {
				switch encodedFeature := wayFeature.(type) {
				case *indexCommon.EncodedWayFeature:
					err := w.writeWayData(encodedFeature)
					if err != nil {
						return err
					}
				case *indexCommon.RawEncodedWayFeature:
					err := w.writeRawWayData(encodedFeature)
					if err != nil {
						return err
					}

				}
			}

			metadata.WayOffsets = append(metadata.WayOffsets, indexCellOffset{StartIndex: startIndex, EndIndex: w.indexFileCursorByte})
			w.wayCache[cellExtent] = make([]feature.WayFeature, 0)
		}
	}

	for cellExtent, relationFeatures := range w.relationCache {
		if len(relationFeatures) > w.maxCacheSize {
			metadata := w.indexMetadata.getCellMetadata(cellExtent)
			startIndex := w.indexFileCursorByte

			sigolo.Debugf("Flush relation cache for extent %v to disk starting at index %d", cellExtent, startIndex)

			for _, relationFeature := range relationFeatures {
				switch encodedFeature := relationFeature.(type) {
				case *indexCommon.EncodedRelationFeature:
					err := w.writeRelationData(encodedFeature)
					if err != nil {
						return err
					}
				case *indexCommon.RawEncodedRelationFeature:
					err := w.writeRawRelationData(encodedFeature)
					if err != nil {
						return err
					}

				}
			}

			metadata.RelationOffsets = append(metadata.RelationOffsets, indexCellOffset{StartIndex: startIndex, EndIndex: w.indexFileCursorByte})
			w.relationCache[cellExtent] = make([]feature.RelationFeature, 0)
		}
	}

	return nil
}

// FlushData writes all dirty caches to disk.
func (w *FeatureStorageWriter) FlushData() error {
	// TODO mutex needed?
	// TODO extract logic and reuse in flushCachesIfNeeded

	for cellExtent, nodeFeatures := range w.nodeCache {
		metadata := w.indexMetadata.getCellMetadata(cellExtent)
		startIndex := w.indexFileCursorByte

		for _, nodeFeature := range nodeFeatures {
			switch encodedFeature := nodeFeature.(type) {
			case *indexCommon.EncodedNodeFeature:
				err := w.writeNodeData(encodedFeature)
				if err != nil {
					return err
				}
			case *indexCommon.RawEncodedNodeFeature:
				err := w.writeRawNodeData(encodedFeature)
				if err != nil {
					return err
				}

			}
		}

		metadata.NodeOffsets = append(metadata.NodeOffsets, indexCellOffset{StartIndex: startIndex, EndIndex: w.indexFileCursorByte})
	}

	for cellExtent, wayFeatures := range w.wayCache {
		metadata := w.indexMetadata.getCellMetadata(cellExtent)
		startIndex := w.indexFileCursorByte

		for _, wayFeature := range wayFeatures {
			switch encodedFeature := wayFeature.(type) {
			case *indexCommon.EncodedWayFeature:
				err := w.writeWayData(encodedFeature)
				if err != nil {
					return err
				}
			case *indexCommon.RawEncodedWayFeature:
				err := w.writeRawWayData(encodedFeature)
				if err != nil {
					return err
				}

			}
		}

		metadata.WayOffsets = append(metadata.WayOffsets, indexCellOffset{StartIndex: startIndex, EndIndex: w.indexFileCursorByte})
	}

	for cellExtent, relationFeatures := range w.relationCache {
		metadata := w.indexMetadata.getCellMetadata(cellExtent)
		startIndex := w.indexFileCursorByte

		for _, relationFeature := range relationFeatures {
			switch encodedFeature := relationFeature.(type) {
			case *indexCommon.EncodedRelationFeature:
				err := w.writeRelationData(encodedFeature)
				if err != nil {
					return err
				}
			case *indexCommon.RawEncodedRelationFeature:
				err := w.writeRawRelationData(encodedFeature)
				if err != nil {
					return err
				}

			}
		}

		metadata.RelationOffsets = append(metadata.RelationOffsets, indexCellOffset{StartIndex: startIndex, EndIndex: w.indexFileCursorByte})
	}

	sigolo.Debugf("Write index metadata to %s", w.indexMetadataFileName)
	metadataJson, err := json.Marshal(w.indexMetadata)
	if err != nil {
		return err
	}
	return os.WriteFile(w.indexMetadataFileName, metadataJson, 0644)
}

func (w *FeatureStorageWriter) writeNodeData(encodedFeature *indexCommon.EncodedNodeFeature) error {
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

func (w *FeatureStorageWriter) writeRawNodeData(encodedFeature *indexCommon.RawEncodedNodeFeature) error {
	/*
		Entry format: See "writeNodeData".

		We basically use the raw byte array of the feature, change the number of ways and relations, append these to
		the data and then write it to disk.
	*/

	wayIds := encodedFeature.GetWayIds()
	relationIds := encodedFeature.GetRelationIds()

	featureData := encodedFeature.GetData()

	data := make([]byte, len(featureData)+len(wayIds)*4+len(relationIds)*4)

	// Copy existing data
	copy(data[0:], featureData)

	// Update the amounts of way- and relation-IDs
	binary.LittleEndian.PutUint16(data[8+4+4+2+2:], uint16(len(wayIds)))
	binary.LittleEndian.PutUint16(data[8+4+4+2+2+2:], uint16(len(relationIds)))

	pos := len(featureData)

	/*
		Write way-IDs
	*/
	for _, wayId := range wayIds {
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

func (w *FeatureStorageWriter) writeWayData(encodedFeature *indexCommon.EncodedWayFeature) error {
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

func (w *FeatureStorageWriter) writeRawWayData(encodedFeature *indexCommon.RawEncodedWayFeature) error {
	/*
		Entry format: See "writeWayData".

		We basically use the raw byte array of the feature, change the number of relations, append these to
		the data and then write it to disk.
	*/

	relationIds := encodedFeature.GetRelationIds()

	featureData := encodedFeature.GetData()

	data := make([]byte, len(featureData)+len(relationIds)*4)

	// Copy existing data
	copy(data[0:], featureData)

	// Update the amounts of relation-IDs
	binary.LittleEndian.PutUint16(data[8+2+2+2:], uint16(len(relationIds)))

	pos := len(featureData)

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

func (w *FeatureStorageWriter) writeRelationData(encodedFeature *indexCommon.EncodedRelationFeature) error {
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

	geometry := encodedFeature.GetGeometry()
	var bbox orb.Bound
	if geometry != nil {
		bbox = geometry.Bound()
	} else {
		bbox = orb.Bound{
			Min: orb.Point{0, 0},
			Max: orb.Point{0, 0},
		}
	}

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

func (w *FeatureStorageWriter) writeRawRelationData(encodedFeature *indexCommon.RawEncodedRelationFeature) error {
	/*
		Entry format: See "writeRelationData".

		We basically use the raw byte array of the feature, change the number of parent-relations, append these to
		the data and then write it to disk.
	*/

	parentRelationIds := encodedFeature.GetParentRelationIds()

	featureData := encodedFeature.GetData()

	data := make([]byte, len(featureData)+len(parentRelationIds)*4)

	// Copy existing data
	copy(data[0:], featureData)

	// Update the amounts of parent-relation-IDs
	binary.LittleEndian.PutUint16(data[8+16+2+2+2+2+2:], uint16(len(parentRelationIds)))

	pos := len(featureData)

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

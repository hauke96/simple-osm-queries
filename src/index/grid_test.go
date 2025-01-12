package index

import (
	"bytes"
	"encoding/binary"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"io"
	"math"
	"soq/common"
	"soq/feature"
	"sync"
	"testing"
)

func TestGridIndex_writeNodeData(t *testing.T) {
	// Arrange
	gridIndex := &GridIndexWriter{
		BaseGridIndex: BaseGridIndex{
			TagIndex:   nil,
			CellWidth:  10,
			CellHeight: 10,
			BaseFolder: "foobar",
		},
		cacheFileMutexes: map[io.Writer]*sync.Mutex{},
		cacheFileMutex:   &sync.Mutex{},
	}

	var geometry orb.Geometry
	geometry = &orb.Point{1.23, 2.34}
	encodedFeature := &feature.EncodedNodeFeature{
		AbstractEncodedFeature: feature.AbstractEncodedFeature{
			ID:       123,
			Geometry: geometry,
			Keys:     []byte{73, 0, 0}, // LittleEndian: 1001 0010
			Values:   []int{5, 1, 9},   // One value per "1" in "keys"
		},
		WayIds: []osm.WayID{12, 23},
	}
	osmId := osm.NodeID(123)

	f := bytes.NewBuffer([]byte{})
	gridIndex.cacheFileMutexes[f] = &sync.Mutex{}

	// Act
	err := gridIndex.writeNodeData(encodedFeature, f)

	// Assert
	common.AssertNil(t, err)

	data := f.Bytes()
	common.AssertEqual(t, uint64(osmId), binary.LittleEndian.Uint64(data[0:]))

	common.AssertApprox(t, geometry.(*orb.Point).Lon(), float64(math.Float32frombits(binary.LittleEndian.Uint32(data[8:]))), 0.00001)
	common.AssertApprox(t, geometry.(*orb.Point).Lat(), float64(math.Float32frombits(binary.LittleEndian.Uint32(data[12:]))), 0.00001)

	common.AssertEqual(t, uint32(1), binary.LittleEndian.Uint32(data[16:]))
	common.AssertEqual(t, uint32(3), binary.LittleEndian.Uint32(data[20:]))
	common.AssertEqual(t, uint16(2), binary.LittleEndian.Uint16(data[24:]))

	common.AssertEqual(t, encodedFeature.Keys[0], data[28])

	p := 28 + 1
	common.AssertEqual(t, encodedFeature.Values[0], int(uint32(data[p])|uint32(data[p+1])<<8|uint32(data[p+2])<<16))
	p += 3
	common.AssertEqual(t, encodedFeature.Values[1], int(uint32(data[p])|uint32(data[p+1])<<8|uint32(data[p+2])<<16))
	p += 3
	common.AssertEqual(t, encodedFeature.Values[2], int(uint32(data[p])|uint32(data[p+1])<<8|uint32(data[p+2])<<16))

	p = 38
	common.AssertEqual(t, encodedFeature.WayIds[0], osm.WayID(binary.LittleEndian.Uint64(data[p:])))
	p += 8
	common.AssertEqual(t, encodedFeature.WayIds[1], osm.WayID(binary.LittleEndian.Uint64(data[p:])))

	common.AssertEqual(t, 54, len(data))
}

func TestGridIndex_readFeaturesFromCellData(t *testing.T) {
	// Arrange
	gridIndexWriter := &GridIndexWriter{
		BaseGridIndex: BaseGridIndex{
			TagIndex: &TagIndex{
				BaseFolder: "",
				keyMap:     []string{"k1", "k2", "k3"},
				valueMap: [][]string{ // Indices must match the bit-string on the feature: 1001 0010
					{"v1_1"}, // Index 0
					{},
					{},
					{"v2_1", "v2_2"}, // Index 3
					{},
					{},
					{"v3_1", "v3_2"}, // Index 6
				},
				keyReverseMap:   nil,
				valueReverseMap: nil,
			},
			CellWidth:  10,
			CellHeight: 10,
			BaseFolder: "foobar",
		},
		cacheFileMutexes: map[io.Writer]*sync.Mutex{},
		cacheFileMutex:   &sync.Mutex{},
	}
	gridIndexReader := &GridIndexReader{
		BaseGridIndex: BaseGridIndex{
			TagIndex:   nil,
			CellWidth:  10,
			CellHeight: 10,
			BaseFolder: "foobar",
		},
	}

	geometry := &orb.Point{1.23, 2.34}
	originalFeature := &feature.EncodedNodeFeature{
		AbstractEncodedFeature: feature.AbstractEncodedFeature{
			ID:       123,
			Geometry: geometry,
			Keys:     []byte{73, 0, 0}, // LittleEndian: 1001 0010
			Values:   []int{0, 1, 0},   // One value per "1" in "keys"
		},
	}

	f := bytes.NewBuffer([]byte{})
	gridIndexWriter.cacheFileMutexes[f] = &sync.Mutex{}

	err := gridIndexWriter.writeNodeData(originalFeature, f)
	common.AssertNil(t, err)

	outputChannel := make(chan []feature.Feature)
	var result []feature.Feature

	// Act
	go func() {
		for features := range outputChannel {
			result = append(result, features...)
		}
	}()
	gridIndexReader.readNodesFromCellData(outputChannel, f.Bytes())
	close(outputChannel)

	// Assert
	common.AssertNotNil(t, result[0])
	for i := 1; i < len(result); i++ {
		if result[i] != nil {
			t.Fail()
		}
	}

	encodedFeature := result[0]
	common.AssertEqual(t, 1, len(encodedFeature.GetKeys()))
	common.AssertEqual(t, originalFeature.Keys[0], encodedFeature.GetKeys()[0])
	common.AssertEqual(t, originalFeature.Values, encodedFeature.GetValues())
	common.AssertApprox(t, originalFeature.GetGeometry().(*orb.Point).Lon(), encodedFeature.GetGeometry().(*orb.Point).Lon(), 0.0001)
	common.AssertApprox(t, originalFeature.GetGeometry().(*orb.Point).Lat(), encodedFeature.GetGeometry().(*orb.Point).Lat(), 0.0001)
}

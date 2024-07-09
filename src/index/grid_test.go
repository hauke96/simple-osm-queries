package index

import (
	"bytes"
	"encoding/binary"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"io"
	"math"
	"soq/feature"
	"soq/util"
	"sync"
	"testing"
)

func TestGridIndex_writeNodeData(t *testing.T) {
	// Arrange
	gridIndex := &baseGridIndex{
		TagIndex:         nil,
		CellWidth:        10,
		CellHeight:       10,
		BaseFolder:       "foobar",
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
	util.AssertNil(t, err)

	data := f.Bytes()
	util.AssertEqual(t, uint64(osmId), binary.LittleEndian.Uint64(data[0:]))

	util.AssertApprox(t, geometry.(*orb.Point).Lon(), float64(math.Float32frombits(binary.LittleEndian.Uint32(data[8:]))), 0.00001)
	util.AssertApprox(t, geometry.(*orb.Point).Lat(), float64(math.Float32frombits(binary.LittleEndian.Uint32(data[12:]))), 0.00001)

	util.AssertEqual(t, uint32(1), binary.LittleEndian.Uint32(data[16:]))
	util.AssertEqual(t, uint32(3), binary.LittleEndian.Uint32(data[20:]))
	util.AssertEqual(t, uint16(2), binary.LittleEndian.Uint16(data[24:]))

	util.AssertEqual(t, encodedFeature.Keys[0], data[26])

	p := 26 + 1
	util.AssertEqual(t, encodedFeature.Values[0], int(uint32(data[p])|uint32(data[p+1])<<8|uint32(data[p+2])<<16))
	p += 3
	util.AssertEqual(t, encodedFeature.Values[1], int(uint32(data[p])|uint32(data[p+1])<<8|uint32(data[p+2])<<16))
	p += 3
	util.AssertEqual(t, encodedFeature.Values[2], int(uint32(data[p])|uint32(data[p+1])<<8|uint32(data[p+2])<<16))

	p = 36
	util.AssertEqual(t, encodedFeature.WayIds[0], osm.WayID(binary.LittleEndian.Uint64(data[p:])))
	p += 8
	util.AssertEqual(t, encodedFeature.WayIds[1], osm.WayID(binary.LittleEndian.Uint64(data[p:])))

	util.AssertEqual(t, 52, len(data))
}

func TestGridIndex_readFeaturesFromCellData(t *testing.T) {
	// Arrange
	gridIndex := &baseGridIndex{
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
		CellWidth:        10,
		CellHeight:       10,
		BaseFolder:       "foobar",
		cacheFileMutexes: map[io.Writer]*sync.Mutex{},
		cacheFileMutex:   &sync.Mutex{},
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
	gridIndex.cacheFileMutexes[f] = &sync.Mutex{}

	err := gridIndex.writeNodeData(originalFeature, f)
	util.AssertNil(t, err)

	outputChannel := make(chan []feature.EncodedFeature)
	var result []feature.EncodedFeature

	// Act
	go func() {
		for features := range outputChannel {
			result = append(result, features...)
		}
	}()
	gridIndex.readNodesFromCellData(outputChannel, f.Bytes())
	close(outputChannel)

	// Assert
	util.AssertNotNil(t, result[0])
	for i := 1; i < len(result); i++ {
		if result[i] != nil {
			t.Fail()
		}
	}

	encodedFeature := result[0]
	util.AssertEqual(t, 1, len(encodedFeature.GetKeys()))
	util.AssertEqual(t, originalFeature.Keys[0], encodedFeature.GetKeys()[0])
	util.AssertEqual(t, originalFeature.Values, encodedFeature.GetValues())
	util.AssertApprox(t, originalFeature.GetGeometry().(*orb.Point).Lon(), encodedFeature.GetGeometry().(*orb.Point).Lon(), 0.0001)
	util.AssertApprox(t, originalFeature.GetGeometry().(*orb.Point).Lat(), encodedFeature.GetGeometry().(*orb.Point).Lat(), 0.0001)
}

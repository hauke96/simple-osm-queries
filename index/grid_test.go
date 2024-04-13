package index

import (
	"bytes"
	"encoding/binary"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
	"math"
	"soq/util"
	"testing"
)

func TestGridIndex_writeNodeData(t *testing.T) {
	// Arrange
	gridIndex := &GridIndex{
		TagIndex:   nil,
		CellWidth:  10,
		CellHeight: 10,
		BaseFolder: "foobar",
	}

	geometry := &orb.Point{1.23, 2.34}
	feature := &EncodedFeature{
		Geometry: geometry,
		keys:     []byte{73, 0, 0}, // LittleEndian: 1001 0010
		values:   []int{5, 1, 9},   // One value per "1" in "keys"
	}
	osmId := osm.NodeID(123)

	f := bytes.NewBuffer([]byte{})

	// Act
	err := gridIndex.writeNodeData(osmId, feature, f)

	// Assert
	util.AssertNil(t, err)

	data := f.Bytes()
	util.AssertEqual(t, uint64(osmId), binary.LittleEndian.Uint64(data[0:]))

	util.AssertApprox(t, geometry.Lon(), float64(math.Float32frombits(binary.LittleEndian.Uint32(data[8:]))), 0.00001)
	util.AssertApprox(t, geometry.Lat(), float64(math.Float32frombits(binary.LittleEndian.Uint32(data[12:]))), 0.00001)

	util.AssertEqual(t, uint32(1), binary.LittleEndian.Uint32(data[16:]))
	util.AssertEqual(t, uint32(3), binary.LittleEndian.Uint32(data[20:]))

	util.AssertEqual(t, feature.keys[0], data[24])

	p := 24 + 1
	util.AssertEqual(t, feature.values[0], int(uint32(data[p])|uint32(data[p+1])<<8|uint32(data[p+2])<<16))
	p += 3
	util.AssertEqual(t, feature.values[1], int(uint32(data[p])|uint32(data[p+1])<<8|uint32(data[p+2])<<16))
	p += 3
	util.AssertEqual(t, feature.values[2], int(uint32(data[p])|uint32(data[p+1])<<8|uint32(data[p+2])<<16))

	util.AssertEqual(t, 34, len(data))
}

func TestGridIndex_readFeaturesFromCellData(t *testing.T) {
	// Arrange
	gridIndex := &GridIndex{
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
	}

	geometry := &orb.Point{1.23, 2.34}
	originalFeature := &EncodedFeature{
		ID:       123,
		Geometry: geometry,
		keys:     []byte{73, 0, 0}, // LittleEndian: 1001 0010
		values:   []int{0, 1, 0},   // One value per "1" in "keys"
	}
	osmId := osm.NodeID(123)

	f := bytes.NewBuffer([]byte{})

	err := gridIndex.writeNodeData(osmId, originalFeature, f)
	util.AssertNil(t, err)

	outputChannel := make(chan []*EncodedFeature)
	var result []*EncodedFeature

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

	feature := result[0]
	util.AssertEqual(t, 1, len(feature.keys))
	util.AssertEqual(t, originalFeature.keys[0], feature.keys[0])
	util.AssertEqual(t, originalFeature.values, feature.values)
	util.AssertApprox(t, originalFeature.Geometry.(*orb.Point).Lon(), feature.Geometry.(*orb.Point).Lon(), 0.0001)
	util.AssertApprox(t, originalFeature.Geometry.(*orb.Point).Lat(), feature.Geometry.(*orb.Point).Lat(), 0.0001)
}

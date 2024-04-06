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

func TestGridIndex_writeNodeBytes(t *testing.T) {
	// Arrange
	gridIndex := &GridIndex{
		TagIndex:   nil,
		CellWidth:  10,
		CellHeight: 10,
		BaseFolder: "foobar",
	}

	geometry := orb.Point{1.23, 2.34}
	feature := &EncodedFeature{
		geometry: geometry,
		keys:     []byte{73},     // LittleEndian: 1001 0010
		values:   []int{5, 1, 9}, // One value per "1" in "keys"
	}
	osmId := osm.NodeID(123)

	f := bytes.NewBuffer([]byte{})

	// Act
	err := gridIndex.writeNodeData(osmId, feature, f)

	// Assert
	util.AssertNil(t, err)

	data := f.Bytes()
	util.AssertEqual(t, uint64(osmId), binary.LittleEndian.Uint64(data[0:]))

	util.AssertEqual(t, geometry.Lon(), math.Float64frombits(binary.LittleEndian.Uint64(data[8:])))
	util.AssertEqual(t, geometry.Lat(), math.Float64frombits(binary.LittleEndian.Uint64(data[16:])))

	util.AssertEqual(t, uint32(len(feature.keys)), binary.LittleEndian.Uint32(data[24:]))
	util.AssertEqual(t, uint32(len(feature.values)), binary.LittleEndian.Uint32(data[28:]))

	util.AssertEqual(t, feature.keys[0], data[32])
	util.AssertEqual(t, feature.values[0], int(binary.LittleEndian.Uint32(data[33:])))
	util.AssertEqual(t, feature.values[1], int(binary.LittleEndian.Uint32(data[37:])))
	util.AssertEqual(t, feature.values[2], int(binary.LittleEndian.Uint32(data[41:])))

	util.AssertEqual(t, 45, len(data))
}

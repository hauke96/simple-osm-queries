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

	geometry := orb.Point{1.23, 2.34}
	feature := &EncodedFeature{
		Geometry: geometry,
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

	util.AssertApprox(t, geometry.Lon(), float64(math.Float32frombits(binary.LittleEndian.Uint32(data[8:]))), 0.00001)
	util.AssertApprox(t, geometry.Lat(), float64(math.Float32frombits(binary.LittleEndian.Uint32(data[12:]))), 0.00001)

	util.AssertEqual(t, uint32(len(feature.keys)), binary.LittleEndian.Uint32(data[16:]))
	util.AssertEqual(t, uint32(len(feature.values)), binary.LittleEndian.Uint32(data[20:]))

	util.AssertEqual(t, feature.keys[0], data[24])

	p := 24 + 1
	util.AssertEqual(t, feature.values[0], int(uint32(data[p])|uint32(data[p+1])<<8|uint32(data[p+2])<<16))
	p += 3
	util.AssertEqual(t, feature.values[1], int(uint32(data[p])|uint32(data[p+1])<<8|uint32(data[p+2])<<16))
	p += 3
	util.AssertEqual(t, feature.values[2], int(uint32(data[p])|uint32(data[p+1])<<8|uint32(data[p+2])<<16))

	util.AssertEqual(t, 34, len(data))
}

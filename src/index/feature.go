package index

import (
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
)

type AbstractEncodedFeature struct {
	ID uint64

	Geometry orb.Geometry

	// A bit-string defining which keys are set and which aren't. A 1 at index i says that the key with numeric
	// representation i is set.
	Keys []byte

	// A list of all set values. The i-th entry corresponds to the i-th 1 in the keys bit-string and holds the numeric
	// representation of the value. This means the amount of entries in this array is equal to the amount of ones in
	// the keys bit-string.
	Values []int
}

func (f *AbstractEncodedFeature) GetID() uint64 {
	return f.ID
}

func (f *AbstractEncodedFeature) GetGeometry() orb.Geometry {
	return f.Geometry
}

func (f *AbstractEncodedFeature) GetKeys() []byte {
	return f.Keys
}

func (f *AbstractEncodedFeature) GetValues() []int {
	return f.Values
}

func (f *AbstractEncodedFeature) HasKey(keyIndex int) bool {
	if keyIndex == -1 {
		return false
	}

	bin := keyIndex / 8 // Element of the array

	// In case a key is requested that not even exists in the current bin, then if course the key is not set.
	if bin > len(f.GetKeys())-1 {
		return false
	}

	idxInBin := keyIndex % 8 // Bit position within the byte
	return f.GetKeys()[bin]&(1<<idxInBin) != 0
}

// GetValueIndex returns the value index (numerical representation of the actual value) for a given key index. This
// function assumes that the key is set on the feature. Use featureHasKey to check this.
func (f *AbstractEncodedFeature) GetValueIndex(keyIndex int) int {
	// Go through all bits to count the number of 1's.
	// TODO This can probably be optimised by preprocessing this (i.e. map from keyIndex to position in values array)
	valueIndexPosition := 0
	for i := 0; i < keyIndex; i++ {
		bin := i / 8      // Element of the array
		idxInBin := i % 8 // Bit position within the byte
		if f.GetKeys()[bin]&(1<<idxInBin) != 0 {
			// Key at "i" is set -> store its value
			valueIndexPosition++
		}
	}

	return f.GetValues()[valueIndexPosition]
}

func (f *AbstractEncodedFeature) HasTag(keyIndex int, valueIndex int) bool {
	if !f.HasKey(keyIndex) {
		return false
	}

	return f.GetValueIndex(keyIndex) == valueIndex
}

func (f *AbstractEncodedFeature) Print() {
	if !sigolo.ShouldLogTrace() {
		return
	}

	sigolo.Tracef("Feature:")
	sigolo.Tracef("  id=%d", f.GetID())
	sigolo.Tracef("  keys=%v", f.GetKeys())
	var setKeyBits []int
	for i := 0; i < len(f.GetKeys())*8; i++ {
		if f.HasKey(i) {
			setKeyBits = append(setKeyBits, i)
		}
	}
	sigolo.Tracef("  set key bit positions=%v", setKeyBits)
	sigolo.Tracef("  values=%v", f.GetValues())
}

type EncodedNodeFeature struct {
	AbstractEncodedFeature
	WayIds      []osm.WayID      // An ID list of all ways this node is part of.
	RelationIds []osm.RelationID // An ID list of all relations this node is part of.
}

func (f *EncodedNodeFeature) GetWayIds() []osm.WayID {
	return f.WayIds
}

func (f *EncodedNodeFeature) SetWayIds(wayIds []osm.WayID) {
	f.WayIds = wayIds
}

func (f *EncodedNodeFeature) GetRelationIds() []osm.RelationID {
	return f.RelationIds
}

func (f *EncodedNodeFeature) SetRelationIds(relationIds []osm.RelationID) {
	f.RelationIds = relationIds
}

func (f *EncodedNodeFeature) GetLon() float64 {
	return f.Geometry.(*orb.Point).Lon()
}

func (f *EncodedNodeFeature) GetLat() float64 {
	return f.Geometry.(*orb.Point).Lat()
}

type EncodedWayFeature struct {
	AbstractEncodedFeature
	Nodes       osm.WayNodes     // A list of all nodes of the way. These nodes only contain their ID, lat and lon.
	RelationIds []osm.RelationID // An ID list of all relations this way is part of.
}

func (f *EncodedWayFeature) GetNodes() osm.WayNodes {
	return f.Nodes
}

func (f *EncodedWayFeature) GetRelationIds() []osm.RelationID {
	return f.RelationIds
}

func (f *EncodedWayFeature) SetRelationIds(relationIds []osm.RelationID) {
	f.RelationIds = relationIds
}

type EncodedRelationFeature struct {
	AbstractEncodedFeature
	NodeIds           []osm.NodeID
	WayIds            []osm.WayID
	ChildRelationIds  []osm.RelationID
	ParentRelationIds []osm.RelationID
}

func (f *EncodedRelationFeature) GetNodeIds() []osm.NodeID {
	return f.NodeIds
}

func (f *EncodedRelationFeature) GetWayIds() []osm.WayID {
	return f.WayIds
}

func (f *EncodedRelationFeature) GetChildRelationIds() []osm.RelationID {
	return f.ChildRelationIds
}

func (f *EncodedRelationFeature) GetParentRelationIds() []osm.RelationID {
	return f.ParentRelationIds
}

func (f *EncodedRelationFeature) SetParentRelationIds(relationIds []osm.RelationID) {
	f.ParentRelationIds = relationIds
}

func (f *EncodedRelationFeature) SetGeometry(geometry orb.Geometry) {
	f.Geometry = geometry
}

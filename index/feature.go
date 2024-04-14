package index

import (
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/osm"
	"github.com/pkg/errors"
	"io"
	"os"
	"time"
)

type EncodedFeature interface {
	GetID() uint64
	GetGeometry() orb.Geometry
	getKeys() []byte
	getValues() []int

	HasKey(keyIndex int) bool
	GetValueIndex(keyIndex int) int
	HasTag(keyIndex int, valueIndex int) bool
	Print()
}

type AbstractEncodedFeature struct {
	ID uint64

	Geometry orb.Geometry

	// A bit-string defining which keys are set and which aren't. A 1 at index i says that the key with numeric
	// representation i is set.
	keys []byte

	// A list of all set values. The i-th entry corresponds to the i-th 1 in the keys bit-string and holds the numeric
	// representation of the value. This means the amount of entries in this array is equal to the amount of ones in
	// the keys bit-string.
	values []int
}

func featureHasKey(f EncodedFeature, keyIndex int) bool {
	if keyIndex == -1 {
		return false
	}

	bin := keyIndex / 8 // Element of the array

	// In case a key is requested that not even exists in the current bin, then if course the key is not set.
	if bin > len(f.getKeys())-1 {
		return false
	}

	idxInBin := keyIndex % 8 // Bit position within the byte
	return f.getKeys()[bin]&(1<<idxInBin) != 0
}

// FeatureGetValueIndex returns the value index (numerical representation of the actual value) for a given key index. This
// function assumes that the key is set on the feature. Use HasKey to check this.
func featureGetValueIndex(f EncodedFeature, keyIndex int) int {
	// Go through all bits to count the number of 1's.
	// TODO This can probably be optimised by preprocessing this (i.e. map from keyIndex to position in values array)
	valueIndexPosition := 0
	for i := 0; i < keyIndex; i++ {
		bin := i / 8      // Element of the array
		idxInBin := i % 8 // Bit position within the byte
		if f.getKeys()[bin]&(1<<idxInBin) != 0 {
			// Key at "i" is set -> store its value
			valueIndexPosition++
		}
	}

	return f.getValues()[valueIndexPosition]
}

func featureHasTag(f EncodedFeature, keyIndex int, valueIndex int) bool {
	if !f.HasKey(keyIndex) {
		return false
	}

	return f.GetValueIndex(keyIndex) == valueIndex
}

func featurePrint(f EncodedFeature) {
	if !sigolo.ShouldLogTrace() {
		return
	}

	sigolo.Tracef("EncodedFeature:")
	sigolo.Tracef("  id=%d", f.GetID())
	sigolo.Tracef("  keys=%v", f.getKeys())
	var setKeyBits []int
	for i := 0; i < len(f.getKeys())*8; i++ {
		if f.HasKey(i) {
			setKeyBits = append(setKeyBits, i)
		}
	}
	sigolo.Tracef("  set key bit positions=%v", setKeyBits)
	sigolo.Tracef("  values=%v", f.getValues())
}

type EncodedNodeFeature struct {
	AbstractEncodedFeature
}

func (f EncodedNodeFeature) GetID() uint64 {
	return f.ID
}

func (f EncodedNodeFeature) GetGeometry() orb.Geometry {
	return f.Geometry
}

func (f EncodedNodeFeature) getKeys() []byte {
	return f.keys
}

func (f EncodedNodeFeature) getValues() []int {
	return f.values
}

func (f EncodedNodeFeature) HasKey(keyIndex int) bool {
	return featureHasKey(f, keyIndex)
}

func (f EncodedNodeFeature) GetValueIndex(keyIndex int) int {
	return featureGetValueIndex(f, keyIndex)
}

func (f EncodedNodeFeature) HasTag(keyIndex int, valueIndex int) bool {
	return featureHasTag(f, keyIndex, valueIndex)
}

func (f EncodedNodeFeature) Print() {
	featurePrint(f)
}

type EncodedWayFeature struct {
	AbstractEncodedFeature

	// A list of all nodes of the way. These nodes only contain their ID, lat and lon.
	nodes osm.WayNodes
}

func (f EncodedWayFeature) GetNodes() osm.WayNodes {
	return f.nodes
}

func (f EncodedWayFeature) GetID() uint64 {
	return f.ID
}

func (f EncodedWayFeature) GetGeometry() orb.Geometry {
	return f.Geometry
}

func (f EncodedWayFeature) getKeys() []byte {
	return f.keys
}

func (f EncodedWayFeature) getValues() []int {
	return f.values
}

func (f EncodedWayFeature) HasKey(keyIndex int) bool {
	return featureHasKey(f, keyIndex)
}

func (f EncodedWayFeature) GetValueIndex(keyIndex int) int {
	return featureGetValueIndex(f, keyIndex)
}

func (f EncodedWayFeature) HasTag(keyIndex int, valueIndex int) bool {
	return featureHasTag(f, keyIndex, valueIndex)
}

func (f EncodedWayFeature) Print() {
	featurePrint(f)
}

func WriteFeaturesAsGeoJsonFile(encodedFeatures []EncodedFeature, tagIndex *TagIndex) error {
	file, err := os.Create("output.geojson")
	if err != nil {
		return err
	}

	defer func() {
		err = file.Close()
		sigolo.FatalCheck(errors.Wrapf(err, "Unable to close file handle for GeoJSON file %s", file.Name()))
	}()

	return WriteFeaturesAsGeoJson(encodedFeatures, tagIndex, file)
}

func WriteFeaturesAsGeoJson(encodedFeatures []EncodedFeature, tagIndex *TagIndex, writer io.Writer) error {
	sigolo.Info("Write features to GeoJSON")
	writeStartTime := time.Now()

	featureCollection := geojson.NewFeatureCollection()
	for _, encodedFeature := range encodedFeatures {
		feature := geojson.NewFeature(encodedFeature.GetGeometry())

		feature.Properties["osm_id"] = encodedFeature.GetID()
		for keyIndex := 0; keyIndex < len(encodedFeature.getKeys())*8; keyIndex++ {
			if !encodedFeature.HasKey(keyIndex) {
				continue
			}

			valueIndex := encodedFeature.GetValueIndex(keyIndex)

			keyString := tagIndex.GetKeyFromIndex(keyIndex)
			valueString := tagIndex.GetValueForKey(keyIndex, valueIndex)

			feature.Properties[keyString] = valueString
		}

		featureCollection.Features = append(featureCollection.Features, feature)
	}

	geojsonBytes, err := featureCollection.MarshalJSON()
	if err != nil {
		return err
	}

	_, err = writer.Write(geojsonBytes)
	if err != nil {
		return err
	}

	queryDuration := time.Since(writeStartTime)
	sigolo.Infof("Finished writing in %s", queryDuration)

	return nil
}

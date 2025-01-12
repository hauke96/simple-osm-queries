package index

import (
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/orb/geojson"
	"github.com/pkg/errors"
	"io"
	"os"
	"soq/feature"
	"time"
)

func WriteFeaturesAsGeoJsonFile(encodedFeatures []feature.Feature, tagIndex *TagIndex) error {
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

func WriteFeaturesAsGeoJson(encodedFeatures []feature.Feature, tagIndex *TagIndex, writer io.Writer) error {
	sigolo.Info("Write features to GeoJSON")
	writeStartTime := time.Now()

	featureCollection := geojson.NewFeatureCollection()
	for _, encodedFeature := range encodedFeatures {
		geoJsonFeature := geojson.NewFeature(encodedFeature.GetGeometry())

		geoJsonFeature.Properties["@osm_id"] = encodedFeature.GetID()

		switch encodedFeature.(type) {
		case feature.NodeFeature:
			geoJsonFeature.Properties["@osm_type"] = "node"
		case feature.WayFeature:
			geoJsonFeature.Properties["@osm_type"] = "way"
		case feature.RelationFeature:
			geoJsonFeature.Properties["@osm_type"] = "relation"
		}

		for keyIndex := 0; keyIndex < len(encodedFeature.GetKeys())*8; keyIndex++ {
			if !encodedFeature.HasKey(keyIndex) {
				continue
			}

			valueIndex := encodedFeature.GetValueIndex(keyIndex)

			keyString := tagIndex.GetKeyFromIndex(keyIndex)
			valueString := tagIndex.GetValueForKey(keyIndex, valueIndex)

			geoJsonFeature.Properties[keyString] = valueString
		}

		featureCollection.Features = append(featureCollection.Features, geoJsonFeature)
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

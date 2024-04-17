package query

import (
	"github.com/hauke96/sigolo/v2"
	"soq/feature"
	"soq/index"
)

type Statement struct {
	location   LocationExpression
	objectType feature.OsmObjectType
	filter     FilterExpression
}

func NewStatement(locationExpression LocationExpression, objectType feature.OsmObjectType, filterExpression FilterExpression) *Statement {
	return &Statement{
		location:   locationExpression,
		objectType: objectType,
		filter:     filterExpression,
	}
}

func (s Statement) GetFeatures(context feature.EncodedFeature) (chan *index.GetFeaturesResult, error) {
	return s.location.GetFeatures(geometryIndex, context, s.objectType)
}

func (s Statement) Applies(feature feature.EncodedFeature, context feature.EncodedFeature) (bool, error) {
	// TODO Respect object type (this should also not be necessary, should it?)

	applies, err := s.filter.Applies(feature, context)
	if err != nil {
		return false, err
	}

	return applies, nil
}

func (s Statement) Execute(context feature.EncodedFeature) ([]feature.EncodedFeature, error) {
	s.Print(0)

	featuresChannel, err := s.GetFeatures(context)
	if err != nil {
		return nil, err
	}

	var result []feature.EncodedFeature

	for getFeatureResult := range featuresChannel {
		sigolo.Tracef("Received %d features from cell %v", len(getFeatureResult.Features), getFeatureResult.Cell)

		for _, feature := range getFeatureResult.Features {
			sigolo.Trace("----- next feature -----")
			if feature != nil {
				feature.Print()

				applies, err := s.Applies(feature, context)
				if err != nil {
					return nil, err
				}

				if applies {
					result = append(result, feature)
				}
			}
		}
	}

	return result, nil
}

func (s Statement) Print(indent int) {
	sigolo.Debugf("%s%s", spacing(indent), "Statement")
	s.location.Print(indent + 2)
	sigolo.Debugf("%stype: %s", spacing(indent+2), s.objectType.String())
	s.filter.Print(indent + 2)
}

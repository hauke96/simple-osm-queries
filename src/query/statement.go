package query

import (
	"github.com/hauke96/sigolo/v2"
	"soq/feature"
	"soq/index"
	"soq/osm"
)

type Statement struct {
	location  LocationExpression
	queryType osm.OsmQueryType
	filter    FilterExpression
}

func NewStatement(locationExpression LocationExpression, queryType osm.OsmQueryType, filterExpression FilterExpression) *Statement {
	return &Statement{
		location:  locationExpression,
		queryType: queryType,
		filter:    filterExpression,
	}
}

func (s Statement) GetFeatures(context feature.EncodedFeature) (chan *index.GetFeaturesResult, error) {
	return s.location.GetFeatures(geometryIndex, context, s.queryType.GetObjectType())
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
	sigolo.Debugf("%stype: %s", spacing(indent+2), s.queryType.String())
	s.filter.Print(indent + 2)
}

func (s Statement) GetFilterExpression() FilterExpression {
	return s.filter
}

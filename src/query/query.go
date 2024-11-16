package query

import (
	"github.com/hauke96/sigolo/v2"
	"soq/feature"
	"soq/index"
	"time"
)

var geometryIndex index.GeometryIndex

type Query struct {
	topLevelStatements []Statement
}

func NewQuery(topLevelStatements []Statement) *Query {
	return &Query{topLevelStatements: topLevelStatements}
}

func (q *Query) Execute(geomIndex index.GeometryIndex) ([]feature.Feature, error) {
	// TODO Refactor this, since this is just a quick and dirty way to make sub-statement access the geometry index.
	geometryIndex = geomIndex

	sigolo.Info("Start query")
	queryStartTime := time.Now()

	var result []feature.Feature

	for _, statement := range q.topLevelStatements {
		statementResult, err := statement.Execute(nil)
		if err != nil {
			return nil, err
		}
		result = append(result, statementResult...)
	}

	queryDuration := time.Since(queryStartTime)
	sigolo.Infof("Executed query in %s", queryDuration)

	return result, nil
}

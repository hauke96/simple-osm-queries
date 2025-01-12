package osm

import (
	"context"
	"github.com/hauke96/sigolo/v2"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
	"github.com/pkg/errors"
	"os"
	"time"
)

// OsmDataHandler are structs that receive OSM data and may aggregate or process the data. They should not change the
// received data! The handling functions are called in order: First the init function, then nodes, ways and relations
// and finally the done function.
type OsmDataHandler interface {
	Name() string
	Init() error
	HandleNode(node *osm.Node) error
	HandleWay(way *osm.Way) error
	HandleRelation(relation *osm.Relation) error
	Done() error
}

// OsmReader reads a given OSM PBF file and calls all given OsmDataHandler on the data.
type OsmReader struct {
	firstNodeHasBeenProcessed     bool
	firstWayHasBeenProcessed      bool
	firstRelationHasBeenProcessed bool
}

func NewOsmReader() *OsmReader {
	return &OsmReader{
		firstNodeHasBeenProcessed:     false,
		firstWayHasBeenProcessed:      false,
		firstRelationHasBeenProcessed: false,
	}
}

func (r *OsmReader) Read(filename string, handlers ...OsmDataHandler) error {
	reader, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "Unable to open OSM input file file %s", filename)
	}

	scanner := osmpbf.New(context.Background(), reader, 1)

	sigolo.Debugf("Start processing OSM data file %s", filename)
	importStartTime := time.Now()

	for _, handler := range handlers {
		err = handler.Init()
		if err != nil {
			return errors.Wrapf(err, "Initializing OSM data handler '%s' failed", handler.Name())
		}
	}

	sigolo.Debug("Start processing nodes (1/3)")
	for scanner.Scan() {
		switch osmObj := scanner.Object().(type) {
		case *osm.Node:
			for _, handler := range handlers {
				err = handler.HandleNode(osmObj)
				if err != nil {
					return errors.Wrapf(err, "Handling node %d using handler '%s' failed", osmObj.ID, handler.Name())
				}
			}
		case *osm.Way:
			if !r.firstWayHasBeenProcessed {
				sigolo.Debug("Start processing ways (2/3)")
				r.firstWayHasBeenProcessed = true
			}

			for _, handler := range handlers {
				err = handler.HandleWay(osmObj)
				if err != nil {
					return errors.Wrapf(err, "Handling way %d using handler '%s' failed", osmObj.ID, handler.Name())
				}
			}
		case *osm.Relation:
			if !r.firstRelationHasBeenProcessed {
				sigolo.Debug("Start processing relations (3/3)")
				r.firstRelationHasBeenProcessed = true
			}

			for _, handler := range handlers {
				err = handler.HandleRelation(osmObj)
				if err != nil {
					return errors.Wrapf(err, "Handling relation %d using handler '%s' failed", osmObj.ID, handler.Name())
				}
			}
		}
	}

	sigolo.Infof("Finished Processing data, start post-processing")
	for _, handler := range handlers {
		err = handler.Done()
		if err != nil {
			return errors.Wrapf(err, "Calling done function on handler '%s' failed", handler.Name())
		}
	}

	err = scanner.Close()
	if err != nil {
		return errors.Wrapf(err, "Unable to close OSM scanner")
	}

	importDuration := time.Since(importStartTime)
	sigolo.Infof("Done processing OSM data in %s", importDuration)

	return nil
}

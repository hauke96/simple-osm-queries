package osm

import (
	"github.com/paulmach/osm"
	"soq/common"
)

type OsmDensityAggregator struct {
	CellToNodeCount     map[common.CellIndex]int
	InputDataCellExtent *common.CellExtent
	cellWidth           float64
	cellHeight          float64
}

func NewOsmDensityAggregator(cellWidth float64, cellHeight float64) *OsmDensityAggregator {
	return &OsmDensityAggregator{
		cellWidth:       cellWidth,
		cellHeight:      cellHeight,
		CellToNodeCount: map[common.CellIndex]int{},
	}
}

func (a *OsmDensityAggregator) Name() string {
	return "OsmDensityAggregator"
}

func (a *OsmDensityAggregator) Init() error {
	return nil
}

func (a *OsmDensityAggregator) HandleNode(node *osm.Node) error {
	cell := common.GetCellIndexForCoordinate(node.Lon, node.Lat, a.cellWidth, a.cellHeight)
	if _, ok := a.CellToNodeCount[cell]; !ok {
		a.CellToNodeCount[cell] = 1
	} else {
		a.CellToNodeCount[cell] = a.CellToNodeCount[cell] + 1
	}

	if a.InputDataCellExtent == nil {
		a.InputDataCellExtent = &common.CellExtent{cell, cell}
	} else {
		newExtent := a.InputDataCellExtent.Expand(cell)
		a.InputDataCellExtent = &newExtent
	}

	return nil
}

func (a *OsmDensityAggregator) HandleWay(way *osm.Way) error {
	return nil
}

func (a *OsmDensityAggregator) HandleRelation(relation *osm.Relation) error {
	return nil
}

func (a *OsmDensityAggregator) Done() error {
	return nil
}

package storage

import "soq/common"

type indexMetadata struct {
	Cells []*indexCellMetadata `json:"cells"`
}

func (i indexMetadata) getCellMetadata(extent common.CellExtent) *indexCellMetadata {
	var cellMetadata *indexCellMetadata

	for _, cell := range i.Cells {
		if cell.Extent.IsEqualTo(extent) {
			cellMetadata = cell
			break
		}
	}

	if cellMetadata == nil {
		cellMetadata = &indexCellMetadata{
			Extent:          extent,
			NodeOffsets:     []int64{},
			WayOffsets:      []int64{},
			RelationOffsets: []int64{},
		}
		i.Cells = append(i.Cells, cellMetadata)
	}

	return cellMetadata
}

type indexCellMetadata struct {
	Extent          common.CellExtent `json:"extent"`
	NodeOffsets     []int64           `json:"node-offsets"`
	WayOffsets      []int64           `json:"way-offsets"`
	RelationOffsets []int64           `json:"relation-offsets"`
}

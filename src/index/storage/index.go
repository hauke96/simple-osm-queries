package storage

import "soq/common"

type indexMetadata struct {
	Cells []*indexCellMetadata `json:"cells"`
}

func (i *indexMetadata) getCellMetadata(extent common.CellExtent) *indexCellMetadata {
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
			NodeOffsets:     []indexCellOffset{},
			WayOffsets:      []indexCellOffset{},
			RelationOffsets: []indexCellOffset{},
		}
		i.Cells = append(i.Cells, cellMetadata)
	}

	return cellMetadata
}

type indexCellMetadata struct {
	Extent          common.CellExtent `json:"extent"`
	NodeOffsets     []indexCellOffset `json:"node-offsets"`
	WayOffsets      []indexCellOffset `json:"way-offsets"`
	RelationOffsets []indexCellOffset `json:"relation-offsets"`
}

type indexCellOffset struct {
	StartIndex int64 `json:"start-index"` // This is the first byte of the cell.
	EndIndex   int64 `json:"end-index"`   // This is the first byte *behind* the cell, i.e. this byte if not part of the cell anymore.
}

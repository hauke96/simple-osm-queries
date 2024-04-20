package util

import (
	"encoding/binary"
	"math"
	"testing"
)

var (
	simpleBinarySchema = BinarySchema{
		Items: []BinaryItem{
			&BinaryDataItem{FieldName: "A", BinaryType: DatatypeInt64},
			&BinaryDataItem{FieldName: "B", BinaryType: DatatypeFloat32},
		},
	}

	rawCollectionBinarySchema = BinarySchema{
		Items: []BinaryItem{
			&BinaryDataItem{FieldName: "A", BinaryType: DatatypeInt64},
			&BinaryRawCollectionItem{FieldName: "B", BinaryType: DatatypeByte},
			&BinaryRawCollectionItem{FieldName: "C", BinaryType: DatatypeInt24},
		},
	}

	collectionBinarySchema = BinarySchema{
		Items: []BinaryItem{
			&BinaryDataItem{FieldName: "A", BinaryType: DatatypeInt64},
			&BinaryCollectionItem{
				FieldName: "B",
				ItemSchema: BinarySchema{
					Items: []BinaryItem{
						&BinaryDataItem{FieldName: "C", BinaryType: DatatypeFloat32},
					},
				},
			},
		},
	}
)

type simpleDao struct {
	A int64
	B float32
}

type rawCollectionDao struct {
	A int64
	B []byte
	C []int
}

type collectionItem struct {
	C float64
}

type collectionDao struct {
	A int64
	B []collectionItem
}

func TestBinary_writeReadSimpleSchema(t *testing.T) {
	// Arrange
	dao := simpleDao{
		A: 123,
		B: 234.56,
	}

	data := make([]byte, 12) // a (8) + b (4) = 12 bytes

	// Act
	index, err := simpleBinarySchema.Write(dao, data, 0)

	// Assert
	AssertNil(t, err)
	AssertEqual(t, len(data), index)
	AssertEqual(t, dao.A, int64(binary.LittleEndian.Uint64(data[0:])))
	AssertApprox(t, dao.B, math.Float32frombits(binary.LittleEndian.Uint32(data[8:])), 0.00001)

	// ----- read the data -----

	// Arrange
	readDao := simpleDao{}

	// Act
	index, err = simpleBinarySchema.Read(&readDao, data, 0)

	// Assert
	AssertNil(t, err)
	AssertEqual(t, len(data), index)
	AssertEqual(t, dao, readDao)
}

func TestBinary_writeReadRawCollectionSchema(t *testing.T) {
	// Arrange
	dao := rawCollectionDao{
		A: 123,
		B: []byte{1, 100, 10},
		C: []int{2, 200, 20},
	}

	data := make([]byte, 28) // a (8) + len(b) (4) + b (3*1) + len(c) (4) + c (3*3) = 28 bytes

	// Act
	index, err := rawCollectionBinarySchema.Write(dao, data, 0)

	// Assert
	AssertNil(t, err)
	AssertEqual(t, len(data), index)
	AssertEqual(t, dao.A, int64(binary.LittleEndian.Uint64(data[0:])))
	AssertEqual(t, len(dao.B), int(binary.LittleEndian.Uint32(data[8:])))
	AssertEqual(t, dao.B, data[12:15])
	AssertEqual(t, len(dao.C), int(binary.LittleEndian.Uint32(data[15:])))
	AssertEqual(t, dao.C[0], int(uint32(data[19])|uint32(data[20])<<8|uint32(data[21])<<16))
	AssertEqual(t, dao.C[1], int(uint32(data[22])|uint32(data[23])<<8|uint32(data[24])<<16))
	AssertEqual(t, dao.C[2], int(uint32(data[25])|uint32(data[26])<<8|uint32(data[27])<<16))

	// ----- read the data -----

	// Arrange
	readDao := rawCollectionDao{}

	// Act
	index, err = rawCollectionBinarySchema.Read(&readDao, data, 0)

	// Assert
	AssertNil(t, err)
	AssertEqual(t, len(data), index)
	AssertEqual(t, dao, readDao)
}

func TestBinary_writeReadCollectionSchema(t *testing.T) {
	// Arrange
	dao := collectionDao{
		A: 123,
		B: []collectionItem{
			{C: 1.23},
			{C: 2.34},
		},
	}

	data := make([]byte, 20) // a (8) + len(b) (4) + 2*C (2*4) = 20 bytes

	// Act
	index, err := collectionBinarySchema.Write(dao, data, 0)

	// Assert
	AssertNil(t, err)
	AssertEqual(t, len(data), index)
	AssertEqual(t, dao.A, int64(binary.LittleEndian.Uint64(data[0:])))
	AssertEqual(t, len(dao.B), int(binary.LittleEndian.Uint32(data[8:])))
	AssertApprox(t, dao.B[0].C, float64(math.Float32frombits(binary.LittleEndian.Uint32(data[12:]))), 0.00001)
	AssertApprox(t, dao.B[1].C, float64(math.Float32frombits(binary.LittleEndian.Uint32(data[16:]))), 0.00001)

	// ----- read the data -----

	// Arrange
	readDao := collectionDao{}

	// Act
	index, err = collectionBinarySchema.Read(&readDao, data, 0)

	// Assert
	AssertNil(t, err)
	AssertEqual(t, len(data), index)
	AssertEqual(t, dao.A, readDao.A)
	AssertEqual(t, len(dao.B), len(readDao.B))
	AssertApprox(t, dao.B[0].C, readDao.B[0].C, 0.00001)
	AssertApprox(t, dao.B[1].C, readDao.B[1].C, 0.00001)
}

package util

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"math"
	"reflect"
)

type Datatype int

const (
	DatatypeByte Datatype = iota
	DatatypeInt16
	DatatypeInt24
	DatatypeInt32
	DatatypeInt64
	DatatypeFloat32
	DatatypeFloat64
)

type BinaryItem interface {
	Write(object any, data []byte, index int) (int, error)
	Read(object any, data []byte, index int) (int, error)
}

type BinarySchema struct {
	Items []BinaryItem // All items of this object schema. They are written and read in the given order.
}

func (b *BinarySchema) Write(object any, data []byte, index int) (int, error) {
	var err error

	for _, item := range b.Items {
		index, err = item.Write(object, data, index)
		if err != nil {
			return -1, err
		}
	}

	return index, nil
}

func (b *BinarySchema) Read(object any, data []byte, index int) (int, error) {
	var err error

	for _, item := range b.Items {
		index, err = item.Read(object, data, index)
		if err != nil {
			return -1, err
		}
	}

	return index, nil
}

type BinaryDataItem struct {
	FieldName  string   // Name of the golang struct field.
	BinaryType Datatype // Type this field should be stored to. This has to be compatible with the FieldType.
}

func (b *BinaryDataItem) Write(object any, data []byte, index int) (int, error) {
	field := reflect.ValueOf(object).FieldByName(b.FieldName)
	binaryType := b.BinaryType
	fieldName := b.FieldName
	return writeBinaryValue(binaryType, fieldName, field, data, index)
}

func (b *BinaryDataItem) Read(object any, data []byte, index int) (int, error) {
	field := reflect.Indirect(reflect.ValueOf(object)).FieldByName(b.FieldName)
	return readBinaryValue(b.BinaryType, b.FieldName, field, data, index)
}

// BinaryRawCollectionItem represents the simple schema for array of e.g. integers. It also stores the size of the array as 32 bit integer.
type BinaryRawCollectionItem struct {
	FieldName  string   // Name of the golang struct slice.
	BinaryType Datatype // Type this field should be stored to. This has to be compatible with the FieldType.
}

func (b *BinaryRawCollectionItem) Write(object any, data []byte, index int) (int, error) {
	reflectionType := reflect.Indirect(reflect.ValueOf(object)).FieldByName(b.FieldName)
	if reflectionType.Kind() != reflect.Slice && reflectionType.Kind() != reflect.Array {
		return -1, errors.Errorf("Unsupported type given to BinaryCollectionItem (type=%v, index=%d, object=%v). Only slices and array are supported.", reflectionType, index, object)
	}

	binary.LittleEndian.PutUint32(data[index:], uint32(reflectionType.Len()))
	index += 4

	var err error
	for i := 0; i < reflectionType.Len(); i++ {
		element := reflectionType.Index(i)
		binaryType := b.BinaryType
		fieldName := b.FieldName

		index, err = writeBinaryValue(binaryType, fieldName, element, data, index)
		if err != nil {
			return -1, err
		}
	}

	return index, nil
}

func (b *BinaryRawCollectionItem) Read(object any, data []byte, index int) (int, error) {
	reflectionType := reflect.Indirect(reflect.ValueOf(object)).FieldByName(b.FieldName)
	if reflectionType.Kind() != reflect.Slice && reflectionType.Kind() != reflect.Array {
		return -1, errors.Errorf("Unsupported type given to BinaryCollectionItem (type=%v, index=%d, object=%v). Only slices and array are supported.", reflectionType, index, object)
	}

	length := int(binary.LittleEndian.Uint32(data[index:]))
	index += 4

	slice := reflect.MakeSlice(reflectionType.Type(), length, length)
	reflectionType.Set(slice)

	var err error
	for i := 0; i < length; i++ {
		element := slice.Index(i)

		index, err = readBinaryValue(b.BinaryType, b.FieldName, element, data, index)
		if err != nil {
			return -1, err
		}
	}

	return index, nil
}

// BinaryCollectionItem represents the simple schema for array of structs.
type BinaryCollectionItem struct {
	FieldName  string       // Name of the golang struct slice.
	ItemSchema BinarySchema // Schema of the item in this collection
}

func (b *BinaryCollectionItem) Write(object any, data []byte, index int) (int, error) {
	reflectionType := reflect.Indirect(reflect.ValueOf(object)).FieldByName(b.FieldName)
	if reflectionType.Kind() != reflect.Slice && reflectionType.Kind() != reflect.Array {
		return -1, errors.Errorf("Unsupported type given to BinaryCollectionItem (type=%v, index=%d, object=%v). Only slices and array are supported.", reflectionType, index, object)
	}

	binary.LittleEndian.PutUint32(data[index:], uint32(reflectionType.Len()))
	index += 4

	var err error
	for i := 0; i < reflectionType.Len(); i++ {
		element := reflectionType.Index(i)
		index, err = b.ItemSchema.Write(element.Interface(), data, index)
		if err != nil {
			return -1, err
		}
	}

	return index, nil
}

func (b *BinaryCollectionItem) Read(object any, data []byte, index int) (int, error) {
	panic("Implement")
}

func writeBinaryValue(binaryType Datatype, fieldName string, value reflect.Value, data []byte, index int) (int, error) {
	switch binaryType {
	case DatatypeByte:
		data[index] = byte(getUint64FromValue(value))
		index += 1
	case DatatypeInt16:
		binary.LittleEndian.PutUint16(data[index:], uint16(getUint64FromValue(value)))
		index += 2
	case DatatypeInt24:
		v := getUint64FromValue(value)
		data[index] = byte(v)
		data[index+1] = byte(v >> 8)
		data[index+2] = byte(v >> 16)
		index += 3
	case DatatypeInt32:
		binary.LittleEndian.PutUint32(data[index:], uint32(getUint64FromValue(value)))
		index += 4
	case DatatypeInt64:
		binary.LittleEndian.PutUint64(data[index:], getUint64FromValue(value))
		index += 8
	case DatatypeFloat32:
		binary.LittleEndian.PutUint32(data[index:], math.Float32bits(float32(value.Float())))
		index += 4
	case DatatypeFloat64:
		binary.LittleEndian.PutUint64(data[index:], math.Float64bits(value.Float()))
		index += 8
	default:
		return -1, errors.Errorf("Unsupported datatype %d for field %s", binaryType, fieldName)
	}
	return index, nil
}

func readBinaryValue(binaryType Datatype, fieldName string, value reflect.Value, data []byte, index int) (int, error) {
	switch binaryType {
	case DatatypeByte:
		value.Set(reflect.ValueOf(data[index]))
		index += 1
	case DatatypeInt16:
		value.Set(reflect.ValueOf(int16(binary.LittleEndian.Uint16(data[index:]))))
		index += 2
	case DatatypeInt24:
		d := data[index:]
		value.Set(reflect.ValueOf(int(uint32(d[0]) | uint32(d[1])<<8 | uint32(d[2])<<16)))
		index += 3
	case DatatypeInt32:
		value.Set(reflect.ValueOf(int32(binary.LittleEndian.Uint32(data[index:]))))
		index += 4
	case DatatypeInt64:
		if value.Kind() == reflect.Uint64 {
			value.Set(reflect.ValueOf(binary.LittleEndian.Uint64(data[index:])))
		} else {
			value.Set(reflect.ValueOf(int64(binary.LittleEndian.Uint64(data[index:]))))
		}
		index += 8
	case DatatypeFloat32:
		if value.Kind() == reflect.Float32 {
			value.Set(reflect.ValueOf(math.Float32frombits(binary.LittleEndian.Uint32(data[index:]))))
		} else {
			value.Set(reflect.ValueOf(float64(math.Float32frombits(binary.LittleEndian.Uint32(data[index:])))))
		}
		index += 4
	case DatatypeFloat64:
		value.Set(reflect.ValueOf(math.Float64frombits(binary.LittleEndian.Uint64(data[index:]))))
		index += 8
	default:
		return -1, errors.Errorf("Unsupported datatype %d for field %s", binaryType, fieldName)
	}

	return index, nil
}

func getUint64FromValue(value reflect.Value) uint64 {
	if value.Kind() == reflect.Int ||
		value.Kind() == reflect.Int8 ||
		value.Kind() == reflect.Int16 ||
		value.Kind() == reflect.Int32 ||
		value.Kind() == reflect.Int64 {
		return uint64(value.Int())
	} else if value.Kind() == reflect.Uint ||
		value.Kind() == reflect.Uint8 ||
		value.Kind() == reflect.Uint16 ||
		value.Kind() == reflect.Uint32 ||
		value.Kind() == reflect.Uint64 {
		return value.Uint()
	}
	panic("Unsupported value type " + value.Kind().String() + " to convert to uint.")
}

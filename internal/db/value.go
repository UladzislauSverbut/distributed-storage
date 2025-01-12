package db

import "fmt"

type ValueType = uint8

type Value interface {
	GetType() ValueType
	Size() int
	Empty() bool
	serialize() []byte
	parse([]byte)
}

const (
	VALUE_TYPE_NULL ValueType = iota
	VALUE_TYPE_STRING
	VALUE_TYPE_INT32
	VALUE_TYPE_INT64
	VALUE_TYPE_UINT32
	VALUE_TYPE_UINT64
)

func createValue(valueType ValueType) Value {
	switch valueType {
	case VALUE_TYPE_STRING:
		return &StringValue{}
	case VALUE_TYPE_INT32:
		return &IntValue[int32]{}
	case VALUE_TYPE_INT64:
		return &IntValue[int64]{}
	case VALUE_TYPE_UINT32:
		return &IntValue[uint32]{}
	case VALUE_TYPE_UINT64:
		return &IntValue[uint64]{}
	default:
		panic(fmt.Sprintf("Value can`t be created because type is not supported %d", valueType))
	}
}

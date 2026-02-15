package vals

import "fmt"

type ValueType = uint8

type Value interface {
	Type() ValueType
	Size() int
	Empty() bool
	Serialize() []byte
	Parse([]byte)
}

const (
	TYPE_NULL ValueType = iota
	TYPE_STRING
	TYPE_INT32
	TYPE_INT64
	TYPE_UINT32
	TYPE_UINT64
)

func New(valueType ValueType) Value {
	switch valueType {
	case TYPE_STRING:
		return &StringValue{}
	case TYPE_INT32:
		return &IntValue[int32]{}
	case TYPE_INT64:
		return &IntValue[int64]{}
	case TYPE_UINT32:
		return &IntValue[uint32]{}
	case TYPE_UINT64:
		return &IntValue[uint64]{}
	default:
		panic(fmt.Sprintf("Value can`t be created because type is not supported %d", valueType))
	}
}

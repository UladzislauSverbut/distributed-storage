package primitive

import "fmt"

type PrimitiveType = uint8

type Primitive interface {
	Type() PrimitiveType
	Empty() bool
	Equal(other Primitive) bool
}

const (
	TYPE_NULL PrimitiveType = iota
	TYPE_STRING
	TYPE_INT32
	TYPE_INT64
	TYPE_UINT32
	TYPE_UINT64
)

func New(primitiveType PrimitiveType) Primitive {
	switch primitiveType {
	case TYPE_NULL:
		return &Null{}
	case TYPE_STRING:
		return &String{}
	case TYPE_INT32:
		return &Int32{}
	case TYPE_INT64:
		return &Int64{}
	case TYPE_UINT32:
		return &Uint32{}
	case TYPE_UINT64:
		return &Uint64{}
	default:
		panic(fmt.Sprintf("Value: can`t create new value because of wrong type %d", primitiveType))
	}
}

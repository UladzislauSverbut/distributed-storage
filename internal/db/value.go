package db

import "fmt"

type ValueType = uint8

type Value interface {
	GetType() ValueType
	Size() int
	serialize() []byte
	parse([]byte)
}

const (
	VALUE_TYPE_STRING ValueType = iota
	VALUE_TYPE_INT64
)

func createValue(valueType ValueType) Value {
	switch valueType {
	case VALUE_TYPE_STRING:
		return &StringValue{}
	case VALUE_TYPE_INT64:
		return &IntValue{}
	default:
		panic(fmt.Sprintf("Value can`t be created because type is not supported %d", valueType))
	}
}

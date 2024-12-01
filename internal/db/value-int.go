package db

import (
	"encoding/binary"
)

type IntValue struct {
	Val int64
}

func (value *IntValue) GetType() ValueType {
	return VALUE_TYPE_INT64
}

func (value *IntValue) Size() int {
	return 8
}

func (value *IntValue) serialize() []byte {
	unsignedInt := uint64(value.Val) + (1 << 63)
	serializedInt := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedInt, unsignedInt)

	return serializedInt
}

func (value *IntValue) parse(serializedInt []byte) {
	signedInt := uint64(binary.LittleEndian.Uint64(serializedInt)) + (1 << 63)

	value.Val = int64(signedInt)
}

func NewIntValue(value int64) *IntValue {
	return &IntValue{value}
}

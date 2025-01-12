package db

import (
	"encoding/binary"
)

type SupportedInts interface {
	int32 | int64 | uint32 | uint64
}

type IntValue[T SupportedInts] struct {
	num T
}

func (value *IntValue[T]) Get() T {
	return value.num
}

func (value *IntValue[T]) GetType() ValueType {
	return VALUE_TYPE_INT64
}

func (value *IntValue[T]) Size() int {
	return 8
}

func (value *IntValue[T]) Empty() bool {
	return false
}

func (value *IntValue[T]) serialize() []byte {
	unsignedInt := uint64(value.num) + (1 << 63)
	serializedInt := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedInt, unsignedInt)

	return serializedInt
}

func (value *IntValue[T]) parse(serializedInt []byte) {
	signedInt := uint64(binary.LittleEndian.Uint64(serializedInt)) + (1 << 63)

	value.num = T(signedInt)
}

func NewIntValue[T SupportedInts](value T) *IntValue[T] {
	return &IntValue[T]{value}
}

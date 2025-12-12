package vals

import (
	"encoding/binary"
)

type SupportedInts interface {
	int32 | int64 | uint32 | uint64
}

type IntValue[T SupportedInts] struct {
	num T
}

func (value *IntValue[T]) Value() T {
	return value.num
}

func (value *IntValue[T]) Type() ValueType {
	return TYPE_INT64
}

func (value *IntValue[T]) Size() int {
	return 8
}

func (value *IntValue[T]) Empty() bool {
	return false
}

func (value *IntValue[T]) Serialize() []byte {
	unsignedInt := uint64(value.num) + (1 << 63)
	serializedInt := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedInt, unsignedInt)

	return serializedInt
}

func (value *IntValue[T]) Parse(serializedInt []byte) {
	signedInt := uint64(binary.LittleEndian.Uint64(serializedInt)) + (1 << 63)

	value.num = T(signedInt)
}

func NewInt[T SupportedInts](value T) *IntValue[T] {
	return &IntValue[T]{value}
}

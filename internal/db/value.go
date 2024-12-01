package db

type ValueType = uint32

type Value interface {
	GetType() ValueType
	Size() int
	serialize() []byte
	parse([]byte)
}

const (
	VALUE_TYPE_STRING = iota
	VALUE_TYPE_INT64
)

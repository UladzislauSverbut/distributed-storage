package db

type ValueType = uint32

type Value interface {
	getType() ValueType
	serialize() []byte
	parse([]byte)
	size() int
}

const (
	VALUE_TYPE_STRING = iota
	VALUE_TYPE_INT64
)

package value

type ValueType = uint32

type Value interface {
	getType() ValueType
	serialize() []byte
	parse([]byte)
	size() int
}

type SupportedValues interface {
	[]byte | int64
}

const (
	VALUE_EMPTY ValueType = iota
	VALUE_TYPE_STRING
	VALUE_TYPE_INT64
)

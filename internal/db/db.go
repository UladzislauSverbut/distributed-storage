package db

const (
	VALUE_TYPE_BYTES ValueType = iota
	VALUE_TYPE_INT64
)

type ValueType = uint32

type Value struct {
	Type ValueType
	I64  int64
	Str  []byte
}

type Record struct {
	Columns []string
	Values  []Value
}

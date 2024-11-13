package db

const (
	VALUE_TYPE_BYTES ValueType = iota
	VALUE_TYPE_INT64
)

type ValueType = uint32

type Value struct {
	Type  ValueType
	Int64 int64
	Str   []byte
}

type Record struct {
	Columns []string
	Values  []Value
}

func (record *Record) addString(key []byte, value []byte) {}
func (record *Record) addInt64(key []byte, value int64)   {}
func (record *Record) get(key []byte) *Value              {}

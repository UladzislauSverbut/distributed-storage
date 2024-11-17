package db

const (
	VALUE_EMPTY ValueType = iota
	VALUE_TYPE_BYTES
	VALUE_TYPE_INT64
)

type ValueType = uint32

type Value struct {
	Type  ValueType
	Int64 int64
	Str   []byte
}

type Record struct {
	Fields []string
	Values []Value
}

func (record *Record) addString(key string, value []byte) {
	record.Fields = append(record.Fields, key)
	record.Values = append(record.Values, Value{Type: VALUE_TYPE_BYTES, Str: value})
}

func (record *Record) addInt64(key string, value int64) {
	record.Fields = append(record.Fields, key)
	record.Values = append(record.Values, Value{Type: VALUE_TYPE_BYTES, Int64: value})
}

func (record *Record) get(key string) Value {
	for index, field := range record.Fields {
		if field == key {
			return record.Values[index]
		}
	}

	return Value{Type: VALUE_EMPTY}
}

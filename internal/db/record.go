package db

import "distributed-storage/internal/db/value"

type Record struct {
	Fields []string
	Values []value.Value
}

func (record *Record) addString(key string, val []byte) {
	record.Fields = append(record.Fields, key)
	record.Values = append(record.Values, value.NewStringValue(val))
}

func (record *Record) addInt(key string, val int64) {
	record.Fields = append(record.Fields, key)
	record.Values = append(record.Values, value.NewIntValue(val))
}

func (record *Record) get(key string) Value {
	for index, field := range record.Fields {
		if field == key {
			return record.Values[index]
		}
	}

	return Value{Type: VALUE_EMPTY}
}

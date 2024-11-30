package db

import (
	"distributed-storage/internal/kv"
	"encoding/binary"
	"fmt"
	"slices"
)

type Table struct {
	name         string
	columnTypes  []ValueType
	columnNames  []string
	indexColumns []string
	prefix       uint32
	kv           *kv.KeyValue
}

func (table *Table) Get(query *Record) (*Record, error) {
	keyColumnValues, err := table.extractKeyColumnValues(query)

	if err != nil {
		return nil, err
	}

	value, err := table.kv.Get(table.createKey(keyColumnValues))

	if err != nil {
		return nil, err
	}

}

func (table *Table) extractKeyColumnValues(query *Record) ([]Value, error) {
	columnValues := make([]Value, len(table.indexColumns))

	if len(table.indexColumns) != len(query.Fields) {
		return nil, fmt.Errorf("Table can`t get record because one of columns are not indexed: %v", query.Fields)
	}

	for indexColumPos, indexColumn := range table.indexColumns {
		columnPos, success := slices.BinarySearch(query.Fields, indexColumn)

		if !success || len(query.Values) < columnPos {
			return nil, fmt.Errorf("Table can`t get record because missed column value: %s", indexColumn)
		}

		columnValues[indexColumPos] = query.Values[columnPos]
	}

	return columnValues, nil
}

func (table *Table) createKey(values []Value) []byte {
	key := make([]byte, 4)

	binary.LittleEndian.PutUint32(key, table.prefix)

	key = append(key, table.encodeValues(values)...)

	return key
}

func (table *Table) encodeValues(values []Value) []byte {
	encodedValues := make([]byte, 0)

	for _, value := range values {
		switch value.Type {
		case VALUE_TYPE_INT64:
			unsignedValue := uint64(value.Int64) + (1 << 63)
			encodedValue := make([]byte, 8)

			binary.LittleEndian.PutUint64(encodedValue, unsignedValue)
			encodedValues = append(encodedValues, encodedValue...)

		case VALUE_TYPE_BYTES:
			encodedValues = append(encodedValues, value.Str...)
			encodedValues = append(encodedValues, 0) // null-terminated string

		default:
			panic(fmt.Sprint("unsupported column value type %d", value.Type))
		}

	}

	return encodedValues
}

func (table *Table) decodeValues(value []byte) ([]Value, error) {

}

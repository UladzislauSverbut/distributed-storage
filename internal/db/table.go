package db

import (
	"distributed-storage/internal/kv"
	"encoding/binary"
	"fmt"
	"slices"
)

type Table struct {
	Name         string
	ColumnTypes  []ValueType
	ColumnNames  []string
	IndexColumns []string
	Prefix       uint32
	kv           *kv.KeyValue
}

func (table *Table) Get(record *Record) (bool, error) {
	indexedColumnValues, err := table.extractIndexedColumnValues(record)

	if err != nil {
		return false, err
	}

	value, err := table.kv.Get(table.createKey(indexedColumnValues))

	if value == nil {
		return false, err
	}

	for columnIndex, columnName := range table.ColumnNames {
		var columnValue Value

		if record.get(columnName) == nil {
			switch table.ColumnTypes[columnIndex] {
			case VALUE_TYPE_INT64:
				columnValue = &IntValue{}
			case VALUE_TYPE_STRING:
				columnValue = &StringValue{}
			default:
				panic(fmt.Sprintf("Table doesn`t support column with type %d:", table.ColumnTypes[columnIndex]))
			}

			columnValue.parse(value)
			value = value[columnValue.size():]

			record.addValue(columnName, columnValue)
		}
	}

	return true, nil
}

func (table *Table) extractIndexedColumnValues(query *Record) ([]Value, error) {
	columnValues := make([]Value, len(table.IndexColumns))

	if len(table.IndexColumns) != len(query.Fields) {
		return nil, fmt.Errorf("Table can`t get record because one of columns are not indexed: %v", query.Fields)
	}

	for indexColumPos, indexColumn := range table.IndexColumns {
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

	binary.LittleEndian.PutUint32(key, table.Prefix)

	for _, value := range values {
		key = append(key, value.serialize()...)
	}

	return key
}

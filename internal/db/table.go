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
	kv           kv.KeyValue
}

func (table *Table) Get(query *Record) (*Record, error) {
	if !table.checkIfColumnsIndexed(query.Fields) {
		return nil, fmt.Errorf("Table can`t get record because one of columns are not indexed: %v", query.Fields)
	}

	record, err := table.kv.Get(table.createRecordKey(query.Values))

	if err != nil {
		return nil, err
	}

}

func (table *Table) checkIfColumnsIndexed(columns []string) bool {
	if len(table.IndexColumns) != len(columns) {
		return false
	}

	for _, indexColumn := range table.IndexColumns {
		if !slices.Contains(columns, indexColumn) {
			return false
		}
	}

	return true
}

func (table *Table) createRecordKey(values []Value) []byte {
	key := make([]byte, 4)

	binary.LittleEndian.PutUint32(key, table.Prefix)

	key = append(key, table.encodeValues(values)...)

	return key
}

func (table *Table) encodeValues(values []Value) []byte {

}

package db

import (
	"distributed-storage/internal/kv"
	"encoding/binary"
	"fmt"
)

const (
	MODE_UPSERT int8 = iota // insert or update record
	MODE_UPDATE             // update existing record
	MODE_INSERT             // insert record
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
	key, err := table.getKey(record)

	if err != nil {
		return false, err
	}

	encodedPayload, err := table.kv.Get(table.encodeKey(key))

	if encodedPayload == nil {
		return false, err
	}

	table.decodePayload(record, encodedPayload)

	return true, nil
}

func (table *Table) Insert(record *Record) (bool, error) {
	return table.update(record, MODE_INSERT)
}

func (table *Table) Update(record *Record) (bool, error) {
	return table.update(record, MODE_UPDATE)
}

func (table *Table) Upsert(record *Record) (bool, error) {
	return table.update(record, MODE_UPSERT)
}

func (table *Table) update(record *Record, mode int8) (bool, error) {
	key, err := table.getKey(record)

	if err != nil {
		return false, err
	}

	encodedKey := table.encodeKey(key)
	encodedPayload, err := table.kv.Get(encodedKey)

	if err != nil {
		return false, err
	}

	if mode == MODE_UPDATE && encodedPayload == nil {
		return false, fmt.Errorf("Table can`t update record because it`s not exist: %v", record)
	}

	if mode == MODE_INSERT && encodedPayload != nil {
		return false, fmt.Errorf("Table can`t insert record because it`s exist: %v", record)
	}

	payload, err := table.getPayload(record)

	if err != nil {
		return false, err
	}

	table.kv.Set(encodedKey, table.encodePayload(payload))

	return true, nil
}

func (table *Table) getKey(query *Record) ([]Value, error) {
	values := make([]Value, len(table.IndexColumns))

	for _, columnName := range table.IndexColumns {
		columnValue := query.Get(columnName)

		if columnValue == nil {
			return nil, fmt.Errorf("Table cant`t create primary key because one of index columns is missed: %s", columnName)
		}

		values = append(values, columnValue)
	}

	return values, nil
}

func (table *Table) encodeKey(values []Value) []byte {
	encodedKey := make([]byte, 4)

	binary.LittleEndian.PutUint32(encodedKey, table.Prefix)

	for _, value := range values {
		encodedKey = append(encodedKey, value.serialize()...)
	}

	return encodedKey
}

func (table *Table) getPayload(query *Record) ([]Value, error) {
	values := make([]Value, len(table.ColumnNames))

	for columnPos, columnName := range table.ColumnNames {
		columnValue := query.Get(columnName)

		if columnValue == nil {
			columnValue = createValue(table.ColumnTypes[columnPos])
		}

		values = append(values, columnValue)
	}

	return values, nil
}

func (table *Table) encodePayload(values []Value) []byte {
	encodedPayload := make([]byte, len(values))

	for _, value := range values {
		encodedPayload = append(encodedPayload, value.serialize()...)
	}

	return encodedPayload
}

func (table *Table) decodePayload(record *Record, encodedPayload []byte) {
	for columnPos, columnName := range table.ColumnNames {
		columnValue := createValue(table.ColumnTypes[columnPos])

		columnValue.parse(encodedPayload)
		encodedPayload = encodedPayload[columnValue.Size():]

		if record.Has(columnName) {
			record.Set(columnName, columnValue)
		}
	}
}

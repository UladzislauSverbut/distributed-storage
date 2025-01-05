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

type TableSchema struct {
	Name                  string
	ColumnTypes           []ValueType
	ColumnNames           []string
	IndexColumns          []string
	SecondaryIndexColumns []string
	Prefix                uint32
}

type Table struct {
	schema *TableSchema
	kv     *kv.KeyValue
}

func (table *Table) Get(query *Record) (bool, error) {
	key, err := table.getKey(query)

	if err != nil {
		return false, err
	}

	getResponse, err := table.kv.Get(&kv.GetRequest{Key: table.encodeKey(key)})

	if getResponse.Value == nil {
		return false, err
	}

	table.decodePayload(query, getResponse.Value)

	return true, nil
}

func (table *Table) Insert(query *Record) error {
	return table.update(query, MODE_INSERT)
}

func (table *Table) Update(query *Record) error {
	return table.update(query, MODE_UPDATE)
}

func (table *Table) Upsert(query *Record) error {
	return table.update(query, MODE_UPSERT)
}

func (table *Table) Delete(query *Record) error {
	key, err := table.getKey(query)

	if err != nil {
		return err
	}

	_, err = table.kv.Delete(&kv.DeleteRequest{Key: table.encodeKey(key)})

	return err
}

func (table *Table) update(query *Record, mode int8) error {
	key, err := table.getKey(query)

	if err != nil {
		return err
	}

	encodedKey := table.encodeKey(key)
	getResponse, err := table.kv.Get(&kv.GetRequest{Key: encodedKey})

	if err != nil {
		return err
	}

	if mode == MODE_UPDATE && getResponse.Value == nil {
		return fmt.Errorf("Table can`t update record because it`s not exist: %v", query)
	}

	if mode == MODE_INSERT && getResponse.Value != nil {
		return fmt.Errorf("Table can`t insert record because it`s exist: %v", query)
	}

	payload, err := table.getPayload(query)

	if err != nil {
		return err
	}

	_, err = table.kv.Set(&kv.SetRequest{Key: encodedKey, Value: table.encodePayload(payload)})

	return err
}

func (table *Table) getKey(query *Record) ([]Value, error) {
	values := make([]Value, len(table.schema.IndexColumns))

	for columnPos, columnName := range table.schema.IndexColumns {
		columnValue := query.Get(columnName)

		if columnValue == nil {
			return nil, fmt.Errorf("Table cant`t create primary key because one of index columns is missed: %s", columnName)
		}

		values[columnPos] = columnValue
	}

	return values, nil
}

func (table *Table) encodeKey(values []Value) []byte {
	encodedKey := make([]byte, 4)

	binary.LittleEndian.PutUint32(encodedKey, table.schema.Prefix)

	for _, value := range values {
		encodedKey = append(encodedKey, value.serialize()...)
	}

	return encodedKey
}

func (table *Table) getPayload(query *Record) ([]Value, error) {
	values := make([]Value, len(table.schema.ColumnNames))

	for columnPos, columnName := range table.schema.ColumnNames {
		columnValue := query.Get(columnName)

		if columnValue == nil {
			columnValue = createValue(table.schema.ColumnTypes[columnPos])
		}

		values[columnPos] = columnValue
	}

	return values, nil
}

func (table *Table) encodePayload(values []Value) []byte {
	encodedPayload := make([]byte, 0)

	for _, value := range values {
		encodedPayload = append(encodedPayload, value.serialize()...)
	}

	return encodedPayload
}

func (table *Table) decodePayload(record *Record, encodedPayload []byte) {
	for columnPos, columnName := range table.schema.ColumnNames {
		columnValue := createValue(table.schema.ColumnTypes[columnPos])

		columnValue.parse(encodedPayload)
		encodedPayload = encodedPayload[columnValue.Size():]

		if record.Has(columnName) {
			record.Set(columnName, columnValue)
		} else {
			record.Set(columnName, columnValue)
		}
	}
}

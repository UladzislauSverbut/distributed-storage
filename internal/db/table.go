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

const PRIMARY_INDEX_ID uint32 = 0

type TableSchema struct {
	Name             string
	ColumnTypes      []ValueType
	ColumnNames      []string
	PrimaryIndex     []string
	SecondaryIndexes [][]string
	Prefix           uint32
}

type Table struct {
	schema *TableSchema
	kv     *kv.KeyValue
}

func (table *Table) Get(query *Record) (bool, error) {
	key, err := table.getPrimaryKey(query)

	if err != nil {
		return false, err
	}

	response, err := table.kv.Get(&kv.GetRequest{Key: key})

	if response.Value == nil {
		return false, err
	}

	table.decodePayload(query, response.Value)

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
	key, err := table.getPrimaryKey(query)

	if err != nil {
		return err
	}

	_, err = table.kv.Delete(&kv.DeleteRequest{Key: key})

	return err
}

func (table *Table) update(query *Record, mode int8) error {
	key, err := table.getPrimaryKey(query)

	if err != nil {
		return err
	}

	getResponse, err := table.kv.Get(&kv.GetRequest{Key: key})

	if err != nil {
		return err
	}

	if mode == MODE_UPDATE && getResponse.Value == nil {
		return fmt.Errorf("Table can`t update record because it`s not exist: %v", query)
	}

	if mode == MODE_INSERT && getResponse.Value != nil {
		return fmt.Errorf("Table can`t insert record because it`s exist: %v", query)
	}

	_, err = table.kv.Set(&kv.SetRequest{Key: key, Value: table.encodePayload(query)})

	return err
}

func (table *Table) encodePayload(query *Record) []byte {
	encodedPayload := make([]byte, 0)

	for columnPos, columnName := range table.schema.ColumnNames {
		columnValue := query.Get(columnName)

		if columnValue == nil {
			columnValue = createValue(table.schema.ColumnTypes[columnPos])
		}

		encodedPayload = append(encodedPayload, columnValue.serialize()...)
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

func (table *Table) getPrimaryKey(query *Record) ([]byte, error) {
	if table.matchIndex(query, table.schema.PrimaryIndex) {
		return table.encodeIndex(PRIMARY_INDEX_ID, table.getIndex(query, table.schema.PrimaryIndex)), nil
	}

	if secondaryKey := table.getSecondaryKey(query); secondaryKey != nil {
		response, err := table.kv.Get(&kv.GetRequest{Key: secondaryKey})

		return response.Value, err
	}

	return nil, nil
}

func (table *Table) getSecondaryKey(query *Record) []byte {
	for indexNumber, secondaryColumns := range table.schema.SecondaryIndexes {
		if table.matchIndex(query, secondaryColumns) {
			return table.encodeIndex(PRIMARY_INDEX_ID+uint32(indexNumber), table.getIndex(query, table.schema.PrimaryIndex))
		}
	}

	return nil
}

func (table *Table) matchIndex(query *Record, indexColumns []string) bool {
	for _, columnName := range indexColumns {
		columnValue := query.Get(columnName)

		if columnValue == nil {
			return false
		}
	}

	return true
}

func (table *Table) getIndex(query *Record, indexColumns []string) []Value {
	values := make([]Value, len(indexColumns))

	for columnPos, columnName := range table.schema.PrimaryIndex {
		values[columnPos] = query.Get(columnName)
	}

	return values
}

func (table *Table) encodeIndex(indexId uint32, values []Value) []byte {
	encodedKey := make([]byte, 8)

	binary.LittleEndian.PutUint32(encodedKey[0:4], table.schema.Prefix)
	binary.LittleEndian.PutUint32(encodedKey[4:8], indexId)

	for _, value := range values {
		encodedKey = append(encodedKey, value.serialize()...)
	}

	return encodedKey
}

package db

import (
	"distributed-storage/internal/kv"
	"encoding/binary"
	"fmt"
	"slices"
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
	primaryKey, err := table.getPrimaryKey(query)

	if err != nil {
		return err
	}

	getResponse, err := table.kv.Get(&kv.GetRequest{Key: primaryKey})

	if err != nil {
		return err
	}

	if mode == MODE_UPDATE && getResponse.Value == nil {
		return fmt.Errorf("Table can`t update record because it`s not exist: %v", query)
	}

	if mode == MODE_INSERT && getResponse.Value != nil {
		return fmt.Errorf("Table can`t insert record because it`s exist: %v", query)
	}

	if _, err := table.kv.Set(&kv.SetRequest{Key: primaryKey, Value: table.encodePayload(query)}); err != nil {
		return err
	}

	if getResponse.Value == nil {
		return table.createSecondaryIndexes(primaryKey, query)
	} else {
		oldQuery := &Record{}
		table.decodePayload(oldQuery, getResponse.Value)

		return table.updateSecondaryIndexes(primaryKey, query, oldQuery)
	}
}

func (table *Table) createSecondaryIndexes(primaryKey []byte, query *Record) error {
	for indexNumber := range table.schema.SecondaryIndexes {

		if secondaryKey := table.getSecondaryKey(indexNumber, query); secondaryKey != nil {

			if _, err := table.kv.Set(&kv.SetRequest{Key: secondaryKey, Value: primaryKey}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (table *Table) updateSecondaryIndexes(primaryKey []byte, query *Record, oldQuery *Record) error {
	oldPrimaryKey, _ := table.getPrimaryKey(oldQuery)
	primaryKeyChanged := slices.Compare(primaryKey, oldPrimaryKey) != 0

	for indexNumber := range table.schema.SecondaryIndexes {
		secondaryKey := table.getSecondaryKey(indexNumber, query)
		oldSecondaryKey := table.getSecondaryKey(indexNumber, oldQuery)

		if slices.Compare(secondaryKey, oldSecondaryKey) != 0 {
			if oldSecondaryKey != nil {
				if _, err := table.kv.Delete(&kv.DeleteRequest{Key: oldSecondaryKey}); err != nil {
					return err
				}
			}

		}

		if primaryKeyChanged && secondaryKey != nil {
			if _, err := table.kv.Set(&kv.SetRequest{Key: secondaryKey, Value: primaryKey}); err != nil {
				return err
			}
		}
	}

	return nil
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

		record.Set(columnName, columnValue)
	}
}

func (table *Table) getPrimaryKey(query *Record) ([]byte, error) {
	if table.matchIndex(query, table.schema.PrimaryIndex) {
		return table.encodeIndex(PRIMARY_INDEX_ID, table.getIndex(query, table.schema.PrimaryIndex)), nil
	}

	for indexNumber := range table.schema.SecondaryIndexes {

		if secondaryKey := table.getSecondaryKey(indexNumber, query); secondaryKey != nil {
			response, err := table.kv.Get(&kv.GetRequest{Key: secondaryKey})

			return response.Value, err
		}
	}

	return nil, fmt.Errorf("Table can`t find primary key for record: %v", query)
}

func (table *Table) getSecondaryKey(indexNumber int, query *Record) []byte {
	if !table.matchIndex(query, table.schema.SecondaryIndexes[indexNumber]) {
		return nil
	}

	return table.encodeIndex(PRIMARY_INDEX_ID+uint32(indexNumber), table.getIndex(query, table.schema.SecondaryIndexes[indexNumber]))
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

	for columnPos, columnName := range indexColumns {
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

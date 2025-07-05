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
}

type Table struct {
	schema *TableSchema
	kv     *kv.ChildNamespace
}

func (table *Table) Get(query *Object) (*Object, error) {
	key := table.getPrimaryKey(query)

	if key == nil {
		return nil, fmt.Errorf("Table cant`t find record because one of primary index columns is missed: %s", query)
	}

	response, err := table.kv.Get(&kv.GetRequest{Key: key})

	if err != nil {
		return nil, err
	}

	return table.decodePayload(response.Value), err
}

func (table *Table) Delete(query *Object) error {
	key := table.getPrimaryKey(query)

	if key == nil {
		return fmt.Errorf("Table cant`t delete record because one of primary index columns is missed: %s", query)
	}

	_, err := table.kv.Delete(&kv.DeleteRequest{Key: key})

	return err
}

func (table *Table) Insert(record *Object) error {
	return table.update(record, MODE_INSERT)
}

func (table *Table) Update(record *Object) error {
	return table.update(record, MODE_UPDATE)
}

func (table *Table) Upsert(record *Object) error {
	return table.update(record, MODE_UPSERT)
}

func (table *Table) update(record *Object, mode int8) error {
	primaryKey := table.getPrimaryKey(record)

	if primaryKey == nil {
		return fmt.Errorf("Table cant`t update record because one of primary index columns is missed: %s", record)
	}

	response, err := table.kv.Get(&kv.GetRequest{Key: primaryKey})

	if err != nil {
		return err
	}

	if mode == MODE_UPDATE && response.Value == nil {
		return fmt.Errorf("Table can`t update record because it`s not exist: %v", record)
	}

	if mode == MODE_INSERT && response.Value != nil {
		return fmt.Errorf("Table can`t insert record because it`s exist: %v", record)
	}

	if _, err := table.kv.Set(&kv.SetRequest{Key: primaryKey, Value: table.encodePayload(record)}); err != nil {
		return err
	}

	if response.Value == nil {
		return table.createSecondaryIndexes(record)
	} else {
		oldRecord := table.decodePayload(response.Value)
		return table.updateSecondaryIndexes(record, oldRecord)
	}

}

func (table *Table) createSecondaryIndexes(record *Object) error {
	for indexNumber := range table.schema.SecondaryIndexes {

		if secondaryKey := table.getSecondaryKey(record, indexNumber); secondaryKey != nil {

			if _, err := table.kv.Set(&kv.SetRequest{Key: secondaryKey}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (table *Table) updateSecondaryIndexes(record *Object, oldRecord *Object) error {
	primaryKey := table.getPrimaryKey(record)
	oldPrimaryKey := table.getPrimaryKey(oldRecord)

	primaryKeyChanged := slices.Compare(primaryKey, oldPrimaryKey) != 0

	for indexNumber := range table.schema.SecondaryIndexes {
		secondaryKey := table.getSecondaryKey(record, indexNumber)
		oldSecondaryKey := table.getSecondaryKey(oldRecord, indexNumber)

		if slices.Compare(secondaryKey, oldSecondaryKey) != 0 {
			if oldSecondaryKey != nil {
				if _, err := table.kv.Delete(&kv.DeleteRequest{Key: oldSecondaryKey}); err != nil {
					return err
				}
			}

		}

		if primaryKeyChanged && secondaryKey != nil {
			if _, err := table.kv.Set(&kv.SetRequest{Key: secondaryKey}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (table *Table) encodePayload(record *Object) []byte {
	if record == nil {
		return nil
	}

	encodedPayload := make([]byte, 0)

	for _, columnName := range table.schema.ColumnNames {
		columnValue := record.Get(columnName)

		if columnValue.Empty() {
			encodedPayload = append(encodedPayload, byte(0))
		} else {
			encodedPayload = append(encodedPayload, byte(1))
			encodedPayload = append(encodedPayload, columnValue.serialize()...)
		}

	}

	return encodedPayload
}

func (table *Table) decodePayload(encodedPayload []byte) *Object {
	if encodedPayload == nil {
		return nil
	}

	record := NewObject()

	for columnPos, columnName := range table.schema.ColumnNames {
		if encodedPayload[0] == 0 {
			record.Set(columnName, NewNullValue())
		} else {
			columnValue := createValue(table.schema.ColumnTypes[columnPos])
			columnValue.parse(encodedPayload[1:])

			encodedPayload = encodedPayload[columnValue.Size()+1:]

			record.Set(columnName, columnValue)
		}
	}

	return record
}

func (table *Table) getPrimaryKey(query *Object) []byte {
	values := query.GetMany(table.schema.PrimaryIndex)

	if table.hasEmptyValues(values) {
		return nil
	}

	primaryKey := make([]byte, 4)

	binary.LittleEndian.PutUint32(primaryKey[0:4], PRIMARY_INDEX_ID)

	for _, value := range values {
		primaryKey = append(primaryKey, value.serialize()...)
	}

	return primaryKey
}

func (table *Table) getSecondaryKey(query *Object, indexNumber int) []byte {
	primaryKeyValues := query.GetMany(table.schema.PrimaryIndex)
	secondaryKeyValues := query.GetMany(table.schema.SecondaryIndexes[indexNumber])

	if table.hasEmptyValues(primaryKeyValues) || table.hasEmptyValues(secondaryKeyValues) {
		return nil
	}

	secondaryKey := make([]byte, 4)

	binary.LittleEndian.PutUint32(secondaryKey[0:4], PRIMARY_INDEX_ID+uint32(indexNumber+1))

	for _, value := range secondaryKeyValues {
		secondaryKey = append(secondaryKey, value.serialize()...)
	}

	for _, value := range primaryKeyValues {
		secondaryKey = append(secondaryKey, value.serialize()...)
	}

	return secondaryKey
}

func (table *Table) hasEmptyValues(values []Value) bool {
	return slices.IndexFunc(values, func(value Value) bool { return value.Empty() }) >= 0
}

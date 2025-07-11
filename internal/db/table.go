package db

import (
	"bytes"
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

const PRIMARY_INDEX_ID int = 0
const INDEX_ID_SIZE int = 4

type TableSchema struct {
	Name             string
	ColumnNames      []string
	PrimaryIndex     []string
	SecondaryIndexes [][]string
	ColumnTypes      map[string]ValueType
}

type Table struct {
	schema *TableSchema
	kv     *kv.Namespace
}

func (table *Table) Get(query *Object) (*Object, error) {
	index := table.getPrimaryIndex(query)

	if index == nil {
		return nil, fmt.Errorf("Table cant`t find record because one of primary index columns is missed: %s", query)
	}

	if response, err := table.kv.Get(&kv.GetRequest{Key: index}); err != nil {
		return nil, err
	} else {
		return table.decodePayload(response.Value), err
	}
}

func (table *Table) Find(query *Object) ([]*Object, error) {
	iteratedIndex, isPrimary := table.getPartialIndex(query)
	cursor := table.kv.Scan(&kv.ScanRequest{Key: iteratedIndex})

	records := make([]*Object, 0)

	if isPrimary {
		for index, value := cursor.Current(); table.matchIndexes(index, iteratedIndex); index, value = cursor.Next() {
			records = append(records, table.decodePayload(value))
		}
	} else {
		for index, _ := cursor.Current(); table.matchIndexes(index, iteratedIndex); index, _ = cursor.Next() {
			primaryIndexValues, _, _ := table.decodeSecondaryIndex(index)

			if response, err := table.kv.Get(&kv.GetRequest{Key: table.encodePrimaryIndex(primaryIndexValues)}); err != nil {
				return nil, err
			} else {
				records = append(records, table.decodePayload(response.Value))
			}
		}
	}

	return records, nil
}

func (table *Table) GetAll() []*Object {
	cursor := table.kv.Scan(&kv.ScanRequest{})

	records := make([]*Object, 0)

	for _, value := cursor.Current(); value != nil; _, value = cursor.Next() {
		records = append(records, table.decodePayload(value))
	}

	return records
}

func (table *Table) Delete(query *Object) error {
	index := table.getPrimaryIndex(query)

	if index == nil {
		return fmt.Errorf("Table cant`t delete record because one of primary index columns is missed: %s", query)
	}

	_, err := table.kv.Delete(&kv.DeleteRequest{Key: index})

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
	primaryIndex := table.getPrimaryIndex(record)

	if primaryIndex == nil {
		return fmt.Errorf("Table cant`t update record because one of primary index columns is missed: %s", record)
	}

	response, err := table.kv.Get(&kv.GetRequest{Key: primaryIndex})

	if err != nil {
		return err
	}

	if mode == MODE_UPDATE && response.Value == nil {
		return fmt.Errorf("Table can`t update record because it`s not exist: %v", record)
	}

	if mode == MODE_INSERT && response.Value != nil {
		return fmt.Errorf("Table can`t insert record because it`s exist: %v", record)
	}

	if _, err := table.kv.Set(&kv.SetRequest{Key: primaryIndex, Value: table.encodePayload(record)}); err != nil {
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

		if secondaryIndex := table.getSecondaryIndex(record, indexNumber); secondaryIndex != nil {

			if _, err := table.kv.Set(&kv.SetRequest{Key: secondaryIndex}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (table *Table) updateSecondaryIndexes(record *Object, oldRecord *Object) error {
	primaryIndex := table.getPrimaryIndex(record)
	oldPrimaryIndex := table.getPrimaryIndex(oldRecord)

	primaryIndexChanged := slices.Compare(primaryIndex, oldPrimaryIndex) != 0

	for indexNumber := range table.schema.SecondaryIndexes {
		secondaryIndex := table.getSecondaryIndex(record, indexNumber)
		oldSecondaryIndex := table.getSecondaryIndex(oldRecord, indexNumber)

		if slices.Compare(secondaryIndex, oldSecondaryIndex) != 0 {
			if oldSecondaryIndex != nil {
				if _, err := table.kv.Delete(&kv.DeleteRequest{Key: oldSecondaryIndex}); err != nil {
					return err
				}
			}

		}

		if primaryIndexChanged && secondaryIndex != nil {
			if _, err := table.kv.Set(&kv.SetRequest{Key: secondaryIndex}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (table *Table) getPrimaryIndex(query *Object) []byte {
	values := query.GetMany(table.schema.PrimaryIndex)

	if table.containsEmptyValues(values) {
		return nil
	}

	return table.encodePrimaryIndex(values)
}

func (table *Table) getSecondaryIndex(query *Object, secondaryIndexNumber int) []byte {
	primaryIndexValues := query.GetMany(table.schema.PrimaryIndex)
	secondaryIndexValues := query.GetMany(table.schema.SecondaryIndexes[secondaryIndexNumber])

	if table.containsEmptyValues(primaryIndexValues) || table.containsEmptyValues(secondaryIndexValues) {
		return nil
	}

	return table.encodeSecondaryIndex(primaryIndexValues, secondaryIndexValues, secondaryIndexNumber)
}

func (table *Table) getPartialIndex(query *Object) ([]byte, bool) {
	primaryIndexValues := query.GetMany(table.schema.PrimaryIndex)

	if !table.containsEmptyValues(primaryIndexValues) || len(table.schema.SecondaryIndexes) == 0 {
		return table.encodePrimaryIndex(table.removeEmptyValues(primaryIndexValues)), true
	}

	matchedSecondaryIndexNumber := 0
	matchedSecondaryIndexValues := table.removeEmptyValues(query.GetMany(table.schema.SecondaryIndexes[matchedSecondaryIndexNumber]))

	for secondaryIndexNumber := 1; secondaryIndexNumber < len(table.schema.SecondaryIndexes); secondaryIndexNumber++ {
		secondaryIndexValues := table.removeEmptyValues(query.GetMany(table.schema.SecondaryIndexes[secondaryIndexNumber]))

		if len(secondaryIndexValues) > len(matchedSecondaryIndexValues) {
			matchedSecondaryIndexNumber = secondaryIndexNumber
			matchedSecondaryIndexValues = secondaryIndexValues
		}
	}

	return table.encodeSecondaryIndex(table.removeEmptyValues(primaryIndexValues), matchedSecondaryIndexValues, matchedSecondaryIndexNumber), false
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
	if len(encodedPayload) == 0 {
		return nil
	}

	record := NewObject()

	for _, columnName := range table.schema.ColumnNames {
		if encodedPayload[0] == 0 {
			record.Set(columnName, NewNullValue())
		} else {
			columnValue := createValue(table.schema.ColumnTypes[columnName])
			columnValue.parse(encodedPayload[1:])

			encodedPayload = encodedPayload[columnValue.Size()+1:]

			record.Set(columnName, columnValue)
		}
	}

	return record
}

func (table *Table) encodePrimaryIndex(values []Value) []byte {
	if len(values) == 0 {
		return nil
	}

	primaryIndex := make([]byte, INDEX_ID_SIZE)

	binary.LittleEndian.PutUint32(primaryIndex[0:INDEX_ID_SIZE], uint32(PRIMARY_INDEX_ID))

	for _, value := range values {
		primaryIndex = append(primaryIndex, value.serialize()...)
	}

	return primaryIndex
}

func (table *Table) encodeSecondaryIndex(primaryIndexValues []Value, secondaryIndexValues []Value, secondaryIndexNumber int) []byte {
	if len(secondaryIndexValues) == 0 {
		return nil
	}

	secondaryIndex := make([]byte, INDEX_ID_SIZE)

	binary.LittleEndian.PutUint32(secondaryIndex[0:INDEX_ID_SIZE], uint32(PRIMARY_INDEX_ID+secondaryIndexNumber+1))

	for _, value := range secondaryIndexValues {
		secondaryIndex = append(secondaryIndex, value.serialize()...)
	}

	if len(primaryIndexValues) > 0 {
		for _, value := range primaryIndexValues {
			secondaryIndex = append(secondaryIndex, value.serialize()...)
		}
	}

	return secondaryIndex
}

func (table *Table) decodeSecondaryIndex(encodedIndex []byte) (primaryIndexValues []Value, secondaryIndexValues []Value, secondaryIndexNumber int) {
	if len(encodedIndex) < INDEX_ID_SIZE {
		return
	}

	indexId := binary.LittleEndian.Uint32(encodedIndex[0:INDEX_ID_SIZE])
	secondaryIndexNumber = int(indexId) - PRIMARY_INDEX_ID - 1

	encodedIndex = encodedIndex[INDEX_ID_SIZE:]

	for _, columnName := range table.schema.SecondaryIndexes[secondaryIndexNumber] {
		columnValue := createValue(table.schema.ColumnTypes[columnName])
		columnValue.parse(encodedIndex)
		secondaryIndexValues = append(secondaryIndexValues, columnValue)

		encodedIndex = encodedIndex[columnValue.Size():]
	}

	for _, columnName := range table.schema.PrimaryIndex {
		columnValue := createValue(table.schema.ColumnTypes[columnName])
		columnValue.parse(encodedIndex)
		primaryIndexValues = append(primaryIndexValues, columnValue)

		encodedIndex = encodedIndex[columnValue.Size():]
	}

	return
}

func (table *Table) matchIndexes(firstEncodedIndex []byte, secondEncodedIndex []byte) bool {
	sharedIndexSize := min(len(firstEncodedIndex), len(secondEncodedIndex))

	if sharedIndexSize == 0 {
		return false
	}

	return bytes.Equal(firstEncodedIndex[0:sharedIndexSize], secondEncodedIndex[0:sharedIndexSize])
}

func (table *Table) containsEmptyValues(values []Value) bool {
	return slices.IndexFunc(values, func(value Value) bool { return value.Empty() }) >= 0
}

func (table *Table) removeEmptyValues(values []Value) []Value {
	nonEmptyValues := make([]Value, 0)

	for _, value := range values {
		if value.Empty() {
			break
		}

		nonEmptyValues = append(nonEmptyValues, value)
	}

	return nonEmptyValues
}

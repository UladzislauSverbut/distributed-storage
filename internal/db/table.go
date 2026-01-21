package db

import (
	"bytes"
	"distributed-storage/internal/kv"
	"distributed-storage/internal/pager"
	"distributed-storage/internal/vals"
	"encoding/binary"
	"fmt"
	"slices"
)

const (
	MODE_UPSERT int8 = iota // insert or update record
	MODE_UPDATE             // update existing record
	MODE_INSERT             // insert record
)

const PRIMARY_INDEX_ID int = 0 // primary index id, secondary indexes ids start from 1

const INDEX_ID_SIZE int = 4 // size of index section id in bytes

type TableSchema struct {
	Name             string
	ColumnNames      []string
	PrimaryIndex     []string
	SecondaryIndexes [][]string
	ColumnTypes      map[string]vals.ValueType
}

type Table struct {
	kv     *kv.KeyValue
	schema *TableSchema
	size   uint64
}

func NewTable(root pager.PagePointer, pageManager *pager.PageManager, schema *TableSchema, size uint64) (*Table, error) {
	table := &Table{
		kv:     kv.NewKeyValue(root, pageManager),
		schema: schema,
		size:   size,
	}

	if err := table.validateTableSchema(); err != nil {
		return nil, err
	}

	return table, nil
}

func (table *Table) Get(query *vals.Object) (*vals.Object, error) {
	index := table.getPrimaryIndex(query)

	if index == nil {
		return nil, fmt.Errorf("Table: cant`t find record because one of primary index columns is missed in query %s", query)
	}

	if response, err := table.kv.Get(&kv.GetRequest{Key: index}); err != nil {
		return nil, err
	} else {
		return table.decodePayload(response.Value), err
	}
}

func (table *Table) Find(query *vals.Object) ([]*vals.Object, error) {
	partialIndex, isPrimary := table.getPartialIndex(query)
	cursor := table.kv.Scan(&kv.ScanRequest{Key: partialIndex})

	records := make([]*vals.Object, 0)

	if isPrimary {
		for index, value := cursor.Current(); table.matchIndexes(index, partialIndex); index, value = cursor.Next() {
			records = append(records, table.decodePayload(value))
		}
	} else {
		for index, _ := cursor.Current(); table.matchIndexes(index, partialIndex); index, _ = cursor.Next() {
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

func (table *Table) GetAll() []*vals.Object {
	cursor := table.kv.Scan(&kv.ScanRequest{})

	records := make([]*vals.Object, 0)

	for index, value := cursor.Current(); value != nil; index, value = cursor.Next() {

		if table.matchPrimaryIndex(index) {
			records = append(records, table.decodePayload(value))
		}
	}

	return records
}

func (table *Table) Delete(query *vals.Object) error {
	index := table.getPrimaryIndex(query)

	if index == nil {
		return fmt.Errorf("Table cant`t delete record because one of primary index columns is missed: %s", query)
	}

	_, err := table.kv.Delete(&kv.DeleteRequest{Key: index})

	return err
}

func (table *Table) Insert(record *vals.Object) error {
	return table.update(record, MODE_INSERT)
}

func (table *Table) Update(record *vals.Object) error {
	return table.update(record, MODE_UPDATE)
}

func (table *Table) Upsert(record *vals.Object) error {
	return table.update(record, MODE_UPSERT)
}

func (table *Table) Size() uint64 {
	return table.size
}

func (table *Table) Root() pager.PagePointer {
	return table.kv.Root()
}

func (table *Table) update(record *vals.Object, mode int8) error {
	primaryIndex := table.getPrimaryIndex(record)

	if primaryIndex == nil {
		return fmt.Errorf("Table: cant`t update record because one of primary index columns is missed in record %s", record)
	}

	response, err := table.kv.Get(&kv.GetRequest{Key: primaryIndex})

	if err != nil {
		return err
	}

	if mode == MODE_UPDATE && response.Value == nil {
		return fmt.Errorf("Table: can`t update record because it`s not exist in record %v", record)
	}

	if mode == MODE_INSERT && response.Value != nil {
		return fmt.Errorf("Table: can`t insert record because it`s exist in record %v", record)
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

func (table *Table) createSecondaryIndexes(record *vals.Object) error {
	for indexNumber := range table.schema.SecondaryIndexes {

		if secondaryIndex := table.getSecondaryIndex(record, indexNumber); secondaryIndex != nil {

			if _, err := table.kv.Set(&kv.SetRequest{Key: secondaryIndex}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (table *Table) updateSecondaryIndexes(record *vals.Object, oldRecord *vals.Object) error {
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

func (table *Table) getPrimaryIndex(query *vals.Object) []byte {
	vals := query.GetMany(table.schema.PrimaryIndex)

	if table.containsEmptyValues(vals) {
		return nil
	}

	return table.encodePrimaryIndex(vals)
}

func (table *Table) getSecondaryIndex(query *vals.Object, secondaryIndexNumber int) []byte {
	primaryIndexVals := query.GetMany(table.schema.PrimaryIndex)
	secondaryIndexVals := query.GetMany(table.schema.SecondaryIndexes[secondaryIndexNumber])

	if table.containsEmptyValues(primaryIndexVals) || table.containsEmptyValues(secondaryIndexVals) {
		return nil
	}

	return table.encodeSecondaryIndex(primaryIndexVals, secondaryIndexVals, secondaryIndexNumber)
}

func (table *Table) getPartialIndex(query *vals.Object) ([]byte, bool) {
	primaryIndexVals := query.GetMany(table.schema.PrimaryIndex)

	if !table.containsEmptyValues(primaryIndexVals) || len(table.schema.SecondaryIndexes) == 0 {
		return table.encodePrimaryIndex(table.removeEmptyValues(primaryIndexVals)), true
	}

	matchedSecondaryIndexNumber := 0
	matchedSecondaryIndexVals := table.removeEmptyValues(query.GetMany(table.schema.SecondaryIndexes[matchedSecondaryIndexNumber]))

	for secondaryIndexNumber := 1; secondaryIndexNumber < len(table.schema.SecondaryIndexes); secondaryIndexNumber++ {
		secondaryIndexVals := table.removeEmptyValues(query.GetMany(table.schema.SecondaryIndexes[secondaryIndexNumber]))

		if len(secondaryIndexVals) > len(matchedSecondaryIndexVals) {
			matchedSecondaryIndexNumber = secondaryIndexNumber
			matchedSecondaryIndexVals = secondaryIndexVals
		}
	}

	return table.encodeSecondaryIndex(table.removeEmptyValues(primaryIndexVals), matchedSecondaryIndexVals, matchedSecondaryIndexNumber), false
}

func (table *Table) encodePayload(record *vals.Object) []byte {
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
			encodedPayload = append(encodedPayload, columnValue.Serialize()...)
		}
	}

	return encodedPayload
}

func (table *Table) decodePayload(encodedPayload []byte) *vals.Object {
	if len(encodedPayload) == 0 {
		return nil
	}

	record := vals.NewObject()

	for _, columnName := range table.schema.ColumnNames {
		if encodedPayload[0] == 0 {
			record.Set(columnName, vals.NewNull())
		} else {
			columnValue := vals.New(table.schema.ColumnTypes[columnName])
			columnValue.Parse(encodedPayload[1:])

			encodedPayload = encodedPayload[columnValue.Size()+1:]

			record.Set(columnName, columnValue)
		}
	}

	return record
}

func (table *Table) encodePrimaryIndex(values []vals.Value) []byte {
	if len(values) == 0 {
		return nil
	}

	primaryIndex := make([]byte, INDEX_ID_SIZE)

	binary.LittleEndian.PutUint32(primaryIndex[0:INDEX_ID_SIZE], uint32(PRIMARY_INDEX_ID))

	for _, value := range values {
		primaryIndex = append(primaryIndex, value.Serialize()...)
	}

	return primaryIndex
}

func (table *Table) encodeSecondaryIndex(primaryIndexVals []vals.Value, secondaryIndexVals []vals.Value, secondaryIndexNumber int) []byte {
	if len(secondaryIndexVals) == 0 {
		return nil
	}

	secondaryIndex := make([]byte, INDEX_ID_SIZE)

	binary.LittleEndian.PutUint32(secondaryIndex[0:INDEX_ID_SIZE], uint32(PRIMARY_INDEX_ID+secondaryIndexNumber+1))

	for _, value := range secondaryIndexVals {
		secondaryIndex = append(secondaryIndex, value.Serialize()...)
	}

	for _, value := range primaryIndexVals {
		secondaryIndex = append(secondaryIndex, value.Serialize()...)
	}

	return secondaryIndex
}

func (table *Table) decodeSecondaryIndex(encodedIndex []byte) (primaryIndexVals []vals.Value, secondaryIndexVals []vals.Value, secondaryIndexNumber int) {
	if table.matchPrimaryIndex(encodedIndex) {
		return
	}

	indexId := binary.LittleEndian.Uint32(encodedIndex[0:INDEX_ID_SIZE])
	secondaryIndexNumber = int(indexId) - PRIMARY_INDEX_ID - 1

	encodedIndex = encodedIndex[INDEX_ID_SIZE:]

	for _, columnName := range table.schema.SecondaryIndexes[secondaryIndexNumber] {
		columnValue := vals.New(table.schema.ColumnTypes[columnName])
		columnValue.Parse(encodedIndex)
		secondaryIndexVals = append(secondaryIndexVals, columnValue)

		encodedIndex = encodedIndex[columnValue.Size():]
	}

	for _, columnName := range table.schema.PrimaryIndex {
		columnValue := vals.New(table.schema.ColumnTypes[columnName])
		columnValue.Parse(encodedIndex)
		primaryIndexVals = append(primaryIndexVals, columnValue)

		encodedIndex = encodedIndex[columnValue.Size():]
	}

	return
}

func (table *Table) matchPrimaryIndex(encodedIndex []byte) bool {
	if len(encodedIndex) < INDEX_ID_SIZE {
		return false
	}

	indexId := binary.LittleEndian.Uint32(encodedIndex[0:INDEX_ID_SIZE])

	return indexId == uint32(PRIMARY_INDEX_ID)
}

func (table *Table) matchIndexes(encodedIndex []byte, partialEncodedIndex []byte) bool {
	if len(encodedIndex) == 0 {
		return false
	}

	return bytes.Equal(encodedIndex[0:len(partialEncodedIndex)], partialEncodedIndex)
}

func (table *Table) containsEmptyValues(values []vals.Value) bool {
	return slices.IndexFunc(values, func(value vals.Value) bool { return value.Empty() }) >= 0
}

func (table *Table) removeEmptyValues(values []vals.Value) []vals.Value {
	nonEmptyValues := make([]vals.Value, 0)

	for _, value := range values {
		if value.Empty() {
			break
		}

		nonEmptyValues = append(nonEmptyValues, value)
	}

	return nonEmptyValues
}

func (table *Table) validateTableSchema() error {
	if table.schema.Name == "" {
		return fmt.Errorf("Transaction: couldn't create table because schema must have a name")
	}

	if len(table.schema.ColumnNames) == 0 {
		return fmt.Errorf("Transaction: couldn't create table because schema must have at least one column")
	}

	if len(table.schema.PrimaryIndex) == 0 {
		return fmt.Errorf("Transaction: couldn't create table because schema must have a primary index")
	}

	return nil
}

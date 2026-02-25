package events

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const UPDATE_TABLE_EVENT = "UPDATE_TABLE"

var updateTableParsingError = errors.New("UpdateTable: couldn't parse event")

// UpdateTable describes update of a table. Schemas are stored as raw bytes.
type UpdateTable struct {
	TableName string
	NewSchema []byte
	OldSchema []byte
}

func NewUpdateTable(tableName string, oldSchema []byte, newSchema []byte) *UpdateTable {
	return &UpdateTable{TableName: tableName, OldSchema: oldSchema, NewSchema: newSchema}
}

func (event *UpdateTable) Name() string {
	return UPDATE_TABLE_EVENT
}

func (event *UpdateTable) Serialize() []byte {
	serializedEvent := []byte(event.Name())
	serializedTableNameLength := make([]byte, 8)
	oldSchemaLength := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedTableNameLength, uint64(len(event.TableName)))
	binary.LittleEndian.PutUint64(oldSchemaLength, uint64(len(event.OldSchema)))

	serializedEvent = append(serializedEvent, serializedTableNameLength...)
	serializedEvent = append(serializedEvent, []byte(event.TableName)...)
	serializedEvent = append(serializedEvent, oldSchemaLength...)
	serializedEvent = append(serializedEvent, event.OldSchema...)
	serializedEvent = append(serializedEvent, event.NewSchema...)

	return serializedEvent
}

func ParseUpdateTable(data []byte) (*UpdateTable, error) {
	offset := len(UPDATE_TABLE_EVENT)

	if !bytes.Equal(data[0:offset], []byte(UPDATE_TABLE_EVENT)) {
		return nil, updateTableParsingError
	}

	serializedTableNameLength := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	tableName := string(data[offset : offset+int(serializedTableNameLength)])
	offset += int(serializedTableNameLength)

	oldSchemaLength := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	oldSchema := data[offset : offset+int(oldSchemaLength)]
	offset += int(oldSchemaLength)

	newSchema := data[offset:]

	return NewUpdateTable(tableName, oldSchema, newSchema), nil
}

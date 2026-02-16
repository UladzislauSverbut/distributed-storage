package events

import (
	"distributed-storage/internal/helpers"
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

	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, []byte(event.TableName)...)
	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, event.OldSchema...)
	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, event.NewSchema...)

	return serializedEvent
}

func ParseUpdateTable(data []byte) (*UpdateTable, error) {
	parts := helpers.SplitBy(data, ' ')

	if len(parts) != 4 || string(parts[0]) != UPDATE_TABLE_EVENT {
		return nil, updateTableParsingError
	}

	return NewUpdateTable(string(parts[1]), parts[2], parts[3]), nil
}

package events

import (
	"distributed-storage/internal/helpers"
	"errors"
)

const DELETE_TABLE_EVENT = "DELETE_TABLE"

var deleteTableParsingError = errors.New("DeleteTable: couldn't parse event")

type DeleteTable struct {
	TableName string
}

func NewDeleteTable(tableName string) *DeleteTable {
	return &DeleteTable{TableName: tableName}
}

func (event *DeleteTable) Name() string {
	return DELETE_TABLE_EVENT
}

func (event *DeleteTable) Serialize() []byte {
	serializedEvent := []byte(event.Name())

	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, []byte(event.TableName)...)

	return serializedEvent
}

func ParseDeleteTable(data []byte) (*DeleteTable, error) {
	parts := helpers.SplitBy(data, ' ')

	if len(parts) != 2 || string(parts[0]) != DELETE_TABLE_EVENT {
		return nil, deleteTableParsingError
	}

	return NewDeleteTable(string(parts[1])), nil
}

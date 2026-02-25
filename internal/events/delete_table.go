package events

import (
	"bytes"
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
	serializedEvent = append(serializedEvent, []byte(event.TableName)...)

	return serializedEvent
}

func ParseDeleteTable(data []byte) (*DeleteTable, error) {
	offset := len(DELETE_TABLE_EVENT)

	if !bytes.Equal(data[:offset], []byte(DELETE_TABLE_EVENT)) {
		return nil, deleteTableParsingError
	}

	tableName := string(data[offset:])

	return NewDeleteTable(tableName), nil
}

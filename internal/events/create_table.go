package events

import (
	"distributed-storage/internal/helpers"
	"errors"
)

const CREATE_TABLE_EVENT = "CREATE_TABLE"

var createTableParsingError = errors.New("CreateTable: couldn't parse event")

// CreateTable describes creation of a table. Schema is stored as JSON.
type CreateTable struct {
	TableName string
	Schema    []byte // JSON-encoded TableSchema
}

func NewCreateTable(tableName string, schema []byte) *CreateTable {
	return &CreateTable{TableName: tableName, Schema: schema}
}

func (event *CreateTable) Name() string {
	return CREATE_TABLE_EVENT
}

func (event *CreateTable) Serialize() []byte {
	serializedEvent := []byte(event.Name())

	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, []byte(event.TableName)...)
	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, event.Schema...)

	return serializedEvent
}

func ParseCreateTable(data []byte) (*CreateTable, error) {
	parts := helpers.SplitBy(data, ' ')

	if len(parts) != 3 || string(parts[0]) != CREATE_TABLE_EVENT {
		return nil, createTableParsingError
	}

	return NewCreateTable(string(parts[1]), parts[2]), nil
}

package events

import (
	"bytes"
	"encoding/binary"
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
	serializedTableName := []byte(event.TableName)
	serializedTableNameLength := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedTableNameLength, uint64(len(serializedTableName)))

	serializedEvent = append(serializedEvent, serializedTableNameLength...)
	serializedEvent = append(serializedEvent, serializedTableName...)
	serializedEvent = append(serializedEvent, event.Schema...)

	return serializedEvent
}

func ParseCreateTable(data []byte) (*CreateTable, error) {
	offset := len(CREATE_TABLE_EVENT)

	if !bytes.Equal(data[:offset], []byte(CREATE_TABLE_EVENT)) {
		return nil, createTableParsingError
	}

	serializedTableNameLength := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	tableName := string(data[offset : offset+int(serializedTableNameLength)])
	offset += int(serializedTableNameLength)

	schema := data[offset:]

	return NewCreateTable(tableName, schema), nil
}

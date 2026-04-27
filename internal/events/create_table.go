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
	TableID uint64
	Schema  []byte // JSON-encoded TableSchema
}

func NewCreateTable(tableID uint64, schema []byte) *CreateTable {
	return &CreateTable{TableID: tableID, Schema: schema}
}

func (event *CreateTable) Name() string {
	return CREATE_TABLE_EVENT
}

func (event *CreateTable) Serialize() []byte {
	serializedEvent := []byte(event.Name())
	serializedTableID := make([]byte, 8)
	binary.LittleEndian.PutUint64(serializedTableID, event.TableID)

	serializedEvent = append(serializedEvent, serializedTableID...)
	serializedEvent = append(serializedEvent, event.Schema...)

	return serializedEvent
}

func ParseCreateTable(data []byte) (*CreateTable, error) {
	offset := len(CREATE_TABLE_EVENT)

	if !bytes.Equal(data[:offset], []byte(CREATE_TABLE_EVENT)) {
		return nil, createTableParsingError
	}

	tableID := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	schema := data[offset:]

	return NewCreateTable(tableID, schema), nil
}

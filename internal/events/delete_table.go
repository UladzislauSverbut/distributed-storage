package events

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const DELETE_TABLE_EVENT = "DELETE_TABLE"

var deleteTableParsingError = errors.New("DeleteTable: couldn't parse event")

type DeleteTable struct {
	TableID uint64
}

func NewDeleteTable(tableID uint64) *DeleteTable {
	return &DeleteTable{TableID: tableID}
}

func (event *DeleteTable) Name() string {
	return DELETE_TABLE_EVENT
}

func (event *DeleteTable) Serialize() []byte {
	serializedEvent := []byte(event.Name())
	serializedTableID := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedTableID, event.TableID)

	serializedEvent = append(serializedEvent, serializedTableID...)

	return serializedEvent
}

func ParseDeleteTable(data []byte) (*DeleteTable, error) {
	offset := len(DELETE_TABLE_EVENT)

	if !bytes.Equal(data[:offset], []byte(DELETE_TABLE_EVENT)) {
		return nil, deleteTableParsingError
	}

	tableID := binary.LittleEndian.Uint64(data[offset:])

	return NewDeleteTable(tableID), nil
}

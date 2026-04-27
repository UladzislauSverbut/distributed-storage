package events

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const DELETE_ENTRY_EVENT = "DELETE_ENTRY"

var deleteEntryParsingError = errors.New("DeleteEntry: couldn't parse event")

type DeleteEntry struct {
	TableID uint64
	Key     []byte
	Value   []byte
}

func NewDeleteEntry(tableID uint64, key []byte, value []byte) *DeleteEntry {
	return &DeleteEntry{TableID: tableID, Key: key, Value: value}
}

func (event *DeleteEntry) Name() string {
	return DELETE_ENTRY_EVENT
}

func (event *DeleteEntry) Serialize() []byte {
	serializedEvent := []byte(event.Name())
	serializedTableID := make([]byte, 8)
	serializedKeyLength := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedTableID, event.TableID)
	binary.LittleEndian.PutUint64(serializedKeyLength, uint64(len(event.Key)))

	serializedEvent = append(serializedEvent, serializedTableID...)
	serializedEvent = append(serializedEvent, serializedKeyLength...)
	serializedEvent = append(serializedEvent, event.Key...)
	serializedEvent = append(serializedEvent, event.Value...)

	return serializedEvent
}

func ParseDeleteEntry(data []byte) (*DeleteEntry, error) {
	offset := len(DELETE_ENTRY_EVENT)

	if !bytes.Equal(data[:offset], []byte(DELETE_ENTRY_EVENT)) {
		return nil, deleteEntryParsingError
	}

	tableID := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	keyLength := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	key := data[offset : offset+int(keyLength)]
	offset += int(keyLength)

	value := data[offset:]

	return NewDeleteEntry(tableID, key, value), nil
}

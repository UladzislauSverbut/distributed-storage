package events

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const INSERT_ENTRY_EVENT = "INSERT_ENTRY"

var insertEntryParsingError = errors.New("InsertEntry: couldn't parse event")

type InsertEntry struct {
	TableID uint64
	Key     []byte
	Value   []byte
}

func NewInsertEntry(tableID uint64, key []byte, value []byte) *InsertEntry {
	return &InsertEntry{TableID: tableID, Key: key, Value: value}
}

func (event *InsertEntry) Name() string {
	return INSERT_ENTRY_EVENT
}

func (event *InsertEntry) Serialize() []byte {
	serializedEvent := []byte(event.Name())
	serializedTableID := make([]byte, 8)
	keyLength := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedTableID, event.TableID)
	binary.LittleEndian.PutUint64(keyLength, uint64(len(event.Key)))

	serializedEvent = append(serializedEvent, serializedTableID...)
	serializedEvent = append(serializedEvent, keyLength...)
	serializedEvent = append(serializedEvent, event.Key...)
	serializedEvent = append(serializedEvent, event.Value...)

	return serializedEvent
}

func ParseInsertEntry(data []byte) (*InsertEntry, error) {
	offset := len(INSERT_ENTRY_EVENT)

	if !bytes.Equal(data[0:len(INSERT_ENTRY_EVENT)], []byte(INSERT_ENTRY_EVENT)) {
		return nil, insertEntryParsingError
	}

	tableID := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	keyLength := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	key := data[offset : offset+int(keyLength)]
	offset += int(keyLength)

	value := data[offset:]

	return NewInsertEntry(tableID, key, value), nil
}

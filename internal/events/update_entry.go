package events

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const UPDATE_ENTRY_EVENT = "UPDATE_ENTRY"

var updateEntryParsingError = errors.New("UpdateEntry: couldn't parse event")

type UpdateEntry struct {
	TableID  uint64
	Key      []byte
	NewValue []byte
	OldValue []byte
}

func NewUpdateEntry(tableID uint64, key []byte, oldValue []byte, newValue []byte) *UpdateEntry {
	return &UpdateEntry{TableID: tableID, Key: key, OldValue: oldValue, NewValue: newValue}
}

func (event *UpdateEntry) Name() string {
	return UPDATE_ENTRY_EVENT
}

func (event *UpdateEntry) Serialize() []byte {
	serializedEvent := []byte(event.Name())
	serializedTableID := make([]byte, 8)
	keyLength := make([]byte, 8)
	oldValueLength := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedTableID, event.TableID)
	binary.LittleEndian.PutUint64(keyLength, uint64(len(event.Key)))
	binary.LittleEndian.PutUint64(oldValueLength, uint64(len(event.OldValue)))

	serializedEvent = append(serializedEvent, serializedTableID...)
	serializedEvent = append(serializedEvent, keyLength...)
	serializedEvent = append(serializedEvent, event.Key...)
	serializedEvent = append(serializedEvent, oldValueLength...)
	serializedEvent = append(serializedEvent, event.OldValue...)
	serializedEvent = append(serializedEvent, event.NewValue...)

	return serializedEvent
}

func ParseUpdateEntry(data []byte) (*UpdateEntry, error) {
	offset := len(UPDATE_ENTRY_EVENT)

	if !bytes.Equal(data[0:offset], []byte(UPDATE_ENTRY_EVENT)) {
		return nil, updateEntryParsingError
	}

	tableID := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	keyLength := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	key := data[offset : offset+int(keyLength)]
	offset += int(keyLength)

	oldValueLength := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	oldValue := data[offset : offset+int(oldValueLength)]
	offset += int(oldValueLength)

	newValue := data[offset:]

	return NewUpdateEntry(tableID, key, oldValue, newValue), nil
}

package events

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const DELETE_ENTRY_EVENT = "DELETE_ENTRY"

var deleteEntryParsingError = errors.New("DeleteEntry: couldn't parse event")

type DeleteEntry struct {
	TableName string
	Key       []byte
	Value     []byte
}

func NewDeleteEntry(tableName string, key []byte, value []byte) *DeleteEntry {
	return &DeleteEntry{TableName: tableName, Key: key, Value: value}
}

func (event *DeleteEntry) Name() string {
	return DELETE_ENTRY_EVENT
}

func (event *DeleteEntry) Serialize() []byte {
	serializedEvent := []byte(event.Name())
	serializedTableNameLength := make([]byte, 8)
	serializedKeyLength := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedTableNameLength, uint64(len(event.TableName)))
	binary.LittleEndian.PutUint64(serializedKeyLength, uint64(len(event.Key)))

	serializedEvent = append(serializedEvent, serializedTableNameLength...)
	serializedEvent = append(serializedEvent, []byte(event.TableName)...)
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

	serializedTableNameLength := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	tableName := string(data[offset : offset+int(serializedTableNameLength)])
	offset += int(serializedTableNameLength)

	keyLength := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	key := data[offset : offset+int(keyLength)]
	offset += int(keyLength)

	value := data[offset:]

	return NewDeleteEntry(tableName, key, value), nil
}

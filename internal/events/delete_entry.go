package events

import (
	"distributed-storage/internal/helpers"
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

	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, []byte(event.TableName)...)
	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, event.Key...)
	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, event.Value...)

	return serializedEvent
}

func ParseDeleteEntry(data []byte) (*DeleteEntry, error) {
	parts := helpers.SplitBy(data, ' ')

	if len(parts) != 4 || string(parts[0]) != DELETE_ENTRY_EVENT {
		return nil, deleteEntryParsingError
	}

	return NewDeleteEntry(string(parts[1]), parts[2], parts[3]), nil
}

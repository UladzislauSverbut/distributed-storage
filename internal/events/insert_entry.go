package events

import (
	"distributed-storage/internal/helpers"
	"errors"
)

const INSERT_ENTRY_EVENT = "INSERT_ENTRY"

var insertEntryParsingError = errors.New("InsertEntry: couldn't parse event")

type InsertEntry struct {
	TableName string
	Key       []byte
	Value     []byte
}

func NewInsertEntry(tableName string, key []byte, value []byte) *InsertEntry {
	return &InsertEntry{TableName: tableName, Key: key, Value: value}
}

func (event *InsertEntry) Name() string {
	return INSERT_ENTRY_EVENT
}

func (event *InsertEntry) Serialize() []byte {
	serializedEvent := []byte(event.Name())

	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, []byte(event.TableName)...)
	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, event.Key...)
	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, event.Value...)

	return serializedEvent
}

func ParseInsertEntry(data []byte) (*InsertEntry, error) {
	parts := helpers.SplitBy(data, ' ')

	if len(parts) != 4 || string(parts[0]) != INSERT_ENTRY_EVENT {
		return nil, insertEntryParsingError
	}

	return NewInsertEntry(string(parts[1]), parts[2], parts[3]), nil
}

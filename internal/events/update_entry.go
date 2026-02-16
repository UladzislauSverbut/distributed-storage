package events

import (
	"distributed-storage/internal/helpers"
	"errors"
)

const UPDATE_ENTRY_EVENT = "UPDATE_ENTRY"

var updateEntryParsingError = errors.New("UpdateEntry: couldn't parse event")

type UpdateEntry struct {
	TableName string
	Key       []byte
	NewValue  []byte
	OldValue  []byte
}

func NewUpdateEntry(tableName string, key []byte, oldValue []byte, newValue []byte) *UpdateEntry {
	return &UpdateEntry{TableName: tableName, Key: key, OldValue: oldValue, NewValue: newValue}
}

func (event *UpdateEntry) Name() string {
	return UPDATE_ENTRY_EVENT
}

func (event *UpdateEntry) Serialize() []byte {
	serializedEvent := []byte(event.Name())

	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, []byte(event.TableName)...)
	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, event.Key...)
	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, event.OldValue...)
	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, event.NewValue...)

	return serializedEvent
}

func ParseUpdateEntry(data []byte) (*UpdateEntry, error) {
	parts := helpers.SplitBy(data, ' ')

	if len(parts) != 5 || string(parts[0]) != UPDATE_ENTRY_EVENT {
		return nil, updateEntryParsingError
	}

	return NewUpdateEntry(string(parts[1]), parts[2], parts[3], parts[4]), nil
}

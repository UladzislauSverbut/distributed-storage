package events

import (
	"bytes"
	"fmt"
)

// Event represents a WAL event independent from db package.
// It intentionally does NOT know about db internals to avoid cyclic imports.
type Event interface {
	Name() string
	Serialize() []byte
}

// ParseEvent identifies and parses a serialized event by its name prefix.
func Parse(data []byte) (Event, error) {
	switch {
	case bytes.HasPrefix(data, []byte(START_TRANSACTION_EVENT)):
		return ParseStartTransaction(data)
	case bytes.HasPrefix(data, []byte(COMMIT_TRANSACTION_EVENT)):
		return ParseCommitTransaction(data)
	case bytes.HasPrefix(data, []byte(INSERT_ENTRY_EVENT)):
		return ParseInsertEntry(data)
	case bytes.HasPrefix(data, []byte(DELETE_ENTRY_EVENT)):
		return ParseDeleteEntry(data)
	case bytes.HasPrefix(data, []byte(UPDATE_ENTRY_EVENT)):
		return ParseUpdateEntry(data)
	case bytes.HasPrefix(data, []byte(CREATE_TABLE_EVENT)):
		return ParseCreateTable(data)
	case bytes.HasPrefix(data, []byte(DELETE_TABLE_EVENT)):
		return ParseDeleteTable(data)
	case bytes.HasPrefix(data, []byte(UPDATE_TABLE_EVENT)):
		return ParseUpdateTable(data)
	case bytes.HasPrefix(data, []byte(UPDATE_DB_VERSION)):
		return ParseUpdateDBVersion(data)
	case bytes.HasPrefix(data, []byte(FREE_PAGES_EVENT)):
		return ParseFreePages(data)
	default:
		return nil, fmt.Errorf("events: unknown event type")
	}
}

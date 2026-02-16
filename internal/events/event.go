package events

import (
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
	eventName := ""

	for _, char := range data {
		if char == ' ' {
			break
		}
		eventName += string(char)
	}

	switch eventName {
	case START_TRANSACTION_EVENT:
		return ParseStartTransaction(data)
	case COMMIT_TRANSACTION_EVENT:
		return ParseCommitTransaction(data)
	case INSERT_ENTRY_EVENT:
		return ParseInsertEntry(data)
	case DELETE_ENTRY_EVENT:
		return ParseDeleteEntry(data)
	case UPDATE_ENTRY_EVENT:
		return ParseUpdateEntry(data)
	case CREATE_TABLE_EVENT:
		return ParseCreateTable(data)
	case DELETE_TABLE_EVENT:
		return ParseDeleteTable(data)
	case UPDATE_TABLE_EVENT:
		return ParseUpdateTable(data)
	case UPDATE_DB_VERSION:
		return ParseUpdateDBVersion(data)
	case FREE_PAGES_EVENT:
		return ParseFreePages(data)
	default:
		return nil, fmt.Errorf("events: unknown event type: %s", eventName)
	}
}

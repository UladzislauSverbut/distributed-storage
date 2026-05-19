package events

type InsertEntry struct {
	TableID uint64
	Key     []byte
	Value   []byte
}

func NewInsertEntry(tableID uint64, key []byte, value []byte) *InsertEntry {
	return &InsertEntry{TableID: tableID, Key: key, Value: value}
}

func (event *InsertEntry) Type() EventType {
	return INSERT_ENTRY_EVENT
}

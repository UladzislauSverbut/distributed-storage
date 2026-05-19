package events

type DeleteEntry struct {
	TableID uint64
	Key     []byte
	Value   []byte
}

func NewDeleteEntry(tableID uint64, key []byte, value []byte) *DeleteEntry {
	return &DeleteEntry{TableID: tableID, Key: key, Value: value}
}

func (event *DeleteEntry) Type() EventType {
	return DELETE_ENTRY_EVENT
}

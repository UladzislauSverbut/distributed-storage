package events

type UpdateEntry struct {
	TableID  uint64
	Key      []byte
	NewValue []byte
	OldValue []byte
}

func NewUpdateEntry(tableID uint64, key []byte, oldValue []byte, newValue []byte) *UpdateEntry {
	return &UpdateEntry{TableID: tableID, Key: key, OldValue: oldValue, NewValue: newValue}
}

func (event *UpdateEntry) Type() EventType {
	return UPDATE_ENTRY_EVENT
}

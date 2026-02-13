package events

const UPDATE_ENTRY_EVENT = "UPDATE_ENTRY"

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
	return []byte(event.Name() + "(TABLE=" + event.TableName + ",KEY=" + string(event.Key) + ",OLD_VALUE=" + string(event.OldValue) + ",NEW_VALUE=" + string(event.NewValue) + ")\n")
}

func (event *UpdateEntry) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

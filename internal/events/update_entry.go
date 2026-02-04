package events

const UPDATE_ENTRY_EVENT = "UPDATE_ENTRY"

type UpdateEntry struct {
	TableName string
	Key       []byte
	NewValue  []byte
	OldValue  []byte
}

func (e *UpdateEntry) Name() string {
	return UPDATE_ENTRY_EVENT
}

func (e *UpdateEntry) Serialize() []byte {
	return []byte(e.Name() + "(TABLE=" + e.TableName + ",KEY=" + string(e.Key) + ",OLD_VALUE=" + string(e.OldValue) + ",NEW_VALUE=" + string(e.NewValue) + ")\n")
}

func (e *UpdateEntry) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

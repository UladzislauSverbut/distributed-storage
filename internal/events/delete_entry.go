package events

const DELETE_ENTRY_EVENT = "DELETE_ENTRY"

type DeleteEntry struct {
	TableName string
	Key       []byte
	Value     []byte
}

func (e *DeleteEntry) Name() string {
	return DELETE_ENTRY_EVENT
}

func (e *DeleteEntry) Serialize() []byte {
	return []byte(e.Name() + "(TABLE=" + e.TableName + ",KEY=" + string(e.Key) + ")\n")
}

func (e *DeleteEntry) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

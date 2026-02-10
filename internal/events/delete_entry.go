package events

const DELETE_ENTRY_EVENT = "DELETE_ENTRY"

type DeleteEntry struct {
	TableName string
	Key       []byte
	Value     []byte
}

func (event *DeleteEntry) Name() string {
	return DELETE_ENTRY_EVENT
}

func (event *DeleteEntry) Serialize() []byte {
	return []byte(event.Name() + "(TABLE=" + event.TableName + ",KEY=" + string(event.Key) + ")\n")
}

func (event *DeleteEntry) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

package events

const INSERT_ENTRY_EVENT = "INSERT_ENTRY"

type InsertEntry struct {
	TableName string
	Key       []byte
	Value     []byte
}

func (e *InsertEntry) Name() string {
	return INSERT_ENTRY_EVENT
}

func (e *InsertEntry) Serialize() []byte {
	return []byte(e.Name() + "(TABLE=" + e.TableName + ",KEY=" + string(e.Key) + ",VALUE=" + string(e.Value) + ")\n")
}

func (e *InsertEntry) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

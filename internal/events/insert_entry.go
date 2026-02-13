package events

const INSERT_ENTRY_EVENT = "INSERT_ENTRY"

type InsertEntry struct {
	TableName string
	Key       []byte
	Value     []byte
}

func NewInsertEntry(tableName string, key []byte, value []byte) *InsertEntry {
	return &InsertEntry{TableName: tableName, Key: key, Value: value}
}

func (event *InsertEntry) Name() string {
	return INSERT_ENTRY_EVENT
}

func (event *InsertEntry) Serialize() []byte {
	return []byte(event.Name() + "(TABLE=" + event.TableName + ",KEY=" + string(event.Key) + ",VALUE=" + string(event.Value) + ")\n")
}

func (e *InsertEntry) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

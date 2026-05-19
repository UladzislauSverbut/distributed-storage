package events

// UpdateTable describes update of a table. Schemas are stored as raw bytes.
type UpdateTable struct {
	TableID   uint64
	NewSchema []byte
	OldSchema []byte
}

func NewUpdateTable(tableID uint64, oldSchema []byte, newSchema []byte) *UpdateTable {
	return &UpdateTable{TableID: tableID, OldSchema: oldSchema, NewSchema: newSchema}
}

func (event *UpdateTable) Type() EventType {
	return UPDATE_TABLE_EVENT
}

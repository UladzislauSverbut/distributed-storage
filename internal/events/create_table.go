package events

// CreateTable describes creation of a table. Schema is stored as JSON.
type CreateTable struct {
	TableID uint64
	Schema  []byte // JSON-encoded TableSchema
}

func NewCreateTable(tableID uint64, schema []byte) *CreateTable {
	return &CreateTable{TableID: tableID, Schema: schema}
}

func (event *CreateTable) Type() EventType {
	return CREATE_TABLE_EVENT
}

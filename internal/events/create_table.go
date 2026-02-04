package events

import "encoding/json"

const CREATE_TABLE_EVENT = "CREATE_TABLE"

// CreateTable describes creation of a table. Schema is stored as JSON.
type CreateTable struct {
	TableName string
	Schema    []byte // JSON-encoded TableSchema
}

func (e *CreateTable) Name() string {
	return CREATE_TABLE_EVENT
}

func (e *CreateTable) Serialize() []byte {
	// Ensure schema is valid JSON; if not, best-effort marshal.
	if !json.Valid(e.Schema) {
		b, _ := json.Marshal(string(e.Schema))
		return []byte(e.Name() + "(TABLE=" + e.TableName + ",SCHEMA=" + string(b) + ")\n")
	}

	return []byte(e.Name() + "(TABLE=" + e.TableName + ",SCHEMA=" + string(e.Schema) + ")\n")
}

func (e *CreateTable) Parse(data []byte) error {
	// Will be implemented in the future when we will need to parse events from WAL
	return nil
}

package events

import "encoding/json"

const CREATE_TABLE_EVENT = "CREATE_TABLE"

// CreateTable describes creation of a table. Schema is stored as JSON.
type CreateTable struct {
	TableName string
	Schema    []byte // JSON-encoded TableSchema
}

func NewCreateTable(tableName string, schema []byte) *CreateTable {
	return &CreateTable{TableName: tableName, Schema: schema}
}

func (event *CreateTable) Name() string {
	return CREATE_TABLE_EVENT
}

func (event *CreateTable) Serialize() []byte {
	schema, _ := json.Marshal(string(event.Schema))
	return []byte(event.Name() + "(TABLE=" + event.TableName + ",SCHEMA=" + string(schema) + ")\n")

}

func (e *CreateTable) Parse(data []byte) error {
	// Will be implemented in the future when we will need to parse events from WAL
	return nil
}

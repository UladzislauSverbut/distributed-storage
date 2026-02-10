package events

import "encoding/json"

const UPDATE_TABLE_EVENT = "UPDATE_TABLE"

// UpdateTable describes update of a table. Schema is stored as JSON.
type UpdateTable struct {
	TableName string
	NewSchema []byte // JSON-encoded TableSchema
	OldSchema []byte // JSON-encoded TableSchema
}

func (event *UpdateTable) Name() string {
	return UPDATE_TABLE_EVENT
}

func (event *UpdateTable) Serialize() []byte {
	oldSchema, _ := json.Marshal(string(event.OldSchema))
	newSchema, _ := json.Marshal(string(event.NewSchema))

	return []byte(event.Name() + "(TABLE=" + event.TableName + ",OLD_SCHEMA=" + string(oldSchema) + ",NEW_SCHEMA=" + string(newSchema) + ")\n")
}

func (event *UpdateTable) Parse(data []byte) error {
	// Will be implemented in the future when we will need to parse events from WAL
	return nil
}

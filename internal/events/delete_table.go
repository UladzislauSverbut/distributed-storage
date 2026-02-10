package events

import "distributed-storage/internal/pager"

const DELETE_TABLE_EVENT = "DELETE_TABLE"

type DeleteTable struct {
	TableName string
	TableRoot pager.PagePointer
}

func (event *DeleteTable) Name() string {
	return DELETE_TABLE_EVENT
}

func (event *DeleteTable) Serialize() []byte {
	return []byte(event.Name() + "(TABLE=" + event.TableName + ")\n")
}

func (event *DeleteTable) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

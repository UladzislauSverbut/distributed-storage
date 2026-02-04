package events

import "distributed-storage/internal/pager"

const DELETE_TABLE_EVENT = "DELETE_TABLE"

type DeleteTable struct {
	TableName string
	TableRoot pager.PagePointer
}

func (e *DeleteTable) Name() string {
	return DELETE_TABLE_EVENT
}

func (e *DeleteTable) Serialize() []byte {
	return []byte(e.Name() + "(TABLE=" + e.TableName + ")\n")
}

func (e *DeleteTable) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

package events

type DropTable struct {
	TableID uint64
}

func NewDropTable(tableID uint64) *DropTable {
	return &DropTable{TableID: tableID}
}

func (event *DropTable) Type() EventType {
	return DROP_TABLE_EVENT
}

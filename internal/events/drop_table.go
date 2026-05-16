package events

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const DROP_TABLE_EVENT = "DROP_TABLE"

var dropTableParsingError = errors.New("DropTable: couldn't parse event")

type DropTable struct {
	TableID uint64
}

func NewDropTable(tableID uint64) *DropTable {
	return &DropTable{TableID: tableID}
}

func (event *DropTable) Name() string {
	return DROP_TABLE_EVENT
}

func (event *DropTable) Serialize() []byte {
	serializedEvent := []byte(event.Name())
	serializedTableID := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedTableID, event.TableID)

	serializedEvent = append(serializedEvent, serializedTableID...)

	return serializedEvent
}

func ParseDropTable(data []byte) (*DropTable, error) {
	offset := len(DROP_TABLE_EVENT)

	if !bytes.Equal(data[:offset], []byte(DROP_TABLE_EVENT)) {
		return nil, dropTableParsingError
	}

	tableID := binary.LittleEndian.Uint64(data[offset:])

	return NewDropTable(tableID), nil
}

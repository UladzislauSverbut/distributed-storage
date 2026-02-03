package db

import (
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/store"
	"fmt"
	"strconv"
)

type Wal struct {
	storage store.Storage
}

func NewWal(storage store.Storage) *Wal {
	return &Wal{
		storage: storage,
	}
}

func (wal *Wal) Write(events []Event) error {
	log := []byte{}
	for _, event := range events {
		serializedEvent := wal.serializeEvent(event)
		log = append(log, serializedEvent...)
	}

	return wal.storage.AppendMemorySegment(log)
}

func (wal *Wal) serializeEvent(event Event) []byte {
	switch e := event.(type) {
	case *StartTransaction:
		return []byte(e.Name() + "(TX=" + strconv.FormatUint(uint64(e.TxID), 10) + ")\n")
	case *CommitTransaction:
		return []byte(e.Name() + "(TX=" + strconv.FormatUint(uint64(e.TxID), 10) + ")\n")
	case *CreateTable:
		return []byte(e.Name() + "(TABLE=" + e.TableName + ")\n")
	case *DeleteTable:
		return []byte(e.Name() + "(TABLE=" + e.TableName + ")\n")
	case *UpsertEntry:
		return []byte(e.Name() + "(TABLE=" + e.TableName + ",KEY=" + string(e.Key) + ",VALUE=" + string(e.NewValue) + ")\n")
	case *DeleteEntry:
		return []byte(e.Name() + "(TABLE=" + e.TableName + ",KEY=" + string(e.Key) + ")\n")
	case *UpdateEntry:
		return []byte(e.Name() + "(TABLE=" + e.TableName + ",KEY=" + string(e.Key) + ",NEW_VALUE=" + string(e.NewValue) + ")\n")
	case *InsertEntry:
		return []byte(e.Name() + "(TABLE=" + e.TableName + ",KEY=" + string(e.Key) + ",VALUE=" + string(e.Value) + ")\n")
	case *FreePages:
		return []byte(e.Name() + "(PAGES=" + helpers.StringifySlice(e.Pages, func(page uint64) string { return strconv.FormatUint(page, 10) }, ",") + ")\n")
	default:
		panic(fmt.Sprintf("Wal: unknown event type %s", event.Name()))
	}
}

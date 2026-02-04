package db

import (
	"distributed-storage/internal/store"
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
		log = append(log, event.Serialize()...)
	}

	return wal.storage.AppendMemorySegment(log)
}

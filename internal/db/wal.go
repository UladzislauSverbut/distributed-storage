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
	// Implementation for writing events to the WAL
	return nil
}

package db

import (
	"distributed-storage/internal/store"
	"fmt"
)

type Wal struct {
	storage store.Storage
}

func NewWal(storage store.Storage) *Wal {
	return &Wal{
		storage: storage,
	}
}

func (wal *Wal) Write(events []TableEvent) error {
	log := []byte{}
	for _, event := range events {
		log = append(log, event.Serialize()...)
	}

	if err := wal.storage.AppendMemorySegment(log); err != nil {
		return fmt.Errorf("Wal: failed to write WAL segment %w", err)
	}

	if err := wal.storage.Flush(); err != nil {
		return fmt.Errorf("Wal: failed to flush WAL segment %w", err)
	}

	return nil
}

package db

import (
	"distributed-storage/internal/events"
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	"fmt"
)

type WAL struct {
	pendinLog []byte
	storage   store.Storage
}

func NewWAL(storage store.Storage) *WAL {
	return &WAL{
		storage: storage,
	}
}

func (wal *WAL) WriteTransactions(transactions []TransactionCommit) {
	if len(transactions) == 0 {
		return
	}

	for _, transaction := range transactions {
		wal.pendinLog = append(wal.pendinLog, events.NewStartTransaction(uint64(transaction.ID)).Serialize()...)

		for _, event := range transaction.ChangeEvents {
			wal.pendinLog = append(wal.pendinLog, event.Serialize()...)
		}

		wal.pendinLog = append(wal.pendinLog, events.NewCommitTransaction(uint64(transaction.ID)).Serialize()...)
	}
}

func (wal *WAL) WriteVersion(version DatabaseVersion) {
	wal.pendinLog = append(wal.pendinLog, events.NewUpdateDBVersion(uint64(version)).Serialize()...)
}

func (wal *WAL) WriteFreePages(version DatabaseVersion, pages []pager.PagePointer) {
	if len(pages) == 0 {
		return
	}

	wal.pendinLog = append(wal.pendinLog, events.NewFreePages(uint64(version), pages).Serialize()...)
}

func (wal *WAL) LatestVersion() (DatabaseVersion, error) {
	return 0, nil // TODO: Implement this method to read the latest version from the WAL
}

func (wal *WAL) ChangesSince(version DatabaseVersion) ([]TableEvent, error) {
	return nil, nil // TODO: Implement this method to read all changes since the given version from the WAL
}

func (wal *WAL) Flush() error {
	defer func() { wal.pendinLog = nil }()

	if err := wal.storage.AppendMemorySegment(wal.pendinLog); err != nil {
		return fmt.Errorf("WAL: failed to write WAL segment %w", err)
	}

	return wal.storage.Flush()
}

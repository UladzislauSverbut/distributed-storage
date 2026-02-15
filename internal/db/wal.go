package db

import (
	"distributed-storage/internal/events"
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	"encoding/binary"
	"hash/crc32"

	"fmt"
)

type WAL struct {
	pendingLog []byte
	storage    store.Storage
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
		wal.addLogRow(events.NewStartTransaction(uint64(transaction.ID)).Serialize())

		for _, event := range transaction.ChangeEvents {
			wal.addLogRow(event.Serialize())
		}

		wal.addLogRow(events.NewCommitTransaction(uint64(transaction.ID)).Serialize())
	}
}

func (wal *WAL) WriteVersion(version DatabaseVersion) {
	wal.addLogRow(events.NewUpdateDBVersion(uint64(version)).Serialize())
}

func (wal *WAL) WriteFreePages(version DatabaseVersion, pages []pager.PagePointer) {
	if len(pages) == 0 {
		return
	}

	wal.addLogRow(events.NewFreePages(uint64(version), pages).Serialize())
}

func (wal *WAL) LatestVersion() (DatabaseVersion, error) {
	return 0, nil // TODO: Implement this method to read the latest version from the WAL
}

func (wal *WAL) ChangesSince(version DatabaseVersion) ([]TableEvent, error) {
	return nil, nil // TODO: Implement this method to read all changes since the given version from the WAL
}

func (wal *WAL) Flush() error {
	defer func() { wal.pendingLog = nil }()

	if err := wal.storage.AppendMemorySegment(wal.pendingLog); err != nil {
		return fmt.Errorf("WAL: failed to write WAL segment %w", err)
	}

	return wal.storage.Flush()
}

func (wal *WAL) Truncate(version DatabaseVersion) error {
	return nil // TODO: Implement this method to truncate the WAL up to the given version
}

func (wal *WAL) addLogRow(log []byte) {
	size := len(log)
	hash := make([]byte, 4)
	binary.BigEndian.PutUint32(hash, crc32.ChecksumIEEE(log))

	wal.pendingLog = append(wal.pendingLog, byte(size))
	wal.pendingLog = append(wal.pendingLog, hash...)
	wal.pendingLog = append(wal.pendingLog, log...)
}

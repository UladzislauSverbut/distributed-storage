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
		wal.addEventToLog(events.NewStartTransaction(uint64(transaction.ID)))

		for _, event := range transaction.ChangeEvents {
			wal.addEventToLog(event)
		}

		wal.addEventToLog(events.NewCommitTransaction(uint64(transaction.ID)))
	}
}

func (wal *WAL) WriteVersion(version DatabaseVersion) {
	wal.addEventToLog(events.NewUpdateDBVersion(uint64(version)))
}

func (wal *WAL) WriteFreePages(version DatabaseVersion, pages []pager.PagePointer) {
	if len(pages) == 0 {
		return
	}

	wal.addEventToLog(events.NewFreePages(uint64(version), pages))
}

func (wal *WAL) LatestVersion() (DatabaseVersion, error) {

	return 0, nil // TODO: Implement this method to read the latest version from the WAL
}

func (wal *WAL) ChangesSince(version DatabaseVersion) ([]TableEvent, error) {
	return nil, nil // TODO: Implement this method to read all changes since the given version from the WAL
}

func (wal *WAL) Flush() error {
	defer func() { wal.pendingLog = nil }()

	if err := wal.storage.AppendSegmentAndFlush(wal.pendingLog); err != nil {
		return fmt.Errorf("WAL: failed to write WAL segment %w", err)
	}

	return nil
}

func (wal *WAL) Truncate(version DatabaseVersion) error {
	return nil // TODO: Implement this method to truncate the WAL up to the given version
}

func (wal *WAL) addEventToLog(event TableEvent) {
	wal.pendingLog = append(wal.pendingLog, wal.eventToRow(event)...)
}

func (wal *WAL) eventToRow(event TableEvent) []byte {
	data := event.Serialize()
	size := uint32(len(data))

	row := make([]byte, 8+size+1) // 8 Bytes for size and hash, then the event data, and 1 byte for a newline character

	binary.BigEndian.PutUint32(row, size)
	binary.BigEndian.PutUint32(row[4:], crc32.ChecksumIEEE(data))

	copy(row[8:], data)
	row[len(row)-1] = '\n' // Add a newline character at the end of the row

	return row
}

func (wal *WAL) rowToEvent(row []byte) (TableEvent, error) {
	if len(row) < 8 {
		return nil, fmt.Errorf("WAL: invalid row size")
	}

	size := binary.BigEndian.Uint32(row)
	hash := binary.BigEndian.Uint32(row[4:8])

	if uint32(len(row[8:])) != size {
		return nil, fmt.Errorf("WAL: row size mismatch")
	}

	data := row[8:]
	if crc32.ChecksumIEEE(data) != hash {
		return nil, fmt.Errorf("WAL: row checksum mismatch")
	}

	event, err := events.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("WAL: failed to deserialize event: %w", err)
	}

	return event, nil
}

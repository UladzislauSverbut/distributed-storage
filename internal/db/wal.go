package db

import (
	"distributed-storage/internal/events"
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/pager"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"fmt"
)

const SEGMENT_NAME_FORMAT = "segment_%010d.wal"

type SegmentID uint64

type WAL struct {
	segment         *os.File
	segmentID       SegmentID
	segmentSize     int
	segmentCapacity int64

	directory        string
	archiveDirectory string

	pendingLog         []byte
	pendingArchivation bool

	mu sync.Mutex
}

func NewWAL(directory string, archiveDirectory string, segmentSize int) (*WAL, error) {
	wal := &WAL{
		segmentSize: segmentSize,

		directory:        directory,
		archiveDirectory: archiveDirectory,

		pendingLog:         []byte{},
		pendingArchivation: false,
	}

	var err error

	if wal.segmentID, err = wal.findSegment(directory); err != nil {
		return nil, fmt.Errorf("WAL: failed to find existing segment: %w", err)
	}

	if wal.segmentID == 0 {
		if wal.segmentID, err = wal.findSegment(archiveDirectory); err != nil {
			return nil, fmt.Errorf("WAL: failed to find existing segment in archive directory: %w", err)
		}
	}

	if wal.segment, wal.segmentCapacity, err = wal.openSegmentOrCreate(wal.segmentID); err != nil {
		return nil, fmt.Errorf("WAL: failed to open existing segment file: %w", err)
	}

	return wal, nil
}

func (wal *WAL) AppendTransactions(transactions []TransactionCommit) {
	if len(transactions) == 0 {
		return
	}

	for _, transaction := range transactions {
		wal.AppendEvent(events.NewStartTransaction(uint64(transaction.ID)))

		for _, event := range transaction.ChangeEvents {
			wal.AppendEvent(event)
		}

		wal.AppendEvent(events.NewCommitTransaction(uint64(transaction.ID)))
	}
}

func (wal *WAL) AppendVersionUpdate(version DatabaseVersion) {
	wal.AppendEvent(events.NewUpdateDBVersion(uint64(version)))
}

func (wal *WAL) AppendFreePages(version DatabaseVersion, pages []pager.PagePointer) {
	if len(pages) == 0 {
		return
	}

	wal.AppendEvent(events.NewFreePages(uint64(version), pages))
}

func (wal *WAL) LatestUpdatedVersion() (DatabaseVersion, error) {
	return 0, nil // TODO: Implement this method to read the latest version from the WAL
}

func (wal *WAL) ChangesSince(version DatabaseVersion) ([]TableEvent, error) {
	return nil, nil // TODO: Implement this method to read all changes since the given version from the WAL
}

func (wal *WAL) AppendEvent(event TableEvent) {
	wal.pendingLog = append(wal.pendingLog, wal.decodeEvent(event)...)
}

func (wal *WAL) Sync() error {
	wal.mu.Lock()

	defer func() {
		wal.pendingLog = []byte{}
		wal.mu.Unlock()
	}()

	if _, err := wal.segment.Write(wal.pendingLog); err != nil {
		return fmt.Errorf("WAL: failed to write WAL segment %w", err)
	}

	if err := wal.segment.Sync(); err != nil {
		return fmt.Errorf("WAL: failed to sync WAL segment %w", err)
	}

	wal.segmentCapacity += int64(len(wal.pendingLog))

	if !wal.pendingArchivation && wal.segmentFull(wal.segmentCapacity) {
		wal.pendingArchivation = true
		go wal.archiveSegment()
	}

	return nil
}

func (wal *WAL) decodeEvent(event TableEvent) []byte {
	data := event.Serialize()
	size := uint32(len(data))

	row := make([]byte, 8+size+1) // 8 Bytes for size and hash, then the event data, and 1 byte for a newline character

	binary.BigEndian.PutUint32(row, size)
	binary.BigEndian.PutUint32(row[4:], crc32.ChecksumIEEE(data))

	copy(row[8:], data)
	row[len(row)-1] = '\n' // Add a newline character at the end of the row

	return row
}

func (wal *WAL) encodeEvent(row []byte) (TableEvent, error) {
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

func (WAL *WAL) findSegment(directory string) (SegmentID, error) {
	segmentID := SegmentID(0)

	entries, err := os.ReadDir(directory)
	if err != nil {
		return segmentID, fmt.Errorf("WAL: failed to read directory: %w", err)
	}

	for idx := len(entries) - 1; idx >= 0; idx-- {
		entry := entries[idx]

		if parsed, _ := fmt.Sscanf(entry.Name(), SEGMENT_NAME_FORMAT, &segmentID); parsed == 1 {
			break
		}
	}

	return segmentID, nil
}

func (wal *WAL) openSegmentOrCreate(segmentID SegmentID) (segment *os.File, capacity int64, err error) {
	defer func() {
		if err != nil && segment != nil {
			segment.Close()
		}
	}()

	if segment, err = os.OpenFile(wal.segmentName(wal.directory, segmentID), os.O_RDWR|os.O_CREATE, 0644); err != nil {
		err = fmt.Errorf("WAL: failed to open segment file: %w", err)
		return
	}

	if capacity, err = segment.Seek(0, io.SeekEnd); err != nil {
		err = fmt.Errorf("WAL: failed to seek to end of segment: %w", err)
		return
	}

	if _, err = helpers.IncreaseFileSize(segment, wal.segmentSize); err != nil {
		err = fmt.Errorf("WAL: failed to set segment file size: %w", err)
		return
	}

	return
}

func (wal *WAL) archiveSegment() error {
	archivedSegment := wal.segment
	archivedSegmentID := wal.segmentID
	newSegmentID := wal.segmentID + 1

	segment, capacity, err := wal.openSegmentOrCreate(newSegmentID)

	if err != nil {
		return fmt.Errorf("WAL: failed to archive active segment: %w", err)
	}

	wal.mu.Lock()
	wal.segment = segment
	wal.segmentID = newSegmentID
	wal.segmentCapacity = capacity
	wal.pendingArchivation = false
	wal.mu.Unlock()

	if err := os.Rename(archivedSegment.Name(), wal.segmentName(wal.archiveDirectory, archivedSegmentID)); err != nil {
		fmt.Printf("WAL: failed to move archived segment to archive directory: %v\n", err)
	}

	if err := archivedSegment.Close(); err != nil {
		fmt.Printf("WAL: failed to close archived segment: %v\n", err)
	}

	return nil
}

func (wal *WAL) segmentName(directory string, segmentID SegmentID) string {
	return fmt.Sprintf("%s/"+SEGMENT_NAME_FORMAT, directory, segmentID)
}

func (wal *WAL) segmentFull(capacity int64) bool {
	return capacity >= int64(wal.segmentSize)
}

package db

import (
	"bytes"
	"distributed-storage/internal/events"
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/pager"
	"encoding/binary"
	"hash/crc32"
	"io"
	"iter"
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

	pendingLog     []byte
	pendingArchive bool

	mu sync.Mutex
}

func newWAL(config DatabaseConfig) (*WAL, error) {
	wal := &WAL{
		segmentSize: config.WALSegmentSize,

		directory:        config.WALDirectory,
		archiveDirectory: config.WALArchiveDirectory,

		pendingLog:     []byte{},
		pendingArchive: false,
	}

	var err error

	if wal.segmentID, err = wal.findSegment(wal.directory); err != nil {
		return nil, fmt.Errorf("WAL: failed to find existing segment: %w", err)
	}

	if wal.segmentID == 0 {
		if wal.segmentID, err = wal.findSegment(wal.archiveDirectory); err != nil {
			return nil, fmt.Errorf("WAL: failed to find existing segment in archive directory: %w", err)
		}
	}

	if wal.segment, wal.segmentCapacity, err = wal.openSegmentOrCreate(wal.segmentID); err != nil {
		return nil, fmt.Errorf("WAL: failed to open existing segment file: %w", err)
	}

	return wal, nil
}

func (wal *WAL) appendTransactions(transactions []TransactionCommit) {
	if len(transactions) == 0 {
		return
	}

	for _, transaction := range transactions {
		wal.appendEvent(events.NewStartTransaction(uint64(transaction.ID)))

		for _, event := range transaction.ChangeEvents {
			wal.appendEvent(event)
		}

		wal.appendEvent(events.NewCommitTransaction(uint64(transaction.ID)))
	}
}

func (wal *WAL) appendVersionUpdate(version DatabaseVersion) {
	wal.appendEvent(events.NewUpdateDBVersion(uint64(version)))
}

func (wal *WAL) appendFreePages(version DatabaseVersion, pages []pager.PagePointer) {
	if len(pages) == 0 {
		return
	}

	wal.appendEvent(events.NewFreePages(uint64(version), pages))
}

func (wal *WAL) latestUpdatedVersion() (DatabaseVersion, error) {

	return 0, nil // TODO: Implement this method to read the latest version from the WAL
}

func (wal *WAL) changesSince(version DatabaseVersion) ([]TableEvent, error) {
	return nil, nil // TODO: Implement this method to read all changes since the given version from the WAL
}

func (wal *WAL) appendEvent(event TableEvent) {
	wal.pendingLog = append(wal.pendingLog, wal.decodeEvent(event)...)
}

func (wal *WAL) sync() error {
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

	if !wal.pendingArchive && wal.segmentFull(wal.segmentCapacity) {
		wal.pendingArchive = true
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

	if segment, err = os.OpenFile(wal.segmentName(wal.directory, segmentID), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644); err != nil {
		err = fmt.Errorf("WAL: failed to open segment file: %w", err)
		return
	}

	stat, err := segment.Stat()
	if err != nil {
		err = fmt.Errorf("WAL: failed to stat segment: %w", err)
		return
	}

	capacity = stat.Size()

	if capacity > 0 {
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
	wal.pendingArchive = false
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

func (wal *WAL) scanSegment(segment *os.File) iter.Seq[TableEvent] {
	cursor := wal.segmentCapacity
	buffer := make([]byte, 8096) // 4KB buffer for reading the segment file

	return func(yield func(TableEvent) bool) {
		for cursor > 0 {
			cursor -= int64(len(buffer))
			readBytes, err := segment.ReadAt(buffer, cursor)

			if err == io.EOF {
				return
			}

			lines := bytes.Split(buffer[:readBytes], []byte{'\n'})

			for idx := len(lines) - 2; idx >= 0; idx-- { // Skip last line because it's always empty due to the trailing newline character
				line := lines[idx]

				if event, err := wal.encodeEvent(line); err != nil {
					fmt.Printf("WAL: failed to decode event from segment: %v\n", err)
					return
				} else if !yield(event) {
					return
				}
			}
		}
	}
}

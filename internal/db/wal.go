package db

import (
	"bufio"
	"distributed-storage/internal/events"
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/pager"
	"encoding/binary"
	"hash/crc32"
	"io"
	"iter"
	"os"
	"slices"
	"sync"

	"fmt"
)

const SEGMENT_NAME_FORMAT = "segment_%010d.wal"
const INITIAL_SEGMENT_ID SegmentID = 1

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
	var segmentFound bool

	if wal.segmentID, segmentFound, err = wal.latestSegmentID(wal.directory); err != nil { // We will reuse the latest active segment if it exists
		return nil, fmt.Errorf("WAL: failed to find existing segment: %w", err)
	}

	if !segmentFound { // If there is no existing active segment, check for the latest archived
		if wal.segmentID, segmentFound, err = wal.latestSegmentID(wal.archiveDirectory); err != nil {
			return nil, fmt.Errorf("WAL: failed to find existing segment in archive directory: %w", err)
		}

		if segmentFound {
			wal.segmentID++ // Start with a new segment ID as the latest archived segment is considered full and should not be reused
		}
	}

	if wal.segment, wal.segmentCapacity, err = wal.openSegment(wal.segmentID, wal.directory); err != nil {
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

func (wal *WAL) eventsSince(version DatabaseVersion) ([]TableEvent, error) {
	wal.mu.Lock()         // Lock the WAL to prevent concurrent archiving current segment and closing files while reading changes.
	defer wal.mu.Unlock() // This method is called during db initialization, so no need to worry about performance implications of locking here.

	segment := wal.segment
	segmentID := wal.segmentID

	changes := [][]TableEvent{}
	changesFound := false

	for {
		segmentChanges := []TableEvent{}

		for event := range wal.scanSegment(segment) {
			segmentChanges = append(segmentChanges, event)

			if versionEvent, ok := event.(*events.UpdateDBVersion); ok && versionEvent.Version == uint64(version) {
				changesFound = true
				segmentChanges = nil // Clear changes collected so far as they are from a previous version
			}
		}

		if changesFound || segmentID == INITIAL_SEGMENT_ID { // If changes for the requested version are found or we have reached the initial segment, stop scanning further
			break
		}

		nextSegment, _, err := wal.openSegment(segmentID-1, wal.archiveDirectory)
		if err != nil {
			return nil, fmt.Errorf("WAL: failed to open previous segment file: %w", err)
		}

		defer nextSegment.Close()

		segment = nextSegment
		segmentID -= 1
		changes = append(changes, segmentChanges)
	}

	if !changesFound {
		return nil, fmt.Errorf("WAL: no changes found for version %d", version)
	}

	slices.Reverse(changes)

	return helpers.Flatten(changes), nil
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

func (wal *WAL) empty() bool {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.segmentID == INITIAL_SEGMENT_ID && wal.segmentCapacity == 0
}

func (wal *WAL) decodeEvent(event TableEvent) []byte {
	data := event.Serialize()
	size := uint32(len(data))

	row := make([]byte, 8+size) // 8 Bytes for size and hash, then the event data

	binary.LittleEndian.PutUint32(row, size)
	binary.LittleEndian.PutUint32(row[4:], crc32.ChecksumIEEE(data))

	copy(row[8:], data)

	return row
}

func (wal *WAL) encodeEvent(row []byte) (TableEvent, int, error) {
	if len(row) < 8 {
		return nil, 0, nil
	}

	size := binary.LittleEndian.Uint32(row)
	hash := binary.LittleEndian.Uint32(row[4:8])

	if len(row) < int(8+size) {
		return nil, 0, nil
	}

	data := row[8 : size+8]

	if crc32.ChecksumIEEE(data) != hash {
		return nil, 0, fmt.Errorf("WAL: row checksum mismatch")
	}

	event, err := events.Parse(data)
	if err != nil {
		return nil, 0, fmt.Errorf("WAL: failed to deserialize event: %w", err)
	}

	return event, int(size) + 8, nil
}

func (wal *WAL) latestSegmentID(directory string) (SegmentID, bool, error) {
	segmentID := INITIAL_SEGMENT_ID

	entries, err := os.ReadDir(directory)
	if err != nil {
		return segmentID, false, fmt.Errorf("WAL: failed to read directory: %w", err)
	}

	for idx := len(entries) - 1; idx >= 0; idx-- {
		entry := entries[idx]

		if parsed, _ := fmt.Sscanf(entry.Name(), SEGMENT_NAME_FORMAT, &segmentID); parsed == 1 {
			return segmentID, true, nil
		}
	}

	return segmentID, false, nil
}

func (wal *WAL) openSegment(segmentID SegmentID, directory string) (segment *os.File, capacity int64, err error) {
	defer func() {
		if err != nil && segment != nil {
			segment.Close()
		}
	}()

	if segment, err = os.OpenFile(wal.segmentName(directory, segmentID), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644); err != nil {
		err = fmt.Errorf("WAL: failed to open segment file: %w", err)
		return
	}

	return
}

func (wal *WAL) archiveSegment() error {
	archivedSegment := wal.segment
	archivedSegmentID := wal.segmentID
	newSegmentID := wal.segmentID + 1

	segment, capacity, err := wal.openSegment(newSegmentID, wal.directory)

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
	return func(yield func(TableEvent) bool) {
		const chunkSize = 4096 // Read the segment in chunks to avoid loading the entire file into memory

		reader := bufio.NewReaderSize(segment, chunkSize)

		accumulator := make([]byte, 0, 2*chunkSize)
		chunk := make([]byte, chunkSize)

		for {
			readBytes, err := reader.Read(chunk)
			accumulator = append(accumulator, chunk[:readBytes]...)

			for len(accumulator) > 0 {
				event, consumed, err := wal.encodeEvent(accumulator)

				if err != nil {
					fmt.Printf("WAL: failed to decode event: %v\n", err)
					return
				}

				if consumed == 0 {
					break // Need to read more data to decode a full event
				}

				accumulator = accumulator[consumed:]

				if !yield(event) {
					return
				}
			}

			if err == io.EOF {
				return
			}
		}
	}
}

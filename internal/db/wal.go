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

const ARCHIVE_SUB_DIRECTORY = "archive"
const SEGMENT_NAME_FORMAT = "segment_%010d.wal"

type SegmentID uint64

type WAL struct {
	segment          *os.File
	segmentID        SegmentID
	segmentSize      int
	segmentOffset    int64
	segmentDirectory string

	pendingLog []byte

	mu sync.Mutex
}

func NewWAL(directory string, segmentSize int) (*WAL, error) {
	if err := os.Mkdir(directory, 0755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("WAL: failed to create segment directory: %w", err)
	}

	if err := os.Mkdir(directory+"/"+ARCHIVE_SUB_DIRECTORY+"/", 0755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("WAL: failed to create archive subdirectory: %w", err)
	}

	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("WAL: failed to read segment directory: %w", err)
	}

	var segmentID SegmentID = 0

	for _, entry := range entries {
		if parsed, _ := fmt.Sscanf(entry.Name(), "segment_%d.wal", &segmentID); parsed == 1 {
			break
		}
	}

	if segmentID != 0 {
		wal := &WAL{
			segmentID:        segmentID,
			segmentSize:      segmentSize,
			segmentDirectory: directory,
			pendingLog:       []byte{},
		}

		if wal.segment, err = os.OpenFile(fmt.Sprintf("%s/"+SEGMENT_NAME_FORMAT, directory, segmentID), os.O_RDWR, 0644); err != nil {
			return nil, fmt.Errorf("WAL: failed to open existing segment file: %w", err)
		}

		if wal.segmentOffset, err = wal.segment.Seek(0, io.SeekEnd); err != nil {
			return nil, fmt.Errorf("WAL: failed to seek to end of segment: %w", err)
		}

		return wal, nil
	}

	archivedEntries, err := os.ReadDir(directory + "/" + ARCHIVE_SUB_DIRECTORY)
	if err != nil {
		return nil, fmt.Errorf("WAL: failed to read archive subdirectory: %w", err)
	}

	if len(archivedEntries) > 0 {
		lastEntryIndex := len(archivedEntries) - 1 // Get the last entry in the archive directory because they are sorted by name, and the name contains the segment ID in increasing order

		for entryIndex := lastEntryIndex; entryIndex >= 0; entryIndex-- {
			if parsed, _ := fmt.Sscanf(archivedEntries[entryIndex].Name(), "segment_%d.wal", &segmentID); parsed == 1 {
				break
			}
		}
	}

	segmentID++ // Start with the next segment ID after the last one found in the archive directory

	segment, err := os.OpenFile(fmt.Sprintf("%s/"+SEGMENT_NAME_FORMAT, directory, segmentID), os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, fmt.Errorf("WAL: failed to create new segment file: %w", err)
	}

	if _, err := helpers.IncreaseFileSize(segment, segmentSize); err != nil {
		return nil, fmt.Errorf("WAL: failed to increase segment file size: %w", err)
	}

	return &WAL{
		segment:          segment,
		segmentID:        segmentID,
		segmentSize:      segmentSize,
		segmentOffset:    0,
		segmentDirectory: directory,
		pendingLog:       []byte{},
	}, nil
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

	if _, err := wal.segment.WriteAt(wal.pendingLog, wal.segmentOffset); err != nil {
		return fmt.Errorf("WAL: failed to write WAL segment %w", err)
	}

	if err := wal.segment.Sync(); err != nil {
		return fmt.Errorf("WAL: failed to sync WAL segment %w", err)
	}

	wal.segmentOffset += int64(len(wal.pendingLog))

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

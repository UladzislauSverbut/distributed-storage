package wal

import (
	"os"
	"sync"

	"fmt"
)

const INITIAL_ENTRY_INDEX EntryIndex = 0

type WALConfig struct {
	Directory        string
	ArchiveDirectory string
	SegmentCapacity  int
}

type WAL struct {
	buffer           [][]byte
	segment          *Segment
	lastSyncedEntry  EntryIndex
	segmentArchiving bool

	directory        string
	archiveDirectory string

	mu sync.Mutex
}

func newWAL(config WALConfig) (*WAL, error) {
	wal := &WAL{
		directory:        config.Directory,
		archiveDirectory: config.ArchiveDirectory,
	}

	segment, err := wal.latestSegment(wal.directory) // We will reuse the latest active segment if it exists
	if err != nil {
		return nil, fmt.Errorf("WAL: failed to find active segment: %w", err)
	}

	if segment != nil {
		wal.segment = segment
		wal.entryIndex = segment.LastFlushed + 1
		return wal, nil
	}

	// If there is no existing active segment, check for the latest archived
	segment, err = wal.latestSegment(wal.archiveDirectory)
	if err != nil {
		return nil, fmt.Errorf("WAL: failed to find archived segment: %w", err)
	}

	if segment != nil {
		wal.entryIndex = segment.LastIndex + 1
		go segment.Close() // Close the archived segment since we won't be using it
	} else {
		wal.entryIndex = INITIAL_ENTRY_INDEX
	}

	wal.segment, err = NewSegment(wal.entryIndex, wal.directory, config.SegmentCapacity)
	if err != nil {
		return nil, fmt.Errorf("WAL: failed to create new segment: %w", err)
	}

	return wal, nil
}

func (wal *WAL) Append(record []byte) {
	wal.buffer = append(wal.buffer, record)
}

func (wal *WAL) Sync() (EntryIndex, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if err := wal.segment.Write(wal.buffer); err != nil {
		return 0, fmt.Errorf("WAL: failed to write buffer to segment: %w", err)
	}

	if wal.segment.IsFull() && !wal.segmentArchiving {
		wal.segmentArchiving = true

		go wal.archiveSegment()
	}

	return wal.segment.LastFlushed, nil
}

func (wal *WAL) Empty() bool {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.segment.LastFlushed == INITIAL_ENTRY_INDEX
}

func (wal *WAL) latestSegment(directory string) (*Segment, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("WAL: failed to read %q directory: %w", directory, err)
	}

	for idx := len(entries) - 1; idx >= 0; idx-- {
		entry := entries[idx]

		if index, err := parseSegmentName(entry.Name()); err == nil {
			return NewSegment(index, directory, wal.segment.Capacity)
		}
	}

	return nil, nil
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

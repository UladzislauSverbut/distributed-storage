package wal

import (
	"fmt"
	"iter"
	"os"
	"strings"
	"sync"
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
	segmentArchiving bool

	config WALConfig
	mu     sync.Mutex
}

func NewWAL(config WALConfig) (*WAL, error) {
	wal := &WAL{config: config}

	segment, err := wal.latestSegment(config.Directory)
	if err != nil {
		return nil, fmt.Errorf("WAL: failed to find active segment: %w", err)
	}

	if segment != nil {
		wal.segment = segment
		return wal, nil
	}

	segment, err = wal.latestSegment(config.ArchiveDirectory)
	if err != nil {
		return nil, fmt.Errorf("WAL: failed to find archived segment: %w", err)
	}

	startIndex := INITIAL_ENTRY_INDEX
	if segment != nil {
		startIndex = segment.Last
		go segment.Close()
	}

	wal.segment, err = NewSegment(startIndex, config.Directory, config.SegmentCapacity)
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
	wal.buffer = wal.buffer[:0]

	if wal.segment.IsFull() && !wal.segmentArchiving {
		wal.segmentArchiving = true
		go wal.archiveSegment()
	}

	return wal.segment.Last, nil
}

func (wal *WAL) Since(entry EntryIndex) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		wal.mu.Lock()
		defer wal.mu.Unlock()

		descriptors, err := readSegments(wal.config.Directory)
		if err != nil {
			yield(nil, fmt.Errorf("WAL: failed to read all active segments: %w", err))
			return
		}

		if !contains(descriptors, entry) {
			archivedDescriptors, err := readSegments(wal.config.ArchiveDirectory)
			if err != nil {
				yield(nil, fmt.Errorf("WAL: failed to read all archive segments: %w", err))
				return
			}
			descriptors = append(archivedDescriptors, descriptors...)
		}

		if !contains(descriptors, entry) {
			yield(nil, fmt.Errorf("WAL: records started from index %d are missed", entry))
			return
		}

		for _, descriptor := range descriptors {
			seg, err := OpenSegment(descriptor.path(), descriptor.start, wal.config.SegmentCapacity)
			if err != nil {
				yield(nil, err)
				return
			}
			for entry, err := range seg.Since(entry) {
				if err != nil {
					seg.Close()
					yield(nil, err)
					return
				}
				if !yield(entry.record, nil) {
					seg.Close()
					return
				}
			}
			seg.Close()
		}
	}
}

func (wal *WAL) Empty() bool {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.segment.Last == INITIAL_ENTRY_INDEX
}

func (wal *WAL) Last() EntryIndex {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.segment.Last
}

func (wal *WAL) latestSegment(directory string) (*Segment, error) {
	descriptors, err := readSegments(directory)
	if err != nil {
		return nil, fmt.Errorf("WAL: failed to read latest segment in %q directory: %w", directory, err)
	}

	if len(descriptors) == 0 {
		return nil, nil
	}

	latest := descriptors[len(descriptors)-1]

	return NewSegment(latest.start, latest.directory, wal.config.SegmentCapacity)
}

func (wal *WAL) archiveSegment() {
	wal.mu.Lock()

	archivedSegment := wal.segment
	segment, err := NewSegment(wal.segment.Last, wal.config.Directory, wal.config.SegmentCapacity)

	if err != nil {
		fmt.Printf("WAL: failed to create new segment during archive: %v\n", err)
		wal.segmentArchiving = false
		wal.mu.Unlock()
		return
	}

	wal.segment = segment
	wal.segmentArchiving = false
	wal.mu.Unlock()

	oldPath := archivedSegment.File.Name()
	newPath := strings.Replace(oldPath, wal.config.Directory, wal.config.ArchiveDirectory, 1)

	if err := os.Rename(oldPath, newPath); err != nil {
		fmt.Printf("WAL: failed to move archived segment to archive directory: %v\n", err)
	}

	if err := archivedSegment.Close(); err != nil {
		fmt.Printf("WAL: failed to close archived segment: %v\n", err)
	}
}

package wal

import (
	"distributed-storage/internal/codec"
	"distributed-storage/internal/helpers"
	"fmt"
	"os"
)

const SEGMENT_NAME_FORMAT = "segment_%010d.wal"
const SEGMENT_META_NAME_FORMAT = "segment_%010d.meta"
const SEGMENT_CAPACITY = 1024 * 1024 * 10 // 10 MB

type EntryIndex uint64
type SegmentID uint64

type Segment struct {
	ID       SegmentID
	File     *os.File
	Size     int
	Capacity int
}

func NewSegment(directory string, id SegmentID) (*Segment, EntryIndex, error) {
	segmentFileName := fmt.Sprintf("%s/"+SEGMENT_NAME_FORMAT, directory, id)
	segmentFile, err := os.Create(segmentFileName)

	if err != nil {
		return nil, INITIAL_LAST_ENTRY_INDEX, fmt.Errorf("WAL Segment: failed to create segment file: %w", err)
	}

	segment := &Segment{
		ID:       id,
		File:     segmentFile,
		Capacity: SEGMENT_CAPACITY,
		Size:     0,
	}

	lastIndex, err := segment.lastIndex()
	if err != nil {
		return nil, INITIAL_LAST_ENTRY_INDEX, fmt.Errorf("WAL Segment: failed to find last index during initialization: %w", err)
	}

	return segment, lastIndex, nil
}

func (segment *Segment) AppendEntry(index EntryIndex, entry []byte) error {
	walEntry := codec.EncodeWALEntry(uint64(index), entry)

	written, err := segment.File.Write(walEntry)
	if err != nil {
		return fmt.Errorf("WAL Segment: failed to write entry to segment file: %w", err)
	}

	segment.Size += written

	return nil
}

func (segment *Segment) Sync() error {
	if err := segment.File.Sync(); err != nil {
		return fmt.Errorf("WAL Segment: failed to sync segment file: %w", err)
	}
	return nil
}

func (segment *Segment) CloseAndArchive() error {
	if err := segment.File.Close(); err != nil {
		return fmt.Errorf("WAL Segment: failed to close segment file: %w", err)
	}
	return nil
}

func (segment *Segment) IsFull(maxSize int) bool {
	return segment.Size >= maxSize
}

func (segment *Segment) IsEmpty() bool {
	return segment.Size == 0
}

func (segment *Segment) lastIndex() (EntryIndex, error) {
	lastIndex := INITIAL_LAST_ENTRY_INDEX

	if segment.IsEmpty() {
		return lastIndex, nil
	}

	chunkSize := SEGMENT_CAPACITY / 10
	accumulator := make([]byte, 0, 2*chunkSize)

	for chunk, err := range helpers.ReadFileByChunk(segment.File, chunkSize) {
		if err != nil {
			return lastIndex, fmt.Errorf("WAL Segment: failed to read segment file: %w", err)
		}

		accumulator = append(accumulator, chunk...)

		for len(accumulator) > 0 {
			index, _, offset, err := codec.DecodeWALEntry(accumulator)

			if err != nil {
				return lastIndex, fmt.Errorf("WAL Segment: failed to decode event: %v", err)
			}

			accumulator = accumulator[offset:]
			lastIndex = EntryIndex(index)
		}
	}

	return lastIndex, nil
}

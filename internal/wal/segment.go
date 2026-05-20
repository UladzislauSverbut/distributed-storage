package wal

import (
	"distributed-storage/internal/helpers"
	"fmt"
	"os"
)

type EntryIndex uint64

type Segment struct {
	LastFlushed EntryIndex
	File        *os.File
	Size        int
	Capacity    int
}

func NewSegment(startIndex EntryIndex, directory string, capacity int) (*Segment, error) {
	segmentFileName := encodeSegmentName(startIndex)
	segmentFile, err := os.Create(segmentFileName)

	if err != nil {
		return nil, fmt.Errorf("WAL Segment: failed to create segment file: %w", err)
	}

	segment := &Segment{
		LastFlushed: startIndex,
		File:        segmentFile,
		Capacity:    capacity,
		Size:        0,
	}

	lastFlushed, err := segment.lastFlushed()

	if err != nil {
		return nil, fmt.Errorf("WAL Segment: failed to find last flushed index during initialization: %w", err)
	}

	segment.LastFlushed = lastFlushed

	return segment, nil
}

func (segment *Segment) Write(records [][]byte) error {
	data := make([]byte, 0)
	for idx, record := range records {
		index := segment.LastFlushed + EntryIndex(idx) + 1
		data = append(data, encodeSegmentEntry(index, record)...)
	}

	written, err := segment.File.Write(data)
	if err != nil {
		return fmt.Errorf("WAL Segment: failed to write records to segment file: %w", err)
	}
	if err := segment.File.Sync(); err != nil {
		return fmt.Errorf("WAL Segment: failed to sync segment file: %w", err)
	}

	segment.Size += written
	segment.LastFlushed += EntryIndex(len(records))

	return nil
}

func (segment *Segment) Close() error {
	if err := segment.File.Close(); err != nil {
		return fmt.Errorf("WAL Segment: failed to close segment file: %w", err)
	}
	return nil
}

func (segment *Segment) IsFull() bool {
	return segment.Size >= segment.Capacity
}

func (segment *Segment) IsEmpty() bool {
	return segment.Size == 0
}

func (segment *Segment) lastFlushed() (EntryIndex, error) {
	index := segment.LastFlushed

	if segment.IsEmpty() {
		return index, nil
	}

	chunkSize := segment.Capacity / 10
	accumulator := make([]byte, 0, 2*chunkSize)

	for chunk, err := range helpers.ReadFileByChunk(segment.File, chunkSize) {
		if err != nil {
			return index, fmt.Errorf("WAL Segment: failed to read segment file: %w", err)
		}

		accumulator = append(accumulator, chunk...)

		for len(accumulator) > 0 {
			index, _, offset, err := decodeSegmentEntry(accumulator)

			if err != nil {
				return index, fmt.Errorf("WAL Segment: failed to decode event: %v", err)
			}

			accumulator = accumulator[offset:]
			index = EntryIndex(index)
		}
	}

	return index, nil
}

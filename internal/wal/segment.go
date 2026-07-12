package wal

import (
	"distributed-storage/internal/helpers"
	"fmt"
	"io"
	"iter"
	"os"
)

type EntryIndex uint64

type segmentEntry struct {
	index  EntryIndex
	record []byte
}

type Segment struct {
	Start    EntryIndex
	Last     EntryIndex
	File     *os.File
	Size     int
	Capacity int
}

func OpenSegment(path string, start EntryIndex, capacity int) (*Segment, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("WAL Segment: failed to open %q: %w", path, err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("WAL Segment: failed to seek to start of %q: %w", path, err)
	}
	return &Segment{Start: start, Last: start, File: file, Capacity: capacity}, nil
}

func NewSegment(startIndex EntryIndex, directory string, capacity int) (*Segment, error) {
	file, err := os.OpenFile(directory+"/"+encodeSegmentName(startIndex), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("WAL Segment: failed to create segment file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("WAL Segment: failed to stat segment file: %w", err)
	}

	seg := &Segment{Start: startIndex, Last: startIndex, File: file, Capacity: capacity, Size: int(info.Size())}

	if seg.Last, err = seg.last(); err != nil {
		file.Close()
		return nil, fmt.Errorf("WAL Segment: failed to find last flushed index during initialization: %w", err)
	}

	return seg, nil
}

func (segment *Segment) Write(records [][]byte) error {
	var data []byte

	for idx, record := range records {
		index := segment.Last + EntryIndex(idx) + 1
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
	segment.Last += EntryIndex(len(records))

	return nil
}

func (segment *Segment) Close() error {
	if err := segment.File.Close(); err != nil {
		return fmt.Errorf("WAL Segment: failed to close segment file: %w", err)
	}
	return nil
}

func (segment *Segment) All() iter.Seq2[segmentEntry, error] {
	return func(yield func(segmentEntry, error) bool) {
		chunkSize := segment.Capacity / 10
		accumulator := make([]byte, 0, 2*chunkSize)

		for chunk, err := range helpers.ReadFileByChunk(segment.File, chunkSize) {
			if err != nil {
				yield(segmentEntry{}, fmt.Errorf("WAL Segment: failed to read segment file: %w", err))
				return
			}

			accumulator = append(accumulator, chunk...)

			for len(accumulator) > 0 {
				index, record, offset, err := decodeSegmentEntry(accumulator)
				if err != nil {
					yield(segmentEntry{}, fmt.Errorf("WAL Segment: failed to decode entry: %w", err))
					return
				}
				accumulator = accumulator[offset:]

				if !yield(segmentEntry{index, record}, nil) {
					return
				}
			}
		}
	}
}

func (segment *Segment) Since(start EntryIndex) iter.Seq2[segmentEntry, error] {
	return func(yield func(segmentEntry, error) bool) {
		for entry, err := range segment.All() {
			if err != nil {
				yield(segmentEntry{}, err)
				return
			}
			if entry.index > start {
				if !yield(entry, nil) {
					return
				}
			}
		}
	}
}

func (segment *Segment) IsFull() bool {
	return segment.Size >= segment.Capacity
}

func (segment *Segment) IsEmpty() bool {
	return segment.Size == 0
}

func (segment *Segment) last() (EntryIndex, error) {
	index := segment.Start

	if segment.IsEmpty() {
		return index, nil
	}

	for entry, err := range segment.All() {
		if err != nil {
			return index, err
		}
		index = entry.index
	}

	return index, nil
}

package store

import (
	"distributed-storage/internal/helpers"
	"fmt"
	"math"
	"sync"
)

type MemoryStorage struct {
	size   int
	offset int
	memory [][]byte

	mu sync.RWMutex
}

func NewMemoryStorage(initialSize int) *MemoryStorage {
	storage := &MemoryStorage{
		memory: [][]byte{},
		size:   0,
		offset: 0,
	}

	storage.ensureSize(initialSize)

	return storage
}

func (storage *MemoryStorage) Segment(offset int, size int) []byte {

	storage.mu.RLock()
	defer storage.mu.RUnlock()

	if size+offset > storage.size {
		panic(fmt.Sprintf("MemoryStorage: getting memory segment is out of range %d > %d", size+offset, storage.size))
	}

	return helpers.ReadFromSegments(storage.memory, offset, size)
}

func (storage *MemoryStorage) UpdateSegments(updates []SegmentUpdate) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	for _, update := range updates {
		storage.ensureSize(update.Offset + len(update.Data))

		helpers.WriteToSegments(storage.memory, update.Offset, update.Data)
	}

	return nil
}

func (storage *MemoryStorage) UpdateSegmentsAndFlush(updates []SegmentUpdate) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	for _, update := range updates {
		storage.ensureSize(update.Offset + len(update.Data))

		helpers.WriteToSegments(storage.memory, update.Offset, update.Data)
	}

	return storage.Flush()
}

func (storage *MemoryStorage) AppendSegmentAndFlush(data []byte) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	storage.ensureSize(storage.offset + len(data))

	helpers.WriteToSegments(storage.memory, storage.offset, data)
	storage.offset += len(data)

	return storage.Flush()
}

func (storage *MemoryStorage) Flush() error {
	return nil
}

func (storage *MemoryStorage) Size() int {
	storage.mu.RLock()
	defer storage.mu.RUnlock()

	return storage.size
}

func (storage *MemoryStorage) ensureSize(desiredSize int) {
	if desiredSize <= storage.size {
		return
	}

	totalSize := int(math.Max(float64(desiredSize), float64(storage.size)*1.25))

	storage.memory = append(storage.memory, make([]byte, totalSize-storage.size))
	storage.size = totalSize
}

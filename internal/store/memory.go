package store

import (
	"fmt"
	"math"
	"sync"
)

type MemoryStorage struct {
	size   int
	memory [][]byte

	mu sync.RWMutex
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		memory: [][]byte{},
		size:   0,
	}
}

func (storage *MemoryStorage) Flush() error {
	return nil
}

func (storage *MemoryStorage) Size() int {
	storage.mu.RLock()
	defer storage.mu.RUnlock()

	return storage.size
}

func (storage *MemoryStorage) MemorySegment(offset int, size int) []byte {

	storage.mu.RLock()
	defer storage.mu.RUnlock()

	if size+offset > storage.size {
		panic(fmt.Sprintf("MemoryStorage: getting memory segment is out of range %d > %d", size+offset, storage.size))
	}

	return findMemorySegment(storage.memory, offset, size)
}

func (storage *MemoryStorage) UpdateMemorySegment(offset int, data []byte) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	if len(data)+offset > storage.size {
		storage.increaseSize(len(data) + offset + offset)
	}

	writeMemorySegment(storage.memory, offset, data)
	return nil
}

func (storage *MemoryStorage) AppendMemorySegment(data []byte) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	previousSize := storage.size
	storage.increaseSize(storage.size + len(data))

	writeMemorySegment(storage.memory, previousSize, data)

	return nil
}

func (storage *MemoryStorage) increaseSize(desiredSize int) {
	if desiredSize <= storage.size {
		return
	}

	totalSize := int(math.Max(float64(desiredSize), float64(storage.size)*1.25))

	storage.memory = append(storage.memory, make([]byte, totalSize-storage.size))
	storage.size = totalSize
}

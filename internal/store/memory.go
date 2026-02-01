package store

import (
	"fmt"
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

func (storage *MemoryStorage) IncreaseSize(size int) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	if size <= storage.size {
		return nil
	}

	storage.memory = append(storage.memory, make([]byte, size-storage.size))
	storage.size = size

	return nil
}

func (storage *MemoryStorage) MemorySegment(offset int, size int) []byte {

	storage.mu.RLock()
	defer storage.mu.RUnlock()

	if size+offset > storage.size {
		panic(fmt.Sprintf("MemoryStorage: getting memory segment is out of range %d > %d", size+offset, storage.size))
	}

	return findMemorySegment(storage.memory, size, offset)
}

func (storage *MemoryStorage) UpdateMemorySegment(offset int, data []byte) {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	if len(data)+offset > storage.size {
		panic(fmt.Sprintf("MemoryStorage: updating memory segment is out of range %d > %d", len(data)+offset, storage.size))
	}

	writeMemorySegment(storage.memory, data, offset)
}

func (storage *MemoryStorage) AppendMemorySegment(offset int, data []byte) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	storage.memory = append(storage.memory, data)
	storage.size = storage.size + len(data)

	return nil
}

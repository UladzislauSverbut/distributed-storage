package store

import (
	"fmt"
)

type MemoryStorage struct {
	size   int
	memory [][]byte
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
	return storage.size
}

func (storage *MemoryStorage) IncreaseSize(size int) error {
	storage.memory = append(storage.memory, make([]byte, size))
	storage.size = size
	return nil
}

func (storage *MemoryStorage) MemoryBlock(size int, offset int) []byte {
	if size+offset > storage.size {
		panic(fmt.Sprintf("MemoryStorage: getting memory block is out of range %d > %d", size+offset, storage.size))
	}

	return findMemoryBlock(storage.memory, size, offset)
}

func (storage *MemoryStorage) UpdateMemoryBlock(data []byte, offset int) {
	if len(data)+offset > storage.size {
		panic(fmt.Sprintf("MemoryStorage: updating memory block is out of range %d > %d", len(data)+offset, storage.size))
	}

	writeMemoryBlock(storage.memory, data, offset)
}

func (storage *MemoryStorage) FlushMemoryBlock(data []byte, offset int) error {
	storage.UpdateMemoryBlock(data, offset)

	return nil
}

package store

import (
	"fmt"
	"math"
	"os"
	"sync"
)

type FileStorage struct {
	file   *os.File
	size   int
	memory [][]byte

	mu sync.RWMutex
}

func NewFileStorage(filePath string, initialSize int) (*FileStorage, error) {
	var file *os.File
	var err error

	defer func() {
		if err != nil && file != nil {
			file.Close()
		}
	}()

	if file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, fmt.Errorf("FileStorage: failed to open file %w", err)
	}

	fileStat, err := file.Stat()

	if err != nil {
		return nil, err
	}

	fileSize := int(fileStat.Size())
	virtualMemory := [][]byte{}
	storage := &FileStorage{
		file:   file,
		size:   fileSize,
		memory: virtualMemory,
	}

	if storage.size > 0 {
		chunk, err := mapFileToMemory(file, 0, fileSize)

		if err != nil {
			return nil, err
		}

		storage.memory = append(storage.memory, chunk)
	}

	if err := storage.increaseSize(initialSize); err != nil {
		return nil, err
	}

	return storage, nil
}

func (storage *FileStorage) Flush() error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	return storage.file.Sync()
}

func (storage *FileStorage) Size() int {
	storage.mu.RLock()
	defer storage.mu.RUnlock()

	return storage.size
}

func (storage *FileStorage) MemorySegment(offset int, size int) []byte {
	storage.mu.RLock()
	defer storage.mu.RUnlock()

	if storage.size < offset+size {
		panic(fmt.Sprintf("FileStorage: getting memory segment is out of range %d > %d", size+offset, storage.size))
	}

	return findMemorySegment(storage.memory, offset, size)
}

func (storage *FileStorage) UpdateMemorySegment(offset int, data []byte) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	expectedSize := offset + len(data)

	if expectedSize > storage.size {
		if err := storage.increaseSize(expectedSize); err != nil {
			return err
		}
	}

	writeMemorySegment(storage.memory, offset, data)

	return nil
}

func (storage *FileStorage) AppendMemorySegment(data []byte) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	previousSize := storage.size
	expectedSize := previousSize + len(data)

	if err := storage.increaseSize(expectedSize); err != nil {
		return fmt.Errorf("FileStorage: failed to increase file size %w", err)
	}

	if _, err := storage.file.WriteAt(data, int64(previousSize)); err != nil {
		return fmt.Errorf("FileStorage: failed to write data to file %w", err)
	}

	return nil
}

func (storage *FileStorage) increaseSize(desiredSize int) error {
	if desiredSize <= storage.size {
		return nil
	}

	totalSize := int(math.Max(float64(desiredSize), float64(storage.size)*1.25))
	if err := increaseFileSize(storage.file, int64(totalSize)); err != nil {
		return fmt.Errorf("FileStorage: failed to increase file size %w", err)
	}

	newMemoryBlock, err := mapFileToMemory(storage.file, int64(storage.size), totalSize-storage.size)
	if err != nil {
		return fmt.Errorf("FileStorage: failed to map file to memory %w", err)
	}

	storage.memory = append(storage.memory, newMemoryBlock)
	storage.size = totalSize

	return nil
}

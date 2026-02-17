package store

import (
	"distributed-storage/internal/helpers"
	"fmt"
	"math"
	"os"
	"sync"
)

type FileStorage struct {
	file   *os.File
	size   int
	offset int
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
		offset: 0,
		memory: virtualMemory,
	}

	if storage.size > 0 {
		chunk, err := mapFileToMemory(file, 0, fileSize)

		if err != nil {
			return nil, err
		}

		storage.memory = append(storage.memory, chunk)
	}

	if err := storage.ensureSize(initialSize); err != nil {
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

	return helpers.ReadFromSegments(storage.memory, offset, size)
}

func (storage *FileStorage) UpdateMemorySegments(updates []MemorySegmentUpdate) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	for _, update := range updates {
		expectedSize := update.Offset + len(update.Data)

		if err := storage.ensureSize(expectedSize); err != nil {
			return err
		}

		helpers.WriteToSegments(storage.memory, update.Offset, update.Data)
	}

	return nil
}

func (storage *FileStorage) AppendMemorySegment(data []byte) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	expectedSize := storage.offset + len(data)

	if err := storage.ensureSize(expectedSize); err != nil {
		return fmt.Errorf("FileStorage: append file size %w", err)
	}

	helpers.WriteToSegments(storage.memory, storage.offset, data)

	storage.offset += len(data)

	return nil
}

func (storage *FileStorage) ensureSize(desiredSize int) error {
	var err error

	if desiredSize <= storage.size {
		return nil
	}

	oldSize := storage.size
	totalSize := int(math.Max(float64(desiredSize), float64(storage.size)*1.25))

	if totalSize, err = increaseFileSize(storage.file, totalSize); err != nil {
		return fmt.Errorf("FileStorage: failed to increase file size %w", err)
	}

	newMemoryBlock, err := mapFileToMemory(storage.file, int64(oldSize), totalSize-oldSize)
	if err != nil {
		return fmt.Errorf("FileStorage: failed to map file to memory %w", err)
	}

	storage.memory = append(storage.memory, newMemoryBlock)
	storage.size = totalSize

	return nil
}

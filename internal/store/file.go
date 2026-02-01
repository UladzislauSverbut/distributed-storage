package store

import (
	"fmt"
	"os"
	"sync"
)

type FileStorage struct {
	file          *os.File
	fileSize      int
	virtualMemory [][]byte

	mu sync.RWMutex
}

func NewFileStorage(filePath string) (*FileStorage, error) {
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

	if fileSize > 0 {
		if segment, err := mapFileToMemory(file, 0, fileSize); err != nil {
			return nil, err
		} else {
			virtualMemory = [][]byte{segment}
		}
	}

	return &FileStorage{
		file:          file,
		fileSize:      fileSize,
		virtualMemory: virtualMemory,
	}, nil
}

func (storage *FileStorage) Flush() error {
	storage.mu.RLock()
	defer storage.mu.RUnlock()

	return storage.file.Sync()
}

func (storage *FileStorage) Size() int {
	storage.mu.RLock()
	defer storage.mu.RUnlock()

	return storage.fileSize
}

func (storage *FileStorage) IncreaseSize(size int) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	if size <= storage.fileSize {
		return nil
	}

	if err := increaseFileSize(storage.file, int64(size)); err != nil {
		return fmt.Errorf("FileStorage: failed to increase file size %w", err)
	}

	newMemoryBlock, err := mapFileToMemory(storage.file, int64(storage.fileSize), size-storage.fileSize)

	if err != nil {
		return fmt.Errorf("FileStorage: failed to map file to memory %w", err)
	}

	storage.virtualMemory = append(storage.virtualMemory, newMemoryBlock)
	storage.fileSize = size

	return nil
}

func (storage *FileStorage) MemorySegment(offset int, size int) []byte {
	storage.mu.RLock()
	defer storage.mu.RUnlock()

	if size+offset > storage.fileSize {
		panic(fmt.Sprintf("FileStorage: getting memory segment is out of range %d > %d", size+offset, storage.fileSize))
	}

	return findMemorySegment(storage.virtualMemory, size, offset)
}

func (storage *FileStorage) UpdateMemorySegment(offset int, data []byte) {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	if len(data)+offset > storage.fileSize {
		panic(fmt.Sprintf("FileStorage: updating memory segment is out of range %d > %d", len(data)+offset, storage.fileSize))
	}

	writeMemorySegment(storage.virtualMemory, data, offset)
}

func (storage *FileStorage) AppendMemorySegment(data []byte) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	currentSize := storage.fileSize
	newSize := currentSize + len(data)

	if err := increaseFileSize(storage.file, int64(newSize)); err != nil {
		return fmt.Errorf("FileStorage: failed to increase file size %w", err)
	}

	newMemoryBlock, err := mapFileToMemory(storage.file, int64(currentSize), len(data))

	if err != nil {
		return fmt.Errorf("FileStorage: failed to map file to memory %w", err)
	}

	if _, err := storage.file.WriteAt(data, int64(currentSize)); err != nil {
		return fmt.Errorf("FileStorage: failed to write data to file %w", err)
	}

	storage.virtualMemory = append(storage.virtualMemory, newMemoryBlock)
	storage.fileSize = newSize

	return nil
}

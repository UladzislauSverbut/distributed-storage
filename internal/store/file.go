package store

import (
	"fmt"
	"os"
)

type FileStorage struct {
	file          *os.File
	fileSize      int
	virtualMemory [][]byte
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
	return storage.file.Sync()
}

func (storage *FileStorage) Size() int {
	return storage.fileSize
}

func (storage *FileStorage) IncreaseSize(size int) error {

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

func (storage *FileStorage) MemorySegment(size int, offset int) []byte {
	if size+offset > storage.fileSize {
		panic(fmt.Sprintf("FileStorage: getting memory segment is out of range %d > %d", size+offset, storage.fileSize))
	}

	return findMemorySegment(storage.virtualMemory, size, offset)
}

func (storage *FileStorage) UpdateMemorySegment(data []byte, offset int) {
	if len(data)+offset > storage.fileSize {
		panic(fmt.Sprintf("FileStorage: updating memory segment is out of range %d > %d", len(data)+offset, storage.fileSize))
	}

	writeMemorySegment(storage.virtualMemory, data, offset)
}

func (storage *FileStorage) SaveMemorySegment(data []byte, offset int) error {
	if len(data)+offset > storage.fileSize {
		panic(fmt.Sprintf("FileStorage: flushed memory segment is out of range %d > %d", len(data)+offset, storage.fileSize))
	}

	_, err := storage.file.WriteAt(data, int64(offset))

	return err
}

package backend

import (
	"fmt"
	"os"
)

type FileBackend struct {
	file          *os.File
	fileSize      int
	virtualMemory [][]byte
}

func NewFileBackend(filePath string) (*FileBackend, error) {
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

	return &FileBackend{
		file:          file,
		fileSize:      fileSize,
		virtualMemory: virtualMemory,
	}, nil
}

func (backend *FileBackend) Flush() error {
	return backend.file.Sync()
}

func (backend *FileBackend) Size() int {
	return backend.fileSize
}

func (backend *FileBackend) IncreaseSize(size int) error {

	if err := increaseFileSize(backend.file, int64(size)); err != nil {
		return fmt.Errorf("FileBackend: failed to increase file size %w", err)
	}

	newMemoryBlock, err := mapFileToMemory(backend.file, int64(backend.fileSize), size-backend.fileSize)

	if err != nil {
		return fmt.Errorf("FileBackend: failed to map file to memory %w", err)
	}

	backend.virtualMemory = append(backend.virtualMemory, newMemoryBlock)
	backend.fileSize += size

	return nil
}

func (backend *FileBackend) MemoryBlock(size int, offset int) []byte {
	if size+offset > backend.fileSize {
		panic(fmt.Sprintf("FileBackend: requested memory block is out of range %d > %d", size+offset, backend.fileSize))
	}

	memoryBlock := make([]byte, size)
	filledMemory := 0
	segmentStart := 0

	for _, segment := range backend.virtualMemory {
		segmentEnd := segmentStart + len(segment)

		if offset < segmentEnd {

			start := offset - segmentStart
			if start < 0 {
				start = 0
			}

			segmentSize := len(segment) - start
			if filledMemory+segmentSize > size {
				segmentSize = size - filledMemory
			}

			copy(memoryBlock[filledMemory:], segment[start:start+segmentSize])
			filledMemory += segmentSize
		}

		segmentStart = segmentEnd
	}

	return memoryBlock
}

func (backend *FileBackend) UpdateMemoryBlock(data []byte, offset int) {
	if len(data)+offset > backend.fileSize {
		panic(fmt.Sprintf("FileBackend: updated memory block is out of range %d > %d", len(data)+offset, backend.fileSize))
	}

	for _, segment := range backend.virtualMemory {
		if offset >= 0 && offset < len(segment) {
			blockEnd := min(len(data), len(segment)-offset)

			copy(segment[offset:offset+blockEnd], data[:blockEnd])

			if blockEnd == len(data) {
				return
			}

			data = data[blockEnd:]
			offset = 0
		} else {
			offset -= len(segment)
			continue
		}
	}
}

func (backend *FileBackend) FlushMemoryBlock(data []byte, offset int) error {
	if len(data)+offset > backend.fileSize {
		panic(fmt.Sprintf("FileBackend: flushed memory block is out of range %d > %d", len(data)+offset, backend.fileSize))
	}

	_, err := backend.file.WriteAt(data, int64(offset))

	return err
}

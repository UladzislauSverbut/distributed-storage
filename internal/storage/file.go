package storage

import (
	"fmt"
	"os"
)

// initialization of file storage
// firstInitialization := false

// 	fileStat, err := file.Stat()

// 	if err != nil {
// 		return nil, err
// 	}

// 	fileSize := int(fileStat.Size())

// 	if fileSize > 0 && fileSize%pageSize != 0 {
// 		return nil, errors.New("PageManager: support only files with size of multiple pages")
// 	}

// 	if fileSize == 0 {
// 		firstInitialization = true
// 		fileSize = pageSize * 10

// 		if err = increaseFileSize(file, int64(fileSize)); err != nil {
// 			return nil, err
// 		}
// 	}

// 	virtualMemory, err := mapFileToMemory(file, 0, fileSize)

// 	if err != nil {
// 		return nil, err
// 	}

type FileStorage struct {
	pointer *os.File
	size    int
	memory  [][]byte
}

func (storage *FileStorage) Size() int {
	return storage.size
}

func (storage *FileStorage) IncreaseSize(requestedSize int) error {
	if requestedSize <= storage.size {
		return nil
	}

	if err := increaseFileSize(storage.pointer, int64(requestedSize)); err != nil {
		return err
	}

	storage.size = requestedSize
	memorySize := 0

	for _, memorySegment := range storage.memory {
		memorySize += len(memorySegment)
	}

	for storage.size > memorySize {
		chunk, err := mapFileToMemory(storage.pointer, int64(memorySize), memorySize)

		if err != nil {
			return fmt.Errorf("FileStorage can't add chunks: %w", err)
		}

		storage.memory = append(storage.memory, chunk)
		memorySize += len(chunk)
	}

	return nil
}

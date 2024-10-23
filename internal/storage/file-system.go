package storage

import (
	"fmt"
	"os"
	"syscall"
)

type File struct {
	pointer *os.File
	path    string
	size    int
}

type FileSystemStorage struct {
	file              File
	pageSize          int
	reservedPages     [][]Page
	virtualMemorySize int
	virtualMemory     [][]byte
}

func (storage *FileSystemStorage) Get(pagePointer uint64) *[]byte {
	firstPagePointer := uint64(0)

	for _, memorySegment := range storage.virtualMemory {
		lastPagePointer := firstPagePointer + uint64(len(memorySegment)/storage.pageSize)

		if lastPagePointer > pagePointer {
			offset := int(pagePointer-firstPagePointer) * storage.pageSize
			return memorySegment[offset : offset+storage.pageSize]
		}

		firstPagePointer = lastPagePointer
	}

	panic(fmt.Sprintf("Cant find unstored page %d", pagePointer))
}

func (storage *FileSystemStorage) splitFile(numberOfPages int) error {
	if storage.virtualMemorySize >= numberOfPages*storage.pageSize {
		return nil
	}

	chunk, err := syscall.Mmap(int(storage.file.pointer.Fd()), int64(storage.virtualMemorySize), storage.virtualMemorySize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	if err != nil {
		return fmt.Errorf("Cant add chunks to file systems storage: %w", err)
	}

	storage.virtualMemorySize = len(chunk)
	storage.virtualMemory = append(storage.virtualMemory, chunk)

	return nil
}

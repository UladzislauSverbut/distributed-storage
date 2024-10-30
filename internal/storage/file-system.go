package storage

import (
	"errors"
	"fmt"
	"math"
	"os"
	"syscall"
)

type pagePointer = uint64

type file struct {
	pointer *os.File
	path    string
	size    int
}

type fileSystemStorage struct {
	file              file
	pageSize          int
	storedPagesNumber int
	allocatedPages    [][]byte
	virtualMemorySize int
	virtualMemory     [][]byte
}

func newFileSystemStorage(filePath string, pageSize int) (*fileSystemStorage, error) {
	pointer, err := os.Open(filePath)

	if err != nil {
		return nil, err
	}

	fileStat, err := pointer.Stat()

	if err != nil {
		return nil, err
	}

	fileSize := int(fileStat.Size())

	if fileSize%pageSize != 0 {
		return nil, errors.New("FileSystem storage supports only files with size of multiple pages")
	}

	virtualMemory, err := syscall.Mmap(int(pointer.Fd()), 0, fileSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	if err != nil {
		return nil, err
	}

	return &fileSystemStorage{
		file: file{
			pointer: pointer,
			path:    filePath,
			size:    fileSize,
		},
		pageSize:          pageSize,
		storedPagesNumber: 0,
		allocatedPages:    [][]byte{},
		virtualMemory:     [][]byte{virtualMemory},
		virtualMemorySize: fileSize,
	}, nil
}

func (storage *fileSystemStorage) getPage(pointer pagePointer) []byte {
	firstPagePointer := pagePointer(0)

	for _, memorySegment := range storage.virtualMemory {
		lastPagePointer := firstPagePointer + uint64(len(memorySegment)/storage.pageSize)

		if lastPagePointer > pointer {
			offset := int(pointer-firstPagePointer) * storage.pageSize
			return memorySegment[offset : offset+storage.pageSize]
		}

		firstPagePointer = lastPagePointer
	}

	panic(fmt.Sprintf("Cant find unstored page %d", pointer))
}

func (storage *fileSystemStorage) createPage() pagePointer {
	page := make([]byte, storage.pageSize)
	pointer := storage.storedPagesNumber + len(storage.allocatedPages)

	storage.allocatedPages = append(storage.allocatedPages, page)

	return pagePointer(pointer)
}

func (storage *fileSystemStorage) splitFile(desireNumberOfPages int) error {
	if storage.virtualMemorySize >= desireNumberOfPages*storage.pageSize {
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

func (storage *fileSystemStorage) extendFile(desireNumberOfPages int) error {
	filePages := storage.file.size / storage.pageSize

	if filePages >= desireNumberOfPages {
		return nil
	}

	for filePages < desireNumberOfPages {
		incrementPages := int(math.Ceil(float64(filePages) / 8))
		filePages += incrementPages
	}

	fileSize := filePages * storage.pageSize

	if err := syscall.Fallocate(int(storage.file.pointer.Fd()), 0, 0, int64(fileSize)); err != nil {
		return err
	}

	storage.file.size = fileSize

	return nil
}

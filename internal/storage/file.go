package storage

import (
	"errors"
	"fmt"
	"math"
	"os"
	"syscall"
)

type PagePointer = uint64

type File struct {
	pointer *os.File
	size    int
}

type FileStorage struct {
	file              File
	pageSize          int
	storedPagesNumber int
	allocatedPages    [][]byte
	virtualMemorySize int
	virtualMemory     [][]byte
}

func NewFileStorage(file *os.File, pageSize int) (*FileStorage, error) {
	fileStat, err := file.Stat()

	if err != nil {
		return nil, err
	}

	fileSize := int(fileStat.Size())

	if fileSize%pageSize != 0 {
		return nil, errors.New("FileSystem storage supports only files with size of multiple pages")
	}

	virtualMemorySize := pageSize << 10

	for virtualMemorySize < fileSize {
		virtualMemorySize *= 2
	}

	virtualMemory, err := syscall.Mmap(int(file.Fd()), 0, virtualMemorySize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	if err != nil {
		return nil, err
	}

	return &FileStorage{
		file: File{
			pointer: file,
			size:    fileSize,
		},
		pageSize:          pageSize,
		storedPagesNumber: 0,
		allocatedPages:    [][]byte{},
		virtualMemory:     [][]byte{virtualMemory},
		virtualMemorySize: virtualMemorySize,
	}, nil
}

func (storage *FileStorage) GetPage(pointer PagePointer) []byte {
	firstPagePointer := PagePointer(0)

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

func (storage *FileStorage) CreatePage() PagePointer {
	page := make([]byte, storage.pageSize)
	pointer := storage.storedPagesNumber + len(storage.allocatedPages)

	storage.allocatedPages = append(storage.allocatedPages, page)

	return PagePointer(pointer)
}

func (storage *FileStorage) DeletePage(pointer PagePointer) {
}

func (storage *FileStorage) SavePage(pointer PagePointer) error {
	if _, err := storage.file.pointer.WriteAt(storage.GetPage(pointer), int64(pointer)*int64(storage.pageSize)); err != nil {
		return err
	}

	return nil
}

func (storage *FileStorage) SavePages() error {
	if err := storage.saveAllocatedPages(); err != nil {
		return err
	}

	return storage.syncPagesWithFile()
}

func (storage *FileStorage) SetNumberOfPages(numberOfPages int) error {
	if numberOfPages < 1 || numberOfPages > storage.file.size/storage.pageSize {
		return fmt.Errorf("FileSystem storage couldn`t contain %d pages", numberOfPages)
	}

	storage.storedPagesNumber = numberOfPages

	return nil
}

func (storage *FileStorage) GetNumberOfPages() int {
	return storage.storedPagesNumber + len(storage.allocatedPages)
}

func (storage *FileStorage) GetFileSize() int {
	return storage.file.size
}

func (storage *FileStorage) GetVirtualMemorySize() int {
	return storage.virtualMemorySize
}

func (storage *FileStorage) syncPagesWithFile() error {
	if err := storage.file.pointer.Sync(); err != nil {
		return err
	}

	storage.storedPagesNumber += len(storage.allocatedPages)
	storage.allocatedPages = make([][]byte, 0)

	return nil
}

func (storage *FileStorage) saveAllocatedPages() error {
	totalPages := storage.storedPagesNumber + len(storage.allocatedPages)

	if err := storage.extendFile(totalPages); err != nil {
		return err
	}

	if err := storage.splitFile(totalPages); err != nil {
		return err
	}

	for pageIndex, page := range storage.allocatedPages {
		pointer := storage.storedPagesNumber + pageIndex
		copy(storage.GetPage(uint64(pointer)), page)
	}

	return nil
}

func (storage *FileStorage) splitFile(desireNumberOfPages int) error {
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

func (storage *FileStorage) extendFile(desireNumberOfPages int) error {
	filePages := storage.file.size / storage.pageSize

	if filePages >= desireNumberOfPages {
		return nil
	}

	for filePages < desireNumberOfPages {
		incrementPages := int(math.Ceil(float64(desireNumberOfPages) / 8))
		filePages += incrementPages
	}

	fileSize := filePages * storage.pageSize

	if err := syscall.Fallocate(int(storage.file.pointer.Fd()), 0, 0, int64(fileSize)); err != nil {
		return err
	}

	storage.file.size = fileSize

	return nil
}

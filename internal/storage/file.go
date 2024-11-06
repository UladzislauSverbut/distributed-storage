package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
)

type File struct {
	pointer *os.File
	size    int
}

type FileStorage struct {
	file              File
	pageSize          int
	storedPagesNumber int
	allocatedPages    map[PagePointer][]byte
	virtualMemorySize int
	virtualMemory     [][]byte
}

const STORAGE_SIGNATURE = "B_TREE_FILE_SIGN"

// we book one file system storage page for storing information about number of used and freed pages
// we call this page as "master"
// master page structure:
// | signature | number of stored pages |  number of freed pages | user meta info |
// |    16B    |            8B          |            8B          |     ...rest    |

func NewFileStorage(file *os.File, pageSize int) (*FileStorage, error) {
	firstInitialization := false

	fileStat, err := file.Stat()

	if err != nil {
		return nil, err
	}

	fileSize := int(fileStat.Size())

	if fileSize > 0 && fileSize%pageSize != 0 {
		return nil, errors.New("FileSystem storage supports only files with size of multiple pages")
	}

	if fileSize == 0 {
		firstInitialization = true
		fileSize = pageSize * 10

		if err = increaseFileSize(file, fileSize); err != nil {
			return nil, err
		}
	}

	virtualMemory, err := mapFileToMemory(file, 0, fileSize)

	if err != nil {
		return nil, err
	}

	fs := &FileStorage{
		file: File{
			pointer: file,
			size:    fileSize,
		},
		pageSize:          pageSize,
		storedPagesNumber: 0,
		allocatedPages:    map[PagePointer][]byte{},
		virtualMemory:     [][]byte{virtualMemory},
		virtualMemorySize: fileSize,
	}

	if firstInitialization {
		// reserve master page
		if err = fs.setNumberOfPages(1); err != nil {
			return nil, err
		}
	} else {
		numberOfStoredPages, err := fs.parseMasterPage()

		if err != nil {
			return nil, err
		}

		if err = fs.setNumberOfPages(numberOfStoredPages); err != nil {
			return nil, err
		}
	}

	return fs, nil
}

func (storage *FileStorage) GetMetaInfo() []byte {
	masterPage := storage.GetPage(PagePointer(0))
	return masterPage[32:]
}

func (storage *FileStorage) SaveMetaInfo(meta []byte) error {
	if len(meta) > storage.pageSize-32 {
		panic(fmt.Sprintf("FileSystem storage couldn`t store metadata with size %d", len(meta)))
	}

	masterPage := storage.GetPage(PagePointer(0))

	copy(masterPage[0:16], []byte(STORAGE_SIGNATURE))
	binary.LittleEndian.PutUint64(masterPage[16:24], uint64(storage.GetNumberOfPages()))
	copy(masterPage[32:], meta)

	if _, err := storage.file.pointer.WriteAt(masterPage, 0); err != nil {
		return err
	}

	return nil
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

	panic(fmt.Sprintf("FileSystem storage cant find unstored page %d", pointer))
}

func (storage *FileStorage) CreatePage() PagePointer {
	page := make([]byte, storage.pageSize)
	pointer := storage.storedPagesNumber + len(storage.allocatedPages)

	storage.allocatedPages[PagePointer(pointer)] = page

	return PagePointer(pointer)
}

func (storage *FileStorage) DeletePage(pointer PagePointer) {
}

func (storage *FileStorage) SavePages() error {
	if err := storage.saveAllocatedPages(); err != nil {
		return err
	}

	return storage.syncPagesWithFile()
}

func (storage *FileStorage) SavePage(pointer PagePointer) error {
	if _, err := storage.file.pointer.WriteAt(storage.GetPage(pointer), int64(pointer)*int64(storage.pageSize)); err != nil {
		return err
	}

	return nil
}

func (storage *FileStorage) GetNumberOfPages() int {
	return storage.storedPagesNumber + len(storage.allocatedPages)
}

func (storage *FileStorage) setNumberOfPages(numberOfPages int) error {
	if numberOfPages < 1 || numberOfPages > storage.file.size/storage.pageSize {
		return fmt.Errorf("FileSystem storage couldn`t contain %d pages", numberOfPages)
	}

	storage.storedPagesNumber = numberOfPages

	return nil
}

func (storage *FileStorage) syncPagesWithFile() error {
	if err := storage.file.pointer.Sync(); err != nil {
		return err
	}

	storage.storedPagesNumber += len(storage.allocatedPages)
	storage.allocatedPages = map[uint64][]byte{}

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

	for pointer, page := range storage.allocatedPages {
		copy(storage.GetPage(uint64(pointer)), page)
	}

	return nil
}

func (storage *FileStorage) splitFile(desireNumberOfPages int) error {
	if storage.virtualMemorySize >= desireNumberOfPages*storage.pageSize {
		return nil
	}

	chunk, err := mapFileToMemory(storage.file.pointer, int64(storage.virtualMemorySize), storage.virtualMemorySize)

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

	if err := increaseFileSize(storage.file.pointer, fileSize); err != nil {
		return err
	}

	storage.file.size = fileSize

	return nil
}

func (storage *FileStorage) parseMasterPage() (int, error) {
	masterPage := storage.GetPage(PagePointer(0))
	fileSignature := masterPage[0:16]
	numberOfStoredPages := binary.LittleEndian.Uint64((masterPage[16:]))

	if !bytes.Equal([]byte(STORAGE_SIGNATURE), fileSignature) {
		return 0, errors.New("FileSystem storage file is corrupted")
	}

	return int(numberOfStoredPages), nil
}

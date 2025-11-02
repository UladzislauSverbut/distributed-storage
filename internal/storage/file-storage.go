package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
)

const NULL_PAGE = PagePointer(0)

const META_INFO_HEADER_SIZE = 40 // size of meta info  in file bytes

type PagePointer = uint64

type File struct {
	pointer *os.File
	size    int
}

type FileStorage struct {
	file               File
	fileMemory         [][]byte
	fileMemorySize     int
	pageSize           int
	pagesCount         int                    // count of total pages
	reusedPagesCount   int                    // count of pages that were reused
	releasedPagesCount int                    // count of pages that were released
	pagePool           *FilePagePool          //  pool of available file pages
	pageBuffer         map[PagePointer][]byte // map of buffered pages that will be synced with file
}

const STORAGE_SIGNATURE = "FILE_STORAGE_SIG"

/*
	We book one file system storage page for storing information about number of used and freed pages
	This page is called as "master"

	Mater Page Format

 	| signature | number of stored pages | page pool pointer | user meta info |
	|    16B    |            8B          |         8B        |     ...rest    |
*/

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

		if err = increaseFileSize(file, int64(fileSize)); err != nil {
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
		fileMemory:         [][]byte{virtualMemory},
		fileMemorySize:     fileSize,
		pageSize:           pageSize,
		pagesCount:         0,
		reusedPagesCount:   0,
		releasedPagesCount: 0,
		pageBuffer:         map[PagePointer][]byte{},
	}

	if firstInitialization {
		fs.pagesCount = 1 // reserve master page
		fs.pagePool = NewFilePagePool(fs.allocateVirtualPage(), fs)
	} else {
		if err = fs.parseMasterPage(); err != nil {
			return nil, err
		}
	}

	return fs, nil
}

func (storage *FileStorage) GetMetaInfo() []byte {
	masterPage := storage.getMasterPage()
	return masterPage[META_INFO_HEADER_SIZE:]
}

func (storage *FileStorage) SaveMetaInfo(meta []byte) error {
	if len(meta) > storage.pageSize-META_INFO_HEADER_SIZE {
		panic(fmt.Sprintf("FileSystem storage couldn`t store metadata with size %d", len(meta)))
	}

	masterPage := storage.getMasterPage()

	copy(masterPage[META_INFO_HEADER_SIZE:], meta)

	if _, err := storage.file.pointer.WriteAt(masterPage, 0); err != nil {
		return err
	}

	return nil
}

func (storage *FileStorage) GetPage(pointer PagePointer) []byte {
	if page, exist := storage.pageBuffer[pointer]; exist {
		return page
	}

	return storage.getFilePage(pointer)
}

func (storage *FileStorage) CreatePage(data []byte) PagePointer {
	pagePointer := storage.reuseFilePage()

	if pagePointer == NULL_PAGE {
		pagePointer = storage.allocateVirtualPage()
	}

	copy(storage.GetPage(pagePointer), data)

	return pagePointer
}

func (storage *FileStorage) DeletePage(pointer PagePointer) {
	storage.pageBuffer[pointer] = nil
	storage.releasedPagesCount++
}

func (storage *FileStorage) GetPagesCount() int {
	return storage.pagesCount + len(storage.pageBuffer)
}

func (storage *FileStorage) SaveChanges() error {
	if err := storage.saveReleasedPages(); err != nil {
		return err
	}

	if err := storage.saveAllocatedPages(); err != nil {
		return err
	}

	return storage.syncPagesWithFile()
}

func (storage *FileStorage) getFilePage(pointer PagePointer) []byte {
	firstPagePointer := PagePointer(0)

	for _, memorySegment := range storage.fileMemory {
		lastPagePointer := firstPagePointer + uint64(len(memorySegment)/storage.pageSize)

		if lastPagePointer > pointer {
			offset := int(pointer-firstPagePointer) * storage.pageSize
			return memorySegment[offset : offset+storage.pageSize]
		}

		firstPagePointer = lastPagePointer
	}

	panic(fmt.Sprintf("FileSystem storage cant find unstored page %d", pointer))
}

func (storage *FileStorage) reuseFilePage() PagePointer {
	if storage.reusedPagesCount == storage.pagePool.getPagesCont() {
		return NULL_PAGE
	}

	storage.reusedPagesCount++

	return storage.pagePool.getPage(storage.reusedPagesCount - 1)
}

func (storage *FileStorage) allocateVirtualPage() PagePointer {
	page := make([]byte, storage.pageSize)
	pointer := PagePointer(storage.pagesCount + len(storage.pageBuffer))

	storage.pageBuffer[pointer] = page

	return pointer
}

func (storage *FileStorage) getMasterPage() []byte {
	return storage.GetPage(PagePointer(0))
}

func (storage *FileStorage) parseMasterPage() error {
	masterPage := storage.GetPage(PagePointer(0))
	fileSignature := masterPage[0:16]
	pagesCount := binary.LittleEndian.Uint64((masterPage[16:]))
	pagePoolHead := binary.LittleEndian.Uint64((masterPage[24:]))

	if !bytes.Equal([]byte(STORAGE_SIGNATURE), fileSignature) {
		return errors.New("FileSystem storage file is corrupted")
	}

	storage.pagesCount = int(pagesCount)
	storage.pagePool = NewFilePagePool(PagePointer(pagePoolHead), storage)

	return nil
}

func (storage *FileStorage) saveReleasedPages() error {
	releasedPages := make([]PagePointer, 0, storage.releasedPagesCount)

	for pointer, page := range storage.pageBuffer {
		if page == nil {
			releasedPages = append(releasedPages, pointer)
			defer delete(storage.pageBuffer, pointer)
		}
	}

	storage.pagePool.updatePages(storage.reusedPagesCount, releasedPages)

	storage.releasedPagesCount = 0
	storage.reusedPagesCount = 0

	return nil
}

func (storage *FileStorage) saveAllocatedPages() error {
	storage.pagesCount += (len(storage.pageBuffer) - storage.releasedPagesCount)

	if err := storage.extendFile(storage.pagesCount); err != nil {
		return err
	}

	if err := storage.splitFile(storage.pagesCount); err != nil {
		return err
	}

	for pointer, page := range storage.pageBuffer {
		if page != nil {
			copy(storage.getFilePage(uint64(pointer)), page)
		}
	}

	storage.pageBuffer = map[PagePointer][]byte{}

	return nil
}

func (storage *FileStorage) syncPagesWithFile() error {
	masterPage := storage.getMasterPage()

	copy(masterPage[0:16], []byte(STORAGE_SIGNATURE))

	binary.LittleEndian.PutUint64(masterPage[16:], uint64(storage.pagesCount))
	binary.LittleEndian.PutUint64(masterPage[24:], uint64(storage.pagePool.head))

	return storage.file.pointer.Sync()
}

func (storage *FileStorage) splitFile(desireNumberOfPages int) error {
	if storage.fileMemorySize >= desireNumberOfPages*storage.pageSize {
		return nil
	}

	chunk, err := mapFileToMemory(storage.file.pointer, int64(storage.fileMemorySize), storage.fileMemorySize)

	if err != nil {
		return fmt.Errorf("FileSystem storage can`t add chunks: %w", err)
	}

	storage.fileMemorySize += len(chunk)
	storage.fileMemory = append(storage.fileMemory, chunk)

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

	if err := increaseFileSize(storage.file.pointer, int64(fileSize)); err != nil {
		return err
	}

	storage.file.size = fileSize

	return nil
}

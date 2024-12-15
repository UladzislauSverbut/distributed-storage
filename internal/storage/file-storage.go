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

type PagePointer = uint64

type File struct {
	pointer *os.File
	size    int
}

type FileStorage struct {
	file                File
	fileMemory          [][]byte
	fileMemorySize      int
	pageSize            int
	pagesNumber         int
	releasedPagesNumber int
	releasedPages       PagePointer            //pointer to store of released pages
	allocatedPages      map[PagePointer][]byte //map of memory allocated pages
}

const STORAGE_SIGNATURE = "FILE_STORAGE_SIG"

// we book one file system storage page for storing information about number of used and freed pages
// we call this page as "master"
// master page structure:
// | signature | number of stored pages |  number of available pages |  freed pages store pointer | user meta info |
// |    16B    |            8B          |            8B              |              8B            |     ...rest    |

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
		fileMemory:          [][]byte{virtualMemory},
		fileMemorySize:      fileSize,
		pageSize:            pageSize,
		pagesNumber:         0,
		releasedPagesNumber: 0,
		allocatedPages:      map[PagePointer][]byte{},
	}

	if firstInitialization {
		// reserve master page
		if err = fs.setNumberOfPages(1); err != nil {
			return nil, err
		}

		fs.releasedPages = fs.CreatePage()
	} else {
		if err = fs.parseMasterPage(); err != nil {
			return nil, err
		}
	}

	return fs, nil
}

func (storage *FileStorage) GetMetaInfo() []byte {
	masterPage := storage.getMasterPage()
	return masterPage[40:]
}

func (storage *FileStorage) SaveMetaInfo(meta []byte) error {
	if len(meta) > storage.pageSize-40 {
		panic(fmt.Sprintf("FileSystem storage couldn`t store metadata with size %d", len(meta)))
	}

	masterPage := storage.getMasterPage()

	copy(masterPage[40:], meta)

	if _, err := storage.file.pointer.WriteAt(masterPage, 0); err != nil {
		return err
	}

	return nil
}

func (storage *FileStorage) GetPage(pointer PagePointer) []byte {
	if page, exist := storage.allocatedPages[pointer]; exist {
		return page
	}

	return storage.getFilePage(pointer)
}

func (storage *FileStorage) CreatePage() PagePointer {
	if pagePointer := storage.findReleasedPage(); pagePointer != NULL_PAGE {
		return pagePointer
	}

	page := make([]byte, storage.pageSize)
	pointer := storage.pagesNumber + len(storage.allocatedPages)
	storage.allocatedPages[PagePointer(pointer)] = page

	return PagePointer(pointer)
}

func (storage *FileStorage) DeletePage(pointer PagePointer) {
	storage.addReleasedPage(pointer)
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
	return storage.pagesNumber + len(storage.allocatedPages)
}

func (storage *FileStorage) getFilePage(pointer PagePointer) []byte {
	firstPagePointer := NULL_PAGE

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

func (storage *FileStorage) findReleasedPage() PagePointer {
	if storage.releasedPagesNumber == 0 {
		return NULL_PAGE
	}

	storage.releasedPagesNumber -= 1

	pageStorePointer := storage.releasedPages
	pageStore := NewPageStore(storage.pageSize, storage.GetPage(pageStorePointer))

	if pageStore.getNumberOfAvailablePages() > 0 {
		return pageStore.getAvailablePage()
	}

	storage.releasedPages = pageStore.getPrevious()

	return pageStorePointer
}

func (storage *FileStorage) addReleasedPage(pointer PagePointer) {
	storage.releasedPagesNumber += 1

	pageStore := NewPageStore(storage.pageSize, storage.GetPage(storage.releasedPages))

	for pageStore.isFull() {
		pageStorePointer := storage.CreatePage()
		pageStore = NewPageStore(storage.pageSize, storage.GetPage(storage.releasedPages))

		pageStore.setPrevious(storage.releasedPages)
		storage.releasedPages = pageStorePointer
	}

	pageStore.addAvailablePage(pointer)
}

func (storage *FileStorage) setNumberOfPages(numberOfPages int) error {
	if numberOfPages < 1 || numberOfPages > storage.file.size/storage.pageSize {
		return fmt.Errorf("FileSystem storage couldn`t contain %d pages", numberOfPages)
	}

	storage.pagesNumber = numberOfPages

	return nil
}

func (storage *FileStorage) syncPagesWithFile() error {
	if err := storage.file.pointer.Sync(); err != nil {
		return err
	}

	storage.pagesNumber += len(storage.allocatedPages)
	storage.allocatedPages = map[uint64][]byte{}

	storage.saveMasterPage()

	return nil
}

func (storage *FileStorage) saveAllocatedPages() error {
	totalPages := storage.pagesNumber + len(storage.allocatedPages)

	if err := storage.extendFile(totalPages); err != nil {
		return err
	}

	if err := storage.splitFile(totalPages); err != nil {
		return err
	}

	for pointer, page := range storage.allocatedPages {
		copy(storage.getFilePage(uint64(pointer)), page)
	}

	return nil
}

func (storage *FileStorage) getMasterPage() []byte {
	return storage.getFilePage(PagePointer(0))
}

func (storage *FileStorage) saveMasterPage() {
	masterPage := storage.getMasterPage()

	copy(masterPage[0:16], []byte(STORAGE_SIGNATURE))
	binary.LittleEndian.PutUint64(masterPage[16:], uint64(storage.pagesNumber))
	binary.LittleEndian.PutUint64(masterPage[24:], uint64(storage.releasedPagesNumber))
	binary.LittleEndian.PutUint64(masterPage[32:], uint64(storage.releasedPages))
}

func (storage *FileStorage) parseMasterPage() error {
	masterPage := storage.GetPage(PagePointer(0))
	fileSignature := masterPage[0:16]
	pagesNumber := binary.LittleEndian.Uint64((masterPage[16:]))
	releasedPagesNumber := binary.LittleEndian.Uint64((masterPage[24:]))
	releasedPages := binary.LittleEndian.Uint64((masterPage[32:]))

	if !bytes.Equal([]byte(STORAGE_SIGNATURE), fileSignature) {
		return errors.New("FileSystem storage file is corrupted")
	}

	storage.pagesNumber = int(pagesNumber)
	storage.releasedPagesNumber = int(releasedPagesNumber)
	storage.releasedPages = releasedPages

	return nil
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

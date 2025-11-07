package fs

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

type PageManager struct {
	file                File
	fileMemory          [][]byte
	fileMemorySize      int
	pageSize            int
	pagesCount          int                    // count of total pages
	allocatedPagesCount int                    // count of pages that were allocated before saving
	reusedPagesCount    int                    // count of pages that were reused before saving
	releasedPagesCount  int                    // count of pages that were released before saving
	pageBuffer          *PageBuffer            //  buffer of available pages
	pageUpdates         map[PagePointer][]byte // map of page updates that will be synced with file
}

const STORAGE_SIGNATURE = "FILE_STORAGE_SIG"

/*
	We book one file system storage page for storing information about number of used and freed pages
	This page is called as "master"

	Mater Page Format

 	| signature | number of stored pages | page pool pointer | user meta info |
	|    16B    |            8B          |         8B        |     ...rest    |
*/

func NewPageManager(file *os.File, pageSize int) (*PageManager, error) {
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

	manager := &PageManager{
		file: File{
			pointer: file,
			size:    fileSize,
		},
		fileMemory:          [][]byte{virtualMemory},
		fileMemorySize:      fileSize,
		pageSize:            pageSize,
		pagesCount:          0,
		allocatedPagesCount: 0,
		reusedPagesCount:    0,
		releasedPagesCount:  0,
		pageUpdates:         map[PagePointer][]byte{},
	}

	if firstInitialization {
		manager.pagesCount = 1 // reserve master page
		manager.pageBuffer = NewPageBuffer(manager.allocateVirtualPage(), manager)
	} else {
		if err = manager.parseMasterPage(); err != nil {
			return nil, err
		}
	}

	return manager, nil
}

func (manager *PageManager) GetMetaInfo() []byte {
	masterPage := manager.getMasterPage()
	return masterPage[META_INFO_HEADER_SIZE:]
}

func (manager *PageManager) SaveMetaInfo(meta []byte) error {
	if len(meta) > manager.pageSize-META_INFO_HEADER_SIZE {
		panic(fmt.Sprintf("FileSystem storage couldn`t store metadata with size %d", len(meta)))
	}

	masterPage := manager.getMasterPage()

	copy(masterPage[META_INFO_HEADER_SIZE:], meta)

	if _, err := manager.file.pointer.WriteAt(masterPage, 0); err != nil {
		return err
	}

	return nil
}

func (manager *PageManager) GetPage(pointer PagePointer) []byte {
	if page, exist := manager.pageUpdates[pointer]; exist {
		return page
	}

	return manager.getFilePage(pointer)
}

func (manager *PageManager) CreatePage(data []byte) PagePointer {
	pagePointer := manager.reuseFilePage()

	if pagePointer == NULL_PAGE {
		pagePointer = manager.allocateVirtualPage()
	}

	manager.pageUpdates[pagePointer] = data

	return pagePointer
}

func (manager *PageManager) DeletePage(pointer PagePointer) {
	manager.pageUpdates[pointer] = nil
	manager.releasedPagesCount++
}

func (manager *PageManager) GetPagesCount() int {
	return manager.pagesCount + manager.allocatedPagesCount
}

func (manager *PageManager) SaveChanges() error {
	if err := manager.saveReleasedPages(); err != nil {
		return err
	}

	if err := manager.saveAllocatedPages(); err != nil {
		return err
	}

	return manager.syncPagesWithFile()
}

func (manager *PageManager) getFilePage(pointer PagePointer) []byte {
	firstPagePointer := PagePointer(0)

	for _, memorySegment := range manager.fileMemory {
		lastPagePointer := firstPagePointer + uint64(len(memorySegment)/manager.pageSize)

		if lastPagePointer > pointer {
			offset := int(pointer-firstPagePointer) * manager.pageSize
			return memorySegment[offset : offset+manager.pageSize]
		}

		firstPagePointer = lastPagePointer
	}

	panic(fmt.Sprintf("FileSystem storage cant find unstored page %d", pointer))
}

func (manager *PageManager) reuseFilePage() PagePointer {
	if manager.reusedPagesCount == manager.pageBuffer.getPagesCount() {
		return NULL_PAGE
	}

	manager.reusedPagesCount++

	return manager.pageBuffer.getPage(manager.reusedPagesCount - 1)
}

func (manager *PageManager) allocateVirtualPage() PagePointer {
	page := make([]byte, manager.pageSize)
	pointer := PagePointer(manager.pagesCount + manager.allocatedPagesCount)

	manager.allocatedPagesCount++
	manager.pageUpdates[pointer] = page

	return pointer

}

func (manager *PageManager) getMasterPage() []byte {
	return manager.GetPage(PagePointer(0))
}

func (manager *PageManager) parseMasterPage() error {
	masterPage := manager.GetPage(PagePointer(0))
	fileSignature := masterPage[0:16]
	pagesCount := binary.LittleEndian.Uint64((masterPage[16:]))
	pageBufferHead := binary.LittleEndian.Uint64((masterPage[24:]))

	if !bytes.Equal([]byte(STORAGE_SIGNATURE), fileSignature) {
		return errors.New("FileSystem storage file is corrupted")
	}

	manager.pagesCount = int(pagesCount)
	manager.pageBuffer = NewPageBuffer(PagePointer(pageBufferHead), manager)

	return nil
}

func (manager *PageManager) saveReleasedPages() error {
	releasedPages := make([]PagePointer, 0, manager.releasedPagesCount)

	for pointer, page := range manager.pageUpdates {
		if page == nil {
			releasedPages = append(releasedPages, pointer)
			defer delete(manager.pageUpdates, pointer)
		}
	}

	manager.pageBuffer.updatePages(manager.reusedPagesCount, releasedPages)

	manager.releasedPagesCount = 0
	manager.reusedPagesCount = 0

	return nil
}

func (manager *PageManager) saveAllocatedPages() error {
	manager.pagesCount += manager.allocatedPagesCount

	if err := manager.extendFile(manager.pagesCount); err != nil {
		return err
	}

	if err := manager.splitFile(manager.pagesCount); err != nil {
		return err
	}

	for pointer, page := range manager.pageUpdates {
		if page != nil {
			copy(manager.getFilePage(uint64(pointer)), page)
		}
	}

	manager.allocatedPagesCount = 0
	manager.pageUpdates = map[PagePointer][]byte{}

	return nil
}

func (manager *PageManager) syncPagesWithFile() error {
	masterPage := manager.getMasterPage()

	copy(masterPage[0:16], []byte(STORAGE_SIGNATURE))

	binary.LittleEndian.PutUint64(masterPage[16:], uint64(manager.pagesCount))
	binary.LittleEndian.PutUint64(masterPage[24:], uint64(manager.pageBuffer.head))

	return manager.file.pointer.Sync()
}

func (manager *PageManager) splitFile(desireNumberOfPages int) error {
	desiredFileMemorySize := desireNumberOfPages * manager.pageSize

	if manager.fileMemorySize >= desiredFileMemorySize {
		return nil
	}

	for desiredFileMemorySize > 0 {
		chunk, err := mapFileToMemory(manager.file.pointer, int64(manager.fileMemorySize), manager.fileMemorySize)

		if err != nil {
			return fmt.Errorf("FileSystem storage can't add chunks: %w", err)
		}

		manager.fileMemorySize += len(chunk)
		manager.fileMemory = append(manager.fileMemory, chunk)

		desiredFileMemorySize -= len(chunk)
	}

	return nil
}

func (manager *PageManager) extendFile(desireNumberOfPages int) error {
	filePages := manager.file.size / manager.pageSize

	if filePages >= desireNumberOfPages {
		return nil
	}

	for filePages < desireNumberOfPages {
		incrementPages := int(math.Ceil(float64(desireNumberOfPages) / 8))
		filePages += incrementPages
	}

	fileSize := filePages * manager.pageSize

	if err := increaseFileSize(manager.file.pointer, int64(fileSize)); err != nil {
		return err
	}

	manager.file.size = fileSize

	return nil
}

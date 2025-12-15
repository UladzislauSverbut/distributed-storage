package pager

import (
	"bytes"
	"distributed-storage/internal/store"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const NULL_PAGE = PagePointer(0)

const RESERVED_HEADER_SIZE = 40 // size of meta info in bytes

type PagePointer = uint64

type PageManager struct {
	storage store.Storage
	config  PageManagerConfig
	state   PageManagerState
}

type PageManagerConfig struct {
	pageSize int
}

type PageManagerState struct {
	pagesCount          int                    // count of total pages
	allocatedPagesCount int                    // count of pages that were allocated before saving
	reusedPagesCount    int                    // count of pages that were reused before saving
	freedPagesCount     int                    // count of pages that were freed before saving
	pageBuffer          *PageBuffer            //  buffer of available pages
	pageUpdates         map[PagePointer][]byte // map of page updates that will be synced with storage
}

const SIGNATURE = "PAGE_MANAGER_SIG"

/*
	We book one page for storing information about number of used and freed pages
	This page is called as "master"

	Mater Page Format

 	| signature | number of stored pages | page buffer pointer | user header info |
	|    16B    |           8B           |          8B         |      ...rest     |
*/

func NewPageManager(storage store.Storage, pageSize int) (*PageManager, error) {
	firstInitialization := false
	storageSize := storage.Size()

	if storageSize > 0 && storageSize%pageSize != 0 {
		return nil, errors.New("PageManager: support only storage with size of multiple pages")
	}

	if storageSize == 0 {
		firstInitialization = true
		storageSize = pageSize * 10

		if err := storage.IncreaseSize(storageSize); err != nil {
			return nil, err
		}
	}

	manager := &PageManager{
		storage: storage,
		config: PageManagerConfig{
			pageSize: pageSize,
		},
		state: PageManagerState{
			pagesCount:          0,
			allocatedPagesCount: 0,
			reusedPagesCount:    0,
			freedPagesCount:     0,
			pageBuffer:          nil,
			pageUpdates:         map[PagePointer][]byte{},
		},
	}

	if firstInitialization {
		manager.state.pagesCount = 1 // reserve master page
		manager.state.pageBuffer = NewPageBuffer(manager.allocateVirtualPage(), manager)
	} else {
		if err := manager.parseMasterPage(manager.masterPage()); err != nil {
			return nil, err
		}
	}

	return manager, nil
}

func (manager *PageManager) Header() []byte {
	masterPage := manager.masterPage()
	return masterPage[RESERVED_HEADER_SIZE:]
}

func (manager *PageManager) SaveHeader(header []byte) error {
	if len(header) > manager.config.pageSize-RESERVED_HEADER_SIZE {
		panic(fmt.Sprintf("PageManager: couldn`t store header with size %d", len(header)))
	}

	masterPage := manager.masterPage()

	copy(masterPage[RESERVED_HEADER_SIZE:], header)

	if err := manager.storage.SaveMemorySegment(masterPage, 0); err != nil {
		return err
	}

	return nil
}

func (manager *PageManager) State() PageManagerState {
	return manager.state
}

func (manager *PageManager) SetState(state PageManagerState) {
	manager.state = state
}

func (manager *PageManager) Page(pointer PagePointer) []byte {
	if page, exist := manager.state.pageUpdates[pointer]; exist {
		return page
	}

	return manager.storage.MemorySegment(manager.config.pageSize, int(pointer)*manager.config.pageSize)
}

func (manager *PageManager) CreatePage(data []byte) PagePointer {
	pagePointer := manager.reusePage()

	if pagePointer == NULL_PAGE {
		pagePointer = manager.allocateVirtualPage()
	}

	manager.state.pageUpdates[pagePointer] = data

	return pagePointer
}

func (manager *PageManager) UpdatePage(pointer PagePointer, data []byte) {
	manager.state.pageUpdates[pointer] = data
}

func (manager *PageManager) FreePage(pointer PagePointer) {
	manager.state.pageUpdates[pointer] = nil
	manager.state.freedPagesCount++
}

func (manager *PageManager) PagesCount() int {
	return manager.state.pagesCount + manager.state.allocatedPagesCount
}

func (manager *PageManager) SavePages() error {
	if err := manager.saveAllocatedPages(); err != nil {
		return err
	}

	manager.saveFreedPages()
	manager.saveMasterPage()

	return nil
}

func (manager *PageManager) reusePage() PagePointer {
	if manager.state.reusedPagesCount == manager.state.pageBuffer.availablePageCount() {
		return NULL_PAGE
	}

	manager.state.reusedPagesCount++

	return manager.state.pageBuffer.pageAt(manager.state.reusedPagesCount - 1)
}

func (manager *PageManager) allocateVirtualPage() PagePointer {
	page := make([]byte, manager.config.pageSize)
	pointer := PagePointer(manager.state.pagesCount + manager.state.allocatedPagesCount)

	manager.state.allocatedPagesCount++
	manager.state.pageUpdates[pointer] = page

	return pointer

}

func (manager *PageManager) masterPage() []byte {
	return manager.Page(PagePointer(0))
}

func (manager *PageManager) parseMasterPage(masterPage []byte) error {
	signature := masterPage[0:16]
	pagesCount := binary.LittleEndian.Uint64((masterPage[16:]))
	pageBufferHead := binary.LittleEndian.Uint64((masterPage[24:]))

	if !bytes.Equal([]byte(SIGNATURE), signature) {
		return errors.New("PageManager: storage is corrupted")
	}

	manager.state.pagesCount = int(pagesCount)
	manager.state.pageBuffer = NewPageBuffer(PagePointer(pageBufferHead), manager)

	return nil
}

func (manager *PageManager) saveFreedPages() {
	freedPages := make([]PagePointer, 0, manager.state.freedPagesCount)

	for pointer, page := range manager.state.pageUpdates {
		if page == nil {
			freedPages = append(freedPages, pointer)
			delete(manager.state.pageUpdates, pointer)
		}
	}

	manager.state.pageBuffer.applyChanges(manager.state.reusedPagesCount, freedPages)

	manager.state.freedPagesCount = 0
	manager.state.reusedPagesCount = 0
}

func (manager *PageManager) saveAllocatedPages() error {
	manager.state.pagesCount += manager.state.allocatedPagesCount

	if err := manager.extendStorage(manager.state.pagesCount); err != nil {
		return err
	}

	for pointer, page := range manager.state.pageUpdates {
		if page != nil {
			manager.storage.UpdateMemorySegment(page[0:manager.config.pageSize], int(pointer)*manager.config.pageSize)
			delete(manager.state.pageUpdates, pointer)
		}
	}

	manager.state.allocatedPagesCount = 0

	return nil
}

func (manager *PageManager) saveMasterPage() {
	masterPage := manager.masterPage()

	copy(masterPage[0:16], []byte(SIGNATURE))

	binary.LittleEndian.PutUint64(masterPage[16:], uint64(manager.state.pagesCount))
	binary.LittleEndian.PutUint64(masterPage[24:], uint64(manager.state.pageBuffer.head))

	manager.storage.UpdateMemorySegment(masterPage, 0)
}

func (manager *PageManager) extendStorage(desireNumberOfPages int) error {
	storagePages := manager.storage.Size() / manager.config.pageSize

	if storagePages >= desireNumberOfPages {
		return nil
	}

	for storagePages < desireNumberOfPages {
		incrementPages := int(math.Ceil(float64(desireNumberOfPages) / 8))
		storagePages += incrementPages
	}

	storageSize := storagePages * manager.config.pageSize

	if err := manager.storage.IncreaseSize(storageSize); err != nil {
		return err
	}

	return nil
}

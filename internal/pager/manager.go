package pager

import (
	"bytes"
	"distributed-storage/internal/backend"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const NULL_PAGE = PagePointer(0)

const META_INFO_HEADER_SIZE = 40 // size of meta info  in file bytes

type PagePointer = uint64

type PageManager struct {
	backend backend.Backend
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
	releasedPagesCount  int                    // count of pages that were released before saving
	pageBuffer          *PageBuffer            //  buffer of available pages
	pageUpdates         map[PagePointer][]byte // map of page updates that will be synced with file
}

const SIGNATURE = "PAGE_MANAGER_SIG"

/*
	We book one page for storing information about number of used and freed pages
	This page is called as "master"

	Mater Page Format

 	| signature | number of stored pages | page pool pointer | user meta info |
	|    16B    |           8B           |         8B        |     ...rest    |
*/

func NewPageManager(backend backend.Backend, pageSize int) (*PageManager, error) {
	firstInitialization := false
	backendSize := backend.Size()

	if backendSize > 0 && backendSize%pageSize != 0 {
		return nil, errors.New("PageManager: support only files with size of multiple pages")
	}

	if backendSize == 0 {
		firstInitialization = true
		backendSize = pageSize * 10

		if err := backend.IncreaseSize(backendSize); err != nil {
			return nil, err
		}
	}

	manager := &PageManager{
		backend: backend,
		config: PageManagerConfig{
			pageSize: pageSize,
		},
		state: PageManagerState{
			pagesCount:          0,
			allocatedPagesCount: 0,
			reusedPagesCount:    0,
			releasedPagesCount:  0,
			pageBuffer:          nil,
			pageUpdates:         map[PagePointer][]byte{},
		},
	}

	if firstInitialization {
		manager.state.pagesCount = 1 // reserve master page
		manager.state.pageBuffer = NewPageBuffer(manager.allocateVirtualPage(), manager)
	} else {
		if err := manager.parseMasterPage(); err != nil {
			return nil, err
		}
	}

	return manager, nil
}

func (manager *PageManager) GetMetaInfo() []byte {
	masterPage := manager.getMasterPage()
	return masterPage[META_INFO_HEADER_SIZE:]
}

func (manager *PageManager) WriteMetaInfo(meta []byte) error {
	if len(meta) > manager.config.pageSize-META_INFO_HEADER_SIZE {
		panic(fmt.Sprintf("PageManager: couldn`t store metadata with size %d", len(meta)))
	}

	masterPage := manager.getMasterPage()

	copy(masterPage[META_INFO_HEADER_SIZE:], meta)

	if err := manager.backend.FlushMemoryBlock(masterPage, 0); err != nil {
		return err
	}

	return nil
}

func (manager *PageManager) GetState() PageManagerState {
	return manager.state
}

func (manager *PageManager) ApplyState(state PageManagerState) {
	manager.state = state
}

func (manager *PageManager) GetPage(pointer PagePointer) []byte {
	if page, exist := manager.state.pageUpdates[pointer]; exist {
		return page
	}

	return manager.backend.MemoryBlock(manager.config.pageSize, int(pointer)*manager.config.pageSize)
}

func (manager *PageManager) CreatePage(data []byte) PagePointer {
	pagePointer := manager.reuseFilePage()

	if pagePointer == NULL_PAGE {
		pagePointer = manager.allocateVirtualPage()
	}

	manager.state.pageUpdates[pagePointer] = data

	return pagePointer
}

func (manager *PageManager) DeletePage(pointer PagePointer) {
	manager.state.pageUpdates[pointer] = nil
	manager.state.releasedPagesCount++
}

func (manager *PageManager) GetPagesCount() int {
	return manager.state.pagesCount + manager.state.allocatedPagesCount
}

func (manager *PageManager) WritePages() error {
	if err := manager.saveAllocatedPages(); err != nil {
		return err
	}

	manager.saveReleasedPages()
	manager.saveMasterPage()

	return manager.backend.Flush()
}

func (manager *PageManager) reuseFilePage() PagePointer {
	if manager.state.reusedPagesCount == manager.state.pageBuffer.getPagesCount() {
		return NULL_PAGE
	}

	manager.state.reusedPagesCount++

	return manager.state.pageBuffer.getPage(manager.state.reusedPagesCount - 1)
}

func (manager *PageManager) allocateVirtualPage() PagePointer {
	page := make([]byte, manager.config.pageSize)
	pointer := PagePointer(manager.state.pagesCount + manager.state.allocatedPagesCount)

	manager.state.allocatedPagesCount++
	manager.state.pageUpdates[pointer] = page

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

	if !bytes.Equal([]byte(SIGNATURE), fileSignature) {
		return errors.New("PageManager: file is corrupted")
	}

	manager.state.pagesCount = int(pagesCount)
	manager.state.pageBuffer = NewPageBuffer(PagePointer(pageBufferHead), manager)

	return nil
}

func (manager *PageManager) saveReleasedPages() {
	releasedPages := make([]PagePointer, 0, manager.state.releasedPagesCount)

	for pointer, page := range manager.state.pageUpdates {
		if page == nil {
			releasedPages = append(releasedPages, pointer)
			defer delete(manager.state.pageUpdates, pointer)
		}
	}

	manager.state.pageBuffer.updatePages(manager.state.reusedPagesCount, releasedPages)

	manager.state.releasedPagesCount = 0
	manager.state.reusedPagesCount = 0
}

func (manager *PageManager) saveAllocatedPages() error {
	manager.state.pagesCount += manager.state.allocatedPagesCount

	if err := manager.extendStorage(manager.state.pagesCount); err != nil {
		return err
	}

	for pointer, page := range manager.state.pageUpdates {
		if page != nil {
			manager.backend.UpdateMemoryBlock(page, int(pointer)*manager.config.pageSize)
		}
	}

	manager.state.allocatedPagesCount = 0
	manager.state.pageUpdates = map[PagePointer][]byte{}

	return nil
}

func (manager *PageManager) saveMasterPage() {
	masterPage := manager.getMasterPage()

	copy(masterPage[0:16], []byte(SIGNATURE))

	binary.LittleEndian.PutUint64(masterPage[16:], uint64(manager.state.pagesCount))
	binary.LittleEndian.PutUint64(masterPage[24:], uint64(manager.state.pageBuffer.head))

	manager.backend.UpdateMemoryBlock(masterPage, 0)
}

func (manager *PageManager) extendStorage(desireNumberOfPages int) error {
	filePages := manager.backend.Size() / manager.config.pageSize

	if filePages >= desireNumberOfPages {
		return nil
	}

	for filePages < desireNumberOfPages {
		incrementPages := int(math.Ceil(float64(desireNumberOfPages) / 8))
		filePages += incrementPages
	}

	fileSize := filePages * manager.config.pageSize

	if err := manager.backend.IncreaseSize(fileSize); err != nil {
		return err
	}

	return nil
}

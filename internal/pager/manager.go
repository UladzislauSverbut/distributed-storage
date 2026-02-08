package pager

import (
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/store"
)

const NULL_PAGE = PagePointer(0)

type PagePointer = uint64

type PageManagerConfig struct {
	pageSize int
}

type PageManagerState struct {
	AllocatedPages helpers.Set[PagePointer] // set of pages that were given by allocator
	AvailablePages helpers.Set[PagePointer] // set of pages that are available for usage
	ReleasedPages  helpers.Set[PagePointer] // set of pages that were released and cannot be used until commit
	TotalPages     uint64                   // total number of pages in storage
	pageUpdates    map[PagePointer][]byte   // map of page updates that will be synced with storage
}

type PageManager struct {
	storage store.Storage
	config  PageManagerConfig
	state   PageManagerState
}

const SIGNATURE = "PAGE_MANAGER_SIG"

func NewPageManager(storage store.Storage, pagesNumber uint64, pageSize int) *PageManager {
	manager := &PageManager{
		storage: storage,
		config: PageManagerConfig{
			pageSize: pageSize,
		},
		state: PageManagerState{
			AllocatedPages: helpers.NewSet[PagePointer](),
			AvailablePages: helpers.NewSet[PagePointer](),
			ReleasedPages:  helpers.NewSet[PagePointer](),
			TotalPages:     pagesNumber,
			pageUpdates:    map[PagePointer][]byte{},
		},
	}

	return manager
}

func (manager *PageManager) Page(pointer PagePointer) []byte {
	if page, exist := manager.state.pageUpdates[pointer]; exist {
		return page
	}

	return manager.storage.MemorySegment(int(pointer)*manager.config.pageSize, manager.config.pageSize)
}

func (manager *PageManager) CreatePage(data []byte) PagePointer {
	var pagePointer PagePointer

	if availablePage, ok := manager.state.AvailablePages.Pop(); ok {
		pagePointer = availablePage
	} else {
		manager.state.TotalPages++

		pagePointer = manager.state.TotalPages
		manager.state.AllocatedPages.Add(pagePointer)
	}

	manager.state.pageUpdates[pagePointer] = data
	return pagePointer
}

func (manager *PageManager) FreePage(pointer PagePointer) {
	// If released page was allocated than we can return it to free pages because nobody can reference this page in other transactions
	if manager.state.AllocatedPages.Has(pointer) {
		manager.state.AvailablePages.Add(pointer)
	} else {
		manager.state.ReleasedPages.Add(pointer)
	}

	delete(manager.state.pageUpdates, pointer)
}

func (manager *PageManager) ReleasedPages() []PagePointer {
	return manager.state.ReleasedPages.Values()
}

func (manager *PageManager) AllocatedPages() []PagePointer {
	return manager.state.AllocatedPages.Values()
}

func (manager *PageManager) TotalPages() uint64 {
	return manager.state.TotalPages
}

func (manager *PageManager) Save() error {
	for pointer, page := range manager.state.pageUpdates {
		if page != nil {
			if err := manager.storage.UpdateMemorySegment(int(pointer)*manager.config.pageSize, page[0:manager.config.pageSize]); err != nil {
				return err
			}
		}
	}

	// Clear all page updates after saving because they are already applied to storage
	manager.state.pageUpdates = map[PagePointer][]byte{}

	return nil
}

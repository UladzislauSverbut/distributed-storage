package pager

import (
	"distributed-storage/internal/store"
	"errors"
	"math"
)

const NULL_PAGE = PagePointer(0)

type PagePointer = uint64

type PageManager struct {
	storage   store.Storage
	allocator *PageAllocator
	config    PageManagerConfig
	state     PageManagerState
}

type PageManagerConfig struct {
	pageSize int
}

type PageManagerState struct {
	AllocatedPages Set[PagePointer]       // set of pages that were given by allocator
	AvailablePages Set[PagePointer]       // set of pages that are available for usage
	ReleasedPages  Set[PagePointer]       // set of pages that were released and cannot be used until commit
	pageUpdates    map[PagePointer][]byte // map of page updates that will be synced with storage
}

const SIGNATURE = "PAGE_MANAGER_SIG"

func NewPageManager(storage store.Storage, allocator *PageAllocator, pageSize int) (*PageManager, error) {
	storageSize := storage.Size()

	if storageSize > 0 && storageSize%pageSize != 0 {
		return nil, errors.New("PageManager: support only storage with size of multiple pages")
	}

	if storageSize == 0 {
		storageSize = pageSize * 10

		if err := storage.IncreaseSize(storageSize); err != nil {
			return nil, err
		}
	}

	manager := &PageManager{
		storage:   storage,
		allocator: allocator,
		config: PageManagerConfig{
			pageSize: pageSize,
		},
		state: PageManagerState{
			AllocatedPages: NewSet[PagePointer](),
			AvailablePages: NewSet[PagePointer](),
			ReleasedPages:  NewSet[PagePointer](),
			pageUpdates:    map[PagePointer][]byte{},
		},
	}

	return manager, nil
}

func (manager *PageManager) State() PageManagerState {
	return manager.state
}

func (manager *PageManager) Page(pointer PagePointer) []byte {
	if page, exist := manager.state.pageUpdates[pointer]; exist {
		return page
	}

	return manager.storage.MemorySegment(manager.config.pageSize, int(pointer)*manager.config.pageSize)
}

func (manager *PageManager) CreatePage(data []byte) PagePointer {
	var pagePointer PagePointer

	if availablePage, ok := manager.state.AvailablePages.Pop(); ok {
		pagePointer = availablePage
	} else {
		pagePointer = manager.allocator.Get()
		manager.state.AllocatedPages.Add(pagePointer)
	}

	manager.state.pageUpdates[pagePointer] = data
	return pagePointer
}

func (manager *PageManager) FreePage(pointer PagePointer) {
	// if released page was allocated than we can return it to free pages because nobody can reference this page in other transactions
	if manager.state.AllocatedPages.Has(pointer) {
		manager.state.AvailablePages.Add(pointer)
	} else {
		manager.state.ReleasedPages.Add(pointer)
	}

	delete(manager.state.pageUpdates, pointer)
}

func (manager *PageManager) SavePages() error {
	for pointer, page := range manager.state.pageUpdates {
		if page != nil {
			manager.storage.UpdateMemorySegment(int(pointer)*manager.config.pageSize, page[0:manager.config.pageSize])
			delete(manager.state.pageUpdates, pointer)
		}
	}

	return nil
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

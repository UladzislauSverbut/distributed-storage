package pager

import (
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/store"
)

const NULL_PAGE = PagePointer(0)

type PagePointer = uint64

type PageAllocatorConfig struct {
	pageSize int
}

type PageAllocatorState struct {
	TotalPages    uint64                   // total number of pages in storage
	PagePool      helpers.Set[PagePointer] // total set of pages that are not reachable by others and could be reused
	ReusablePages helpers.Set[PagePointer] // set of pages that are prepared for reuse
	ReleasedPages helpers.Set[PagePointer] // set of pages that were released and cannot be overwritten due to immutability
	pageUpdates   map[PagePointer][]byte   // map of page updates that will be synced with storage
}

type PageAllocator struct {
	storage store.Storage
	config  PageAllocatorConfig
	state   PageAllocatorState
}

func NewPageAllocator(storage store.Storage, pagesNumber uint64, pageSize int, availablePages ...PagePointer) *PageAllocator {
	allocator := &PageAllocator{
		storage: storage,
		config: PageAllocatorConfig{
			pageSize: pageSize,
		},
		state: PageAllocatorState{
			TotalPages:    pagesNumber,
			PagePool:      helpers.NewSet(availablePages...),
			ReusablePages: helpers.NewSet(availablePages...),
			ReleasedPages: helpers.NewSet[PagePointer](),
			pageUpdates:   map[PagePointer][]byte{},
		},
	}

	return allocator
}

func (allocator *PageAllocator) Page(pointer PagePointer) []byte {
	if page, exist := allocator.state.pageUpdates[pointer]; exist {
		return page
	}

	return allocator.storage.MemorySegment(int(pointer)*allocator.config.pageSize, allocator.config.pageSize)
}

func (allocator *PageAllocator) CreatePage(data []byte) PagePointer {
	var pagePointer PagePointer

	if availablePage, ok := allocator.state.ReusablePages.Pop(); ok {
		pagePointer = availablePage
	} else {
		allocator.state.TotalPages++

		pagePointer = allocator.state.TotalPages
		allocator.state.PagePool.Add(pagePointer)
	}

	allocator.state.pageUpdates[pagePointer] = data
	return pagePointer
}

func (allocator *PageAllocator) FreePage(pointer PagePointer) {
	// If released page was in page pool than we can return it to reusable pages because nobody can reference this page
	if allocator.state.PagePool.Has(pointer) {
		allocator.state.ReusablePages.Add(pointer)
	} else {
		allocator.state.ReleasedPages.Add(pointer)
	}

	delete(allocator.state.pageUpdates, pointer)
}

func (allocator *PageAllocator) ReleasedPages() []PagePointer {
	return allocator.state.ReleasedPages.Values()
}

func (allocator *PageAllocator) ReusablePages() []PagePointer {
	return allocator.state.ReusablePages.Values()
}

func (allocator *PageAllocator) TotalPages() uint64 {
	return allocator.state.TotalPages
}

func (allocator *PageAllocator) Save() error {
	for pointer, page := range allocator.state.pageUpdates {
		if page != nil {
			if err := allocator.storage.UpdateMemorySegment(int(pointer)*allocator.config.pageSize, page[0:allocator.config.pageSize]); err != nil {
				return err
			}
		}
	}

	// Clear all page updates after saving because they are already applied to storage
	allocator.state.pageUpdates = map[PagePointer][]byte{}

	return nil
}

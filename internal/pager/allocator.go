package pager

import (
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/store"
	"fmt"
)

const NULL_PAGE = PagePointer(0)

type PagePointer = uint64

type PageAllocatorConfig struct {
	pageSize int
}

type PageAllocatorState struct {
	TotalPages    uint64                   // Total number of pages in storage
	PagePool      helpers.Set[PagePointer] // Total set of pages that are not reachable by others and could be reused
	ReusablePages helpers.Set[PagePointer] // Set of pages that are prepared for reuse
	ReleasedPages helpers.Set[PagePointer] // Set of pages that were released and cannot be overwritten due to immutability
	pageUpdates   map[PagePointer][]byte   // Map of page updates that will be synced with storage
}

type PageAllocator struct {
	storage store.Storage
	config  PageAllocatorConfig
	state   PageAllocatorState
}

func NewPageAllocator(storage store.Storage, pagesNumber uint64, pageSize int, availablePages ...PagePointer) *PageAllocator {
	return &PageAllocator{
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
}

func (allocator *PageAllocator) Page(pointer PagePointer) []byte {
	if page, exist := allocator.state.pageUpdates[pointer]; exist {
		return page
	}

	return allocator.storage.Segment(int(pointer)*allocator.config.pageSize, allocator.config.pageSize)
}

func (allocator *PageAllocator) UpdatePage(pointer PagePointer, data []byte) error {
	if pointer > allocator.state.TotalPages {
		return fmt.Errorf("PageAllocator: invalid page pointer %d (total pages: %d)", pointer, allocator.state.TotalPages)
	}

	allocator.state.pageUpdates[pointer] = data
	return nil
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
	// If released page was in page pool we can return it to reusable pages because nobody can reference this page
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

func (allocator *PageAllocator) SaveChanges() error {
	updates := make([]store.SegmentUpdate, 0, len(allocator.state.pageUpdates))

	for pointer, page := range allocator.state.pageUpdates {

		updates = append(updates,
			store.SegmentUpdate{
				Offset: int(pointer) * allocator.config.pageSize,
				Data:   page[:allocator.config.pageSize],
			},
		)
	}

	if err := allocator.storage.UpdateSegments(updates); err != nil {
		return fmt.Errorf("PageAllocator: failed to save changes: %w", err)
	}

	allocator.state.pageUpdates = map[PagePointer][]byte{}

	return nil
}

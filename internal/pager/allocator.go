package pager

import (
	"distributed-storage/internal/helpers"
	"sync/atomic"
)

type PageBlock struct {
	pages     []PagePointer
	previous  *PageBlock
	blockSize int
	totalSize int
}

type PageAllocator struct {
	head       atomic.Pointer[PageBlock]
	pagesCount atomic.Uint64
}

func NewPageAllocator(pagesCount uint64) *PageAllocator {
	allocator := &PageAllocator{
		head:       atomic.Pointer[PageBlock]{},
		pagesCount: atomic.Uint64{},
	}

	allocator.head.Store(&PageBlock{
		pages:     make([]PagePointer, 0),
		previous:  nil,
		blockSize: 0,
		totalSize: 0,
	})

	allocator.pagesCount.Store(pagesCount)

	return allocator
}

func (allocator *PageAllocator) Free(pages []PagePointer) {
	pages = helpers.CopySlice(pages)
	newHead := &PageBlock{}

	for {
		head := allocator.head.Load()

		newHead.pages = pages
		newHead.previous = head
		newHead.blockSize = len(pages)
		newHead.totalSize = head.totalSize + len(pages)

		if allocator.head.CompareAndSwap(head, newHead) {
			return
		}
	}
}

func (allocator *PageAllocator) Count() uint64 {
	return allocator.pagesCount.Load()
}

func (allocator *PageAllocator) Get() PagePointer {
	if pointer := allocator.reuse(); pointer != NULL_PAGE {
		return pointer
	}

	return PagePointer(allocator.pagesCount.Add(1))
}

func (allocator *PageAllocator) reuse() PagePointer {
	for {
		head := allocator.head.Load()
		newHead := &PageBlock{}
		if head.totalSize == 0 {

			return NULL_PAGE
		}

		if head.blockSize == 1 {
			if allocator.head.CompareAndSwap(head, head.previous) {
				return head.pages[0]
			}
		} else {
			newHead.pages = head.pages[:head.blockSize-1]
			newHead.blockSize = head.blockSize - 1
			newHead.totalSize = head.totalSize - 1
			newHead.previous = head.previous

			if allocator.head.CompareAndSwap(head, newHead) {
				return head.pages[head.blockSize-1]
			}
		}
	}
}

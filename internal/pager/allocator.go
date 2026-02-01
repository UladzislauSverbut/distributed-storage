package pager

import (
	"sync"
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
	pagesCount uint64

	pool sync.Pool
}

func NewPageAllocator(pagesCount uint64) *PageAllocator {
	allocator := &PageAllocator{
		head:       atomic.Pointer[PageBlock]{},
		pagesCount: pagesCount,

		pool: sync.Pool{
			New: func() any {
				return &PageBlock{
					pages:     make([]PagePointer, 0),
					previous:  nil,
					blockSize: 0,
					totalSize: 0,
				}
			},
		},
	}

	allocator.head.Store(&PageBlock{
		pages:     make([]PagePointer, 0),
		previous:  nil,
		blockSize: 0,
		totalSize: 0,
	})

	return allocator
}

func (allocator *PageAllocator) Free(pages []PagePointer) {
	pages = copySlice(pages)

	for {
		head := allocator.head.Load()

		newHead := allocator.pool.Get().(*PageBlock)
		newHead.pages = pages
		newHead.previous = head
		newHead.blockSize = len(pages)
		newHead.totalSize = head.totalSize + len(pages)

		if allocator.head.CompareAndSwap(head, newHead) {
			return
		}

		allocator.pool.Put(newHead)
	}
}

func (allocator *PageAllocator) Get() PagePointer {
	if pointer := allocator.reuse(); pointer != NULL_PAGE {
		return pointer
	}

	return PagePointer(atomic.AddUint64(&allocator.pagesCount, 1))
}

func (allocator *PageAllocator) reuse() PagePointer {
	for {
		head := allocator.head.Load()

		switch head.totalSize {
		case 0:
			return NULL_PAGE
		case 1:
			if allocator.head.CompareAndSwap(head, head.previous) {
				return head.pages[0]
			}
		default:
			newHead := allocator.pool.Get().(*PageBlock)
			newHead.pages = head.pages[:head.blockSize-1]
			newHead.previous = head.previous
			newHead.blockSize = head.blockSize - 1
			newHead.totalSize = head.totalSize - 1

			if allocator.head.CompareAndSwap(head, newHead) {
				return head.pages[head.blockSize-1]
			}

			allocator.pool.Put(newHead)
		}
	}
}

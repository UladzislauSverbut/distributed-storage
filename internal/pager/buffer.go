package pager

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync/atomic"
	"unsafe"
)

const HEADER_SIZE = 18 // size of block header in file bytes

/*
   Page Buffer Block Format

   | pages stored in block | pointer to previous block | pointers to pages in block with their versions |
   |          2B           |             8B            |    			number of pages * 16B    		   |
*/

type PageBuffer struct {
	head        *PagePointer // pointer to the first block in the buffer
	pageManager *PageManager
}

func NewPageBuffer(head *PagePointer, pageManager *PageManager) *PageBuffer {
	return &PageBuffer{
		head:        head,
		pageManager: pageManager,
	}
}

func (buffer *PageBuffer) Extract() PagePointer {
	for {
		head := atomic.LoadUint64(buffer.head)
		pagesBlock := buffer.pageManager.Page(head)

		if buffer.blockSize(pagesBlock) == 0 {
			return NULL_PAGE
		}

		newHead := buffer.pageAt(pagesBlock, 1)
		newBlock := buffer.pageManager.Page(newHead)

		// we can fill this node before CAS because other gouroutines will to fill this node with the same data
		// outside of buffer this node become immutable
		buffer.initBlock(newBlock, buffer.previousBlock(pagesBlock), buffer.blockPages(pagesBlock, 1, buffer.blockSize(pagesBlock)))

		if atomic.CompareAndSwapUint64(buffer.head, head, uint64(newHead)) {
			return head
		}

		runtime.Gosched()
	}
}

func (buffer *PageBuffer) Add(pages []PagePointer) {
	for {
		head := atomic.LoadUint64(buffer.head)
		newHead := head
		addedPages := pages

		for len(addedPages) > 0 {
			newHead = addedPages[0]
			addedPages = addedPages[1:]

			newPagesBlock := buffer.pageManager.Page(newHead)

			buffer.initBlock(newPagesBlock, head, addedPages[:min(buffer.blockCapacity(newPagesBlock), len(addedPages))])

			buffer.pageManager.UpdatePage(newHead, newPagesBlock)
			addedPages = addedPages[min(buffer.blockCapacity(newPagesBlock), len(addedPages)):]
		}

		if atomic.CompareAndSwapUint64(buffer.head, head, uint64(newHead)) {
			return
		}

		runtime.Gosched()
	}
}

func (buffer *PageBuffer) pageAt(pagesBlock []byte, pageNumber int) PagePointer {

	for buffer.blockSize(pagesBlock) < pageNumber {
		pageNumber -= buffer.blockSize(pagesBlock)
		pagesBlock = buffer.pageManager.Page(buffer.previousBlock(pagesBlock))
	}

	return buffer.blockPage(pagesBlock, pageNumber)
}

func (buffer *PageBuffer) blockPage(pagesBlock []byte, pageNumber int) PagePointer {
	return PagePointer(binary.LittleEndian.Uint64(pagesBlock[HEADER_SIZE+(pageNumber*8):]))
}

func (buffer *PageBuffer) blockPages(pagesBlock []byte, start int, end int) []PagePointer {
	pages := pagesBlock[HEADER_SIZE+(start*8) : HEADER_SIZE+(end*8)]

	return unsafe.Slice((*PagePointer)(unsafe.Pointer(&pages[0])), len(pages)/8)
}

func (buffer *PageBuffer) blockSize(pagesBlock []byte) int {
	return int(binary.LittleEndian.Uint16(pagesBlock))
}

func (buffer *PageBuffer) blockCapacity(pagesBlock []byte) int {
	return (len(pagesBlock) - HEADER_SIZE) / 8
}

func (buffer *PageBuffer) previousBlock(pagesBlock []byte) PagePointer {
	return PagePointer(binary.LittleEndian.Uint64(pagesBlock[10:]))
}

func (buffer *PageBuffer) initBlock(pagesBlock []byte, previousBlockPointer PagePointer, pagePointers []PagePointer) {
	storedPagesCount := len(pagePointers)

	if buffer.blockCapacity(pagesBlock) < storedPagesCount {
		panic(fmt.Sprintf("PageBuffer: not enough space in block to store %d pages", storedPagesCount))
	}

	binary.LittleEndian.PutUint16(pagesBlock, uint16(storedPagesCount))
	binary.LittleEndian.PutUint64(pagesBlock[2:], uint64(previousBlockPointer))

	copy(pagesBlock[HEADER_SIZE:], unsafe.Slice((*byte)(unsafe.Pointer(&pagePointers[0])), len(pagePointers)*8))
}

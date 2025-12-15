package pager

import (
	"encoding/binary"
	"fmt"
)

const HEADER_SIZE = 18 // size of block header in file bytes

/*
   Page Buffer Block Format

   | pages stored in block | total pages in chain | pointer to previous block | pointers to pages in block with their versions |
   |          2B           |          8B          |             8B            |    			number of pages * 16B    		   |
*/

type PageBuffer struct {
	head          PagePointer   // pointer to the first block in the buffer
	reusablePages []PagePointer // pages that can be reused for internal use
	pageManager   *PageManager
}

func NewPageBuffer(head PagePointer, pageManager *PageManager) *PageBuffer {
	return &PageBuffer{
		head:          head,
		pageManager:   pageManager,
		reusablePages: make([]PagePointer, 0),
	}
}

func (buffer *PageBuffer) pageAt(pageNumber int) PagePointer {
	pagesBlock := buffer.pageManager.Page(buffer.head)

	for buffer.blockSize(pagesBlock) < pageNumber {
		pageNumber -= buffer.blockSize(pagesBlock)
		pagesBlock = buffer.pageManager.Page(buffer.previousBlock(pagesBlock))
	}

	return buffer.blockPage(pagesBlock, pageNumber)
}

func (buffer *PageBuffer) applyChanges(removedPagesCount int, addedPages []PagePointer) {
	pagesBlockPointer := buffer.head
	pagesBlock := buffer.pageManager.Page(pagesBlockPointer)

	if buffer.blockTotalPages(pagesBlock) < removedPagesCount {
		panic(fmt.Sprintf("PagePool: not enough pages to remove %d pages", removedPagesCount))
	}

	if removedPagesCount > 0 {
		addedPages = append(addedPages, buffer.removePages(removedPagesCount)...)
	}

	if len(addedPages) > 0 {
		buffer.addPages(addedPages)
	}
}

func (buffer *PageBuffer) removePages(count int) []PagePointer {
	pagesBlockPointer := buffer.head
	pagesBlock := buffer.pageManager.Page(pagesBlockPointer)
	releasedPages := make([]PagePointer, 0)

	for count > buffer.blockSize(pagesBlock) {
		releasedPages = append(releasedPages, pagesBlockPointer)

		count -= buffer.blockSize(pagesBlock)
		pagesBlockPointer = buffer.previousBlock(pagesBlock)
		pagesBlock = buffer.pageManager.Page(pagesBlockPointer)
	}

	if count != 0 {
		releasedPages = append(releasedPages, pagesBlockPointer)

		buffer.reusablePages = append(buffer.reusablePages, buffer.blockPages(pagesBlock, count, buffer.blockSize(pagesBlock))...)

		pagesBlockPointer = buffer.previousBlock(pagesBlock)

	}

	buffer.head = pagesBlockPointer

	return releasedPages
}

func (buffer *PageBuffer) availablePageCount() int {
	pagesBlock := buffer.pageManager.Page(buffer.head)

	return buffer.blockTotalPages(pagesBlock)
}

func (buffer *PageBuffer) addPages(pages []PagePointer) {
	pagesBlockPointer := buffer.head
	pagesBlock := buffer.pageManager.Page(pagesBlockPointer)

	for len(pages) > 0 {
		newBlockPointer := NULL_PAGE

		if len(buffer.reusablePages) > 0 {
			newBlockPointer = buffer.reusablePages[0]
			buffer.reusablePages = buffer.reusablePages[1:]
		} else {
			newBlockPointer = buffer.pageManager.allocateVirtualPage()
		}

		newPagesBlock := buffer.pageManager.Page(newBlockPointer)

		if len(pages)+len(buffer.reusablePages) <= buffer.blockCapacity(newPagesBlock) {
			pages = append(pages, buffer.reusablePages...)
			buffer.reusablePages = nil
		}

		buffer.initBlock(newPagesBlock, pagesBlock, pagesBlockPointer, pages[:min(buffer.blockCapacity(newPagesBlock), len(pages))])

		buffer.pageManager.UpdatePage(newBlockPointer, newPagesBlock)

		pages = pages[min(buffer.blockCapacity(newPagesBlock), len(pages)):]
		pagesBlockPointer = newBlockPointer
		pagesBlock = newPagesBlock

		if len(pages) == 0 && len(buffer.reusablePages) > 0 {
			pages = append(pages, buffer.reusablePages[0])
			buffer.reusablePages = buffer.reusablePages[1:]
		}
	}

	buffer.reusablePages = make([]PagePointer, 0)
	buffer.head = pagesBlockPointer
}

func (buffer *PageBuffer) blockPage(pagesBlock []byte, pageNumber int) PagePointer {
	return PagePointer(binary.LittleEndian.Uint64(pagesBlock[HEADER_SIZE+(pageNumber*8):]))
}

func (buffer *PageBuffer) blockPages(pagesBlock []byte, start int, end int) []PagePointer {
	pages := make([]PagePointer, 0, end-start)

	for pageNumber := start; pageNumber < end; pageNumber++ {
		pages = append(pages, buffer.blockPage(pagesBlock, pageNumber))
	}

	return pages
}

func (buffer *PageBuffer) blockTotalPages(pagesBlock []byte) int {
	return int(binary.LittleEndian.Uint64(pagesBlock[2:10]))
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

func (buffer *PageBuffer) initBlock(pagesBlock []byte, previousBlock []byte, previousBlockPointer PagePointer, storedPages []PagePointer) {
	storedPagesCount := len(storedPages)

	if buffer.blockCapacity(pagesBlock) < storedPagesCount {
		panic(fmt.Sprintf("PageBuffer: not enough space in block to store %d pages", storedPagesCount))
	}

	binary.LittleEndian.PutUint16(pagesBlock, uint16(storedPagesCount))
	binary.LittleEndian.PutUint64(pagesBlock[2:], uint64(buffer.blockTotalPages(previousBlock)+storedPagesCount))
	binary.LittleEndian.PutUint64(pagesBlock[10:], previousBlockPointer)

	for i, pagePointer := range storedPages {
		binary.LittleEndian.PutUint64(pagesBlock[HEADER_SIZE+(i*8):], pagePointer)
	}
}

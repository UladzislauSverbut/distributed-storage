package fs

import (
	"encoding/binary"
	"fmt"
)

const HEADER_SIZE = 18 // size of node header in file bytes

/*
   Page Buffer Node Format

   | node stored pages | total stored pages | pointer to previous buffer node | pointers to node stored pages |
   |         2B        |         8B         |                8B               |  number of stored pages * 8B  |
*/

type PageBuffer struct {
	head          PagePointer   // pointer to the first node in the buffer
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

func (buffer *PageBuffer) getPage(pageNumber int) PagePointer {
	node := buffer.pageManager.GetPage(buffer.head)

	for buffer.getNodeSize(node) < pageNumber {
		pageNumber -= buffer.getNodeSize(node)
		node = buffer.pageManager.GetPage((buffer.getPreviousNode(node)))
	}

	return buffer.getNodePage(node, pageNumber)
}

func (buffer *PageBuffer) getPagesCount() int {
	node := buffer.pageManager.GetPage(buffer.head)

	return buffer.getNodeTotalPages(node)
}

func (buffer *PageBuffer) updatePages(removedPagesCount int, addedPages []PagePointer) {
	nodePointer := buffer.head
	node := buffer.pageManager.GetPage(nodePointer)

	if buffer.getNodeTotalPages(node) < removedPagesCount {
		panic(fmt.Sprintf("PagePool: not enough pages to remove %d pages", removedPagesCount))
	}

	if removedPagesCount > 0 {
		addedPages = append(addedPages, buffer.removePages(removedPagesCount)...)
	}

	if len(addedPages) > 0 {
		buffer.savePages(addedPages)
	}
}

func (buffer *PageBuffer) removePages(count int) []PagePointer {
	headPointer := buffer.head
	head := buffer.pageManager.GetPage(headPointer)
	releasedPages := make([]PagePointer, 0)

	for count > buffer.getNodeSize(head) {
		releasedPages = append(releasedPages, headPointer)

		count -= buffer.getNodeSize(head)
		headPointer = buffer.getPreviousNode(head)
		head = buffer.pageManager.GetPage(headPointer)
	}

	if count != 0 {
		releasedPages = append(releasedPages, headPointer)

		buffer.reusablePages = append(buffer.reusablePages, buffer.getNodePages(head, count, buffer.getNodeSize(head))...)

		headPointer = buffer.getPreviousNode(head)

	}

	buffer.head = headPointer

	return releasedPages
}

func (buffer *PageBuffer) savePages(pages []PagePointer) {
	headPointer := buffer.head
	head := buffer.pageManager.GetPage(headPointer)

	for len(pages) > 0 {
		newHeadPointer := NULL_PAGE

		if len(buffer.reusablePages) > 0 {
			newHeadPointer = buffer.reusablePages[0]
			buffer.reusablePages = buffer.reusablePages[1:]
		} else {
			newHeadPointer = buffer.pageManager.allocateVirtualPage()
		}

		newHead := buffer.pageManager.GetPage(newHeadPointer)

		if len(pages)+len(buffer.reusablePages) <= buffer.getNodeCapacity(newHead) {
			pages = append(pages, buffer.reusablePages...)
			buffer.reusablePages = nil
		}

		buffer.fillNode(newHead, head, headPointer, pages[:min(buffer.getNodeCapacity(newHead), len(pages))])

		pages = pages[min(buffer.getNodeCapacity(newHead), len(pages)):]
		headPointer = newHeadPointer
		head = newHead

		if len(pages) == 0 && len(buffer.reusablePages) > 0 {
			pages = append(pages, buffer.reusablePages[0])
			buffer.reusablePages = buffer.reusablePages[1:]
		}
	}

	buffer.reusablePages = make([]PagePointer, 0)
	buffer.head = headPointer
}

func (buffer *PageBuffer) getNodePage(node []byte, pageNumber int) PagePointer {
	return PagePointer(binary.LittleEndian.Uint64(node[HEADER_SIZE+(pageNumber*8):]))
}

func (buffer *PageBuffer) getNodePages(node []byte, start int, end int) []PagePointer {
	pages := make([]PagePointer, 0, end-start)

	for pageNumber := start; pageNumber < end; pageNumber++ {
		pages = append(pages, buffer.getNodePage(node, pageNumber))
	}

	return pages
}

func (buffer *PageBuffer) getNodeTotalPages(node []byte) int {
	return int(binary.LittleEndian.Uint64(node[2:10]))
}

func (buffer *PageBuffer) getNodeSize(node []byte) int {
	return int(binary.LittleEndian.Uint16(node))
}

func (buffer *PageBuffer) getNodeCapacity(node []byte) int {
	return (len(node) - HEADER_SIZE) / 8
}

func (buffer *PageBuffer) getPreviousNode(node []byte) PagePointer {
	return PagePointer(binary.LittleEndian.Uint64(node[10:]))
}

func (buffer *PageBuffer) fillNode(node []byte, previousNode []byte, previousNodePointer PagePointer, storedPages []PagePointer) {
	storedPagesCount := len(storedPages)

	if buffer.getNodeCapacity(node) < storedPagesCount {
		panic(fmt.Sprintf("PageBuffer: not enough space in node to store %d pages", storedPagesCount))
	}

	binary.LittleEndian.PutUint16(node, uint16(storedPagesCount))
	binary.LittleEndian.PutUint64(node[2:], uint64(buffer.getNodeTotalPages(previousNode)+storedPagesCount))
	binary.LittleEndian.PutUint64(node[10:], previousNodePointer)

	for i, pagePointer := range storedPages {
		binary.LittleEndian.PutUint64(node[HEADER_SIZE+(i*8):], pagePointer)
	}
}

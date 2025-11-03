package storage

import (
	"encoding/binary"
	"fmt"
)

const HEADER_SIZE = 18 // size of node header in file bytes

/*
   Page Pool Node Format

   | node stored pages | total stored pages | pointer to previous pool node | pointers to node stored pages |
   |         2B        |         8B         |              8B               |  number of stored pages * 8B  |
*/

type FilePagePool struct {
	head          PagePointer   // pointer to the first node in the pool
	reusablePages []PagePointer // pages that can be reused for internal use
	storage       *FileStorage
}

func NewFilePagePool(head PagePointer, storage *FileStorage) *FilePagePool {
	return &FilePagePool{
		head:          head,
		reusablePages: make([]PagePointer, 0),
		storage:       storage,
	}
}

func (pool *FilePagePool) getPage(pageNumber int) PagePointer {
	node := pool.storage.GetPage(pool.head)

	for pool.getNodeSize(node) < pageNumber {
		pageNumber -= pool.getNodeSize(node)
		node = pool.storage.GetPage((pool.getPreviousNode(node)))
	}

	return pool.getNodePage(node, pageNumber)
}

func (pool *FilePagePool) getPagesCont() int {
	node := pool.storage.GetPage(pool.head)

	return pool.getNodeTotalPages(node)
}

func (pool *FilePagePool) updatePages(removedPagesCount int, addedPages []PagePointer) {
	nodePointer := pool.head
	node := pool.storage.GetPage(nodePointer)

	if pool.getNodeTotalPages(node) < removedPagesCount {
		panic(fmt.Sprintf("PagePool: not enough pages to remove %d pages", removedPagesCount))
	}

	if removedPagesCount > 0 {
		addedPages = append(addedPages, pool.removePages(removedPagesCount)...)
	}

	if len(addedPages) > 0 {
		pool.savePages(addedPages)
	}
}

func (pool *FilePagePool) removePages(count int) []PagePointer {
	headPointer := pool.head
	head := pool.storage.GetPage(headPointer)
	releasedPages := make([]PagePointer, 0)

	for count > pool.getNodeSize(head) {
		releasedPages = append(releasedPages, headPointer)

		count -= pool.getNodeSize(head)
		headPointer = pool.getPreviousNode(head)
		head = pool.storage.GetPage(headPointer)
	}

	if count != 0 {
		releasedPages = append(releasedPages, headPointer)

		pool.reusablePages = append(pool.reusablePages, pool.getNodePages(head, count, pool.getNodeSize(head))...)

		headPointer = pool.getPreviousNode(head)

	}

	pool.head = headPointer

	return releasedPages
}

func (pool *FilePagePool) savePages(pages []PagePointer) {
	headPointer := pool.head
	head := pool.storage.GetPage(headPointer)

	for len(pages) > 0 {
		newHeadPointer := NULL_PAGE

		if len(pool.reusablePages) > 0 {
			newHeadPointer = pool.reusablePages[0]
			pool.reusablePages = pool.reusablePages[1:]
		} else {
			newHeadPointer = pool.storage.allocateVirtualPage()
		}

		newHead := pool.storage.GetPage(newHeadPointer)

		if len(pages)+len(pool.reusablePages) <= pool.getNodeCapacity(newHead) {
			pages = append(pages, pool.reusablePages...)
			pool.reusablePages = nil
		}

		pool.fillNode(newHead, head, headPointer, pages[:min(pool.getNodeCapacity(newHead), len(pages))])

		pages = pages[min(pool.getNodeCapacity(newHead), len(pages)):]
		headPointer = newHeadPointer
		head = newHead

		if len(pages) == 0 && len(pool.reusablePages) > 0 {
			pages = append(pages, pool.reusablePages[0])
			pool.reusablePages = pool.reusablePages[1:]
		}
	}

	pool.reusablePages = make([]PagePointer, 0)
	pool.head = headPointer
}

func (pool *FilePagePool) getNodePage(node []byte, pageNumber int) PagePointer {
	return PagePointer(binary.LittleEndian.Uint64(node[HEADER_SIZE+(pageNumber*8):]))
}

func (pool *FilePagePool) getNodePages(node []byte, start int, end int) []PagePointer {
	pages := make([]PagePointer, 0, end-start)

	for pageNumber := start; pageNumber < end; pageNumber++ {
		pages = append(pages, pool.getNodePage(node, pageNumber))
	}

	return pages
}

func (pool *FilePagePool) getNodeTotalPages(node []byte) int {
	return int(binary.LittleEndian.Uint64(node[2:10]))
}

func (pool *FilePagePool) getNodeSize(node []byte) int {
	return int(binary.LittleEndian.Uint16(node))
}

func (pool *FilePagePool) getNodeCapacity(node []byte) int {
	return (len(node) - HEADER_SIZE) / 8
}

func (pool *FilePagePool) getPreviousNode(node []byte) PagePointer {
	return PagePointer(binary.LittleEndian.Uint64(node[10:]))
}

func (pool *FilePagePool) fillNode(node []byte, previousNode []byte, previousNodePointer PagePointer, storedPages []PagePointer) {
	storedPagesCount := len(storedPages)

	if pool.getNodeCapacity(node) < storedPagesCount {
		panic(fmt.Sprintf("PagePool: not enough space in node to store %d pages", storedPagesCount))
	}

	binary.LittleEndian.PutUint16(node, uint16(storedPagesCount))
	binary.LittleEndian.PutUint64(node[2:], uint64(pool.getNodeTotalPages(previousNode)+storedPagesCount))
	binary.LittleEndian.PutUint64(node[10:], previousNodePointer)

	for i, pagePointer := range storedPages {
		binary.LittleEndian.PutUint64(node[HEADER_SIZE+(i*8):], pagePointer)
	}
}

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

type PagePool struct {
	head    PagePointer
	storage *FileStorage
}

func NewPagePool(head PagePointer, storage *FileStorage) *PagePool {
	return &PagePool{
		head:    head,
		storage: storage,
	}
}

func (pool *PagePool) getPage(pageNumber int) PagePointer {
	node := pool.storage.GetPage(pool.head)

	for pool.getNodeSize(node) < pageNumber {
		pageNumber -= pool.getNodeSize(node)
		node = pool.storage.GetPage((pool.getPreviousNode(node)))
	}

	return pool.getNodePage(node, pageNumber)
}

func (pool *PagePool) getPagesCont() int {
	node := pool.storage.GetPage(pool.head)

	return pool.getNodeTotalPages(node)
}

func (pool *PagePool) updatePages(removedPagesCount int, addedPages []PagePointer) {
	nodePointer := pool.head
	node := pool.storage.GetPage(nodePointer)
	reusablePages := make([]PagePointer, 0)

	if pool.getNodeTotalPages(node) < removedPagesCount {
		panic(fmt.Sprintf("PagePool: not enough pages to remove %d pages", removedPagesCount))
	}

	for removedPagesCount > 0 && removedPagesCount >= pool.getNodeSize(node) {
		addedPages = append(addedPages, nodePointer)

		removedPagesCount -= pool.getNodeSize(node)
		nodePointer = pool.getPreviousNode(node)
		node = pool.storage.GetPage(nodePointer)
	}

	if removedPagesCount != 0 {
		addedPages = append(addedPages, nodePointer)

		reusablePages = pool.getNodePages(node, removedPagesCount, pool.getNodeSize(node))

		nodePointer = pool.getPreviousNode(node)
		node = pool.storage.GetPage(nodePointer)
	}

	for len(addedPages) > 0 {
		newNodePointer := NULL_PAGE

		if len(reusablePages) > 0 {
			newNodePointer = reusablePages[0]
			reusablePages = reusablePages[1:]
		} else {
			newNodePointer = pool.storage.allocateVirtualPage()
		}

		newNode := pool.storage.GetPage(newNodePointer)

		if len(addedPages)+len(reusablePages) <= pool.getNodeCapacity(newNode) {
			addedPages = append(addedPages, reusablePages...)
			reusablePages = nil
		}

		pool.constructNode(newNode, node, nodePointer, addedPages[:min(pool.getNodeCapacity(newNode), len(addedPages))])

		addedPages = addedPages[min(pool.getNodeCapacity(newNode), len(addedPages)):]
		nodePointer = newNodePointer
		node = newNode

		if len(addedPages) == 0 && len(reusablePages) > 0 {
			addedPages = append(addedPages, reusablePages[0])
			reusablePages = reusablePages[1:]
		}
	}

	pool.head = nodePointer
}

func (pool *PagePool) constructNode(node []byte, previousNode []byte, previousNodePointer PagePointer, storedPages []PagePointer) {
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

func (pool *PagePool) getNodeTotalPages(node []byte) int {
	return int(binary.LittleEndian.Uint64(node[2:10]))
}

func (pool *PagePool) getNodeSize(node []byte) int {
	return int(binary.LittleEndian.Uint16(node))
}

func (pool *PagePool) getNodeCapacity(node []byte) int {
	return (len(node) - HEADER_SIZE) / 8
}

func (pool *PagePool) getPreviousNode(node []byte) PagePointer {
	return PagePointer(binary.LittleEndian.Uint64(node[10:]))
}

func (pool *PagePool) getNodePage(node []byte, pageNumber int) PagePointer {
	return PagePointer(binary.LittleEndian.Uint64(node[HEADER_SIZE+(pageNumber*8):]))
}

func (pool *PagePool) getNodePages(node []byte, start int, end int) []PagePointer {
	pages := make([]PagePointer, 0, end-start)

	for pageNumber := start; pageNumber < end; pageNumber++ {
		pages = append(pages, pool.getNodePage(node, pageNumber))
	}

	return pages
}

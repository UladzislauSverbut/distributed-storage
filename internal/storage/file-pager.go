package storage

import (
	"encoding/binary"
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

	for pool.getNodePagesCount(node) < pageNumber {
		pageNumber -= pool.getNodePagesCount(node)
		node = pool.storage.GetPage((pool.getPreviousNode(node)))
	}

	return pool.getNodePage(node, pageNumber)
}

func (pool *PagePool) getPagesCont() int {
	node := pool.storage.GetPage(pool.head)

	return pool.getNodeTotalPagesCount(node)
}

func (pool *PagePool) updatePages(reusedPagesCount int, releasedPages []PagePointer) {
	nodePointer := pool.head
	node := pool.storage.GetPage(nodePointer)
	pagesCount := pool.getNodeMaxPagesCount(node)
	reusedPages := make([]PagePointer, 0)

	for pagesCount <= reusedPagesCount {
		reusedPagesCount -= pagesCount

		reusedPages = append(reusedPages, nodePointer)

		nodePointer = pool.getPreviousNode(node)
		node = pool.storage.GetPage(nodePointer)
		pagesCount = pool.getNodePagesCount(node)
	}

	if reusedPagesCount != 0 {
		for pageNumber := reusedPagesCount; pageNumber < pagesCount; pageNumber++ {
			reusedPages = append(reusedPages, pool.getNodePage(node, pageNumber))
		}

		nodePointer = pool.getPreviousNode(node)
	}

	for len(releasedPages) > 0 {
		pages := releasedPages[:min(len(releasedPages), pagesCount)]
		releasedPages = releasedPages[len(pages):]

		newNodePointer := NULL_PAGE

		if len(reusedPages) > 0 {
			newNodePointer = releasedPages[0]
			releasedPages = releasedPages[1:]
		} else {
			newNodePointer = pool.storage.allocateVirtualPage()
		}

		newNode := pool.storage.GetPage(newNodePointer)

		pool.setNodePrevious(newNode, nodePointer)
		pool.addNodePages(newNode, pages)

		nodePointer = newNodePointer
		node = newNode
	}

	for len(reusedPages) > 0 {
		newNodePointer := reusedPages[0]
		reusedPages = reusedPages[1:]
		newNode := pool.storage.GetPage(newNodePointer)

		pool.setNodePrevious(newNode, nodePointer)
		pool.addNodePages(newNode, reusedPages[:pagesCount])

		reusedPages = reusedPages[:pagesCount]

		nodePointer = newNodePointer
		node = newNode
	}
}

func (pool *PagePool) getNodeTotalPagesCount(node []byte) int {
	return int(binary.LittleEndian.Uint64(node[2:]))
}

func (pool *PagePool) getNodePage(node []byte, pageNumber int) PagePointer {
	pageAddress := 10 + 8*pageNumber

	return PagePointer(binary.LittleEndian.Uint64(node[pageAddress:]))
}

func (pool *PagePool) getNodePagesCount(node []byte) int {
	return int(binary.LittleEndian.Uint16(node))
}

func (pool *PagePool) getPreviousNode(node []byte) PagePointer {
	return binary.LittleEndian.Uint64(node[10:])
}

func (pool *PagePool) getNodeCapacity(node []byte) int {
	return pool.getNodeMaxPagesCount(node) - pool.getNodePagesCount(node)
}

func (pool *PagePool) getNodeMaxPagesCount(node []byte) int {
	return (len(node) - HEADER_SIZE) / 8
}

func (pool *PagePool) setNodePrevious(node []byte, previousNodePointer PagePointer) {
	totalPagesNumber := pool.getNodePagesCount(node) + pool.getNodePagesCount((pool.storage.GetPage(previousNodePointer)))

	binary.LittleEndian.PutUint64(node[2:], uint64(totalPagesNumber))
	binary.LittleEndian.PutUint64(node[10:], uint64(previousNodePointer))
}

func (pool *PagePool) addNodePages(node []byte, pages []PagePointer) {
	if pool.getNodeCapacity(node) < len(pages) {
		panic("PagePool is full")
	}
	totalPagesCount := pool.getNodeTotalPagesCount(node)
	pagesCount := len(pages)

	binary.LittleEndian.PutUint16(node, uint16(pagesCount))
	binary.LittleEndian.PutUint64(node[10:], uint64(totalPagesCount+pagesCount))
}

package storage

import (
	"encoding/binary"
	"fmt"
)

/*
	Page Pool Format

	| number of stored pages | pointer to next page pool |  pointers to stored pages   |
	|          2B            |             8B            | number of stored pages * 8B |
*/

type PagePool struct {
	pageSize int
	data     []byte
}

func NewPagePool(pagesize int, data []byte) *PagePool {
	return &PagePool{
		pageSize: pagesize,
		data:     data,
	}
}

func (pool *PagePool) getPage(pageNumber int) PagePointer {
	if pageNumber >= pool.getPagesCount() {
		panic(fmt.Sprintf("PagePool doesn't store page pointer at position %d", pageNumber))
	}

	pageAddress := 10 + 8*pageNumber

	return PagePointer(binary.LittleEndian.Uint64(pool.data[pageAddress:]))
}

func (pool *PagePool) getPagesCount() int {
	return int(binary.LittleEndian.Uint16(pool.data))
}

func (pool *PagePool) setPagesCount(pagesCount int) {
	binary.LittleEndian.PutUint16(pool.data, uint16(pagesCount))
}

func (pool *PagePool) getNextPagePool() PagePointer {
	return PagePointer(binary.LittleEndian.Uint64(pool.data[2:]))
}

func (pool *PagePool) setNextPagePool(pointer PagePointer) {
	binary.LittleEndian.PutUint64(pool.data[2:], pointer)
}

func (pool *PagePool) addAvailablePage(pointer PagePointer) {

	if pool.isFull() {
		panic("PagePool can`t add page pointer to full page pool")
	}

	numberOfAvailablePages := pool.getPagesCount()

	pool.setPagesCount(numberOfAvailablePages + 1)

	binary.LittleEndian.PutUint64(pool.data[10+8*(numberOfAvailablePages):], pointer)
}

func (pool *PagePool) isFull() bool {
	return pool.pageSize < 10+int(pool.getPagesCount()+1)*8
}

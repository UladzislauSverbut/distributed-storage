package storage

import (
	"encoding/binary"
)

/*
Pager Format

	| number of available pages | pointer to next page pager |  pointers to available pages   |
	|          2B               |          8B                | number of available pages * 8B |
*/

type Pager struct {
	pageSize int
	data     []byte
}

func NewPager(pagesize int, data []byte) *Pager {
	return &Pager{
		pageSize: pagesize,
		data:     data,
	}
}

func (pager *Pager) getNumberOfAvailablePages() uint16 {
	return binary.LittleEndian.Uint16(pager.data)
}

func (pager *Pager) setNumberOfAvailablePages(numberOfAvailablePages uint16) {
	binary.LittleEndian.PutUint16(pager.data, numberOfAvailablePages)
}

func (pager *Pager) getNext() PagePointer {
	return PagePointer(binary.LittleEndian.Uint64(pager.data[2:]))
}

func (pager *Pager) setNext(pointer PagePointer) {
	binary.LittleEndian.PutUint64(pager.data[2:], pointer)
}

func (pager *Pager) getAvailablePage() PagePointer {
	numberOfAvailablePages := pager.getNumberOfAvailablePages()

	if numberOfAvailablePages == 0 {
		panic("Cant get available page in empty pool")
	}

	pager.setNumberOfAvailablePages(numberOfAvailablePages - 1)

	return PagePointer(binary.LittleEndian.Uint64(pager.data[10+8*(numberOfAvailablePages-1):]))
}

func (pager *Pager) addAvailablePage(pointer PagePointer) {

	if pager.isFull() {
		panic("Cant add page pointer to full page pager")
	}

	numberOfAvailablePages := pager.getNumberOfAvailablePages()

	pager.setNumberOfAvailablePages(numberOfAvailablePages + 1)

	binary.LittleEndian.PutUint64(pager.data[10+8*(numberOfAvailablePages):], pointer)
}

func (pager *Pager) isFull() bool {
	return pager.pageSize < 10+int(pager.getNumberOfAvailablePages()+1)*8
}

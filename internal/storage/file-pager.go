package storage

import (
	"encoding/binary"
)

/*
store Format

	| number of available pages | pointer to previous set of pages |  pointers to available pages   |
	|          2B               |          8B                      | number of available pages * 8B |
*/

type PageStore struct {
	pageSize int
	data     []byte
}

func NewPageStore(pagesize int, data []byte) *PageStore {
	return &PageStore{
		pageSize: pagesize,
		data:     data,
	}
}

func (store *PageStore) getNumberOfAvailablePages() uint16 {
	return binary.LittleEndian.Uint16(store.data)
}

func (store *PageStore) setNumberOfAvailablePages(numberOfAvailablePages uint16) {
	binary.LittleEndian.PutUint16(store.data, numberOfAvailablePages)
}

func (store *PageStore) getPrevious() PagePointer {
	return PagePointer(binary.LittleEndian.Uint64(store.data[2:]))
}

func (store *PageStore) setPrevious(pointer PagePointer) {
	binary.LittleEndian.PutUint64(store.data[2:], pointer)
}

func (store *PageStore) getAvailablePage() PagePointer {
	numberOfAvailablePages := store.getNumberOfAvailablePages()

	if numberOfAvailablePages == 0 {
		panic("Cant get available page in empty pool")
	}

	store.setNumberOfAvailablePages(numberOfAvailablePages - 1)

	return PagePointer(binary.LittleEndian.Uint64(store.data[10+8*(numberOfAvailablePages-1):]))
}

func (store *PageStore) addAvailablePage(pointer PagePointer) {

	if store.isFull() {
		panic("Cant add page pointer to full page store")
	}

	numberOfAvailablePages := store.getNumberOfAvailablePages()

	store.setNumberOfAvailablePages(numberOfAvailablePages + 1)

	binary.LittleEndian.PutUint64(store.data[10+8*(numberOfAvailablePages):], pointer)
}

func (store *PageStore) isFull() bool {
	return store.pageSize < 10+int(store.getNumberOfAvailablePages()+1)*8
}

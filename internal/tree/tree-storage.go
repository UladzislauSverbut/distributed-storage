package tree

import (
	"distributed-storage/internal/pager"
	"encoding/binary"
	"errors"
)

type TreeStorage struct {
	pageManager *pager.PageManager
}

func NewStorage(pageManager *pager.PageManager, pageSize int) *TreeStorage {
	if pageManager.PagesCount() > 1 {
		// validate root node pointer
		rootPointer := binary.LittleEndian.Uint64(pageManager.Header())

		if rootPointer > uint64(pageManager.PagesCount()) {
			panic(errors.New("TreeStorage can`t read data from pager because content is corrupted"))
		}
	}

	return &TreeStorage{pageManager: pageManager}
}

func (storage *TreeStorage) Get(pointer NodePointer) *Node {
	return &Node{data: storage.pageManager.Page(pointer)}
}

func (storage *TreeStorage) Create(node *Node) NodePointer {
	pointer := storage.pageManager.CreatePage(node.data)

	return NodePointer(pointer)
}

func (storage *TreeStorage) Delete(pointer NodePointer) {
	storage.pageManager.FreePage(pointer)
}

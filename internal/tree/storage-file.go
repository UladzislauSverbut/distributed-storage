package tree

import (
	"distributed-storage/internal/fs"
	"encoding/binary"
	"fmt"
	"os"
)

type StorageFile struct {
	pageManager *fs.PageManager
}

func NewStorageFile(filePath string, pageSize int) *StorageFile {
	var file *os.File
	var pageManager *fs.PageManager
	var err error

	defer func() {
		if err != nil && file != nil {
			file.Close()
		}
	}()

	if file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		panic(fmt.Errorf("BTreeFileStorage can`t open data file: %w", err))
	}

	if pageManager, err = fs.NewPageManager(file, pageSize); err != nil {
		panic(fmt.Errorf("BTreeFileStorage can`t read data file: %w", err))
	}

	if pageManager.GetPagesCount() > 1 {
		// validate root node pointer
		rootPointer := binary.LittleEndian.Uint64(pageManager.GetMetaInfo())

		if rootPointer > uint64(pageManager.GetPagesCount()) {
			panic(fmt.Errorf("BTreeFileStorage can`t read data file because content is corrupted: %w", err))
		}
	}

	return &StorageFile{pageManager: pageManager}
}

func (storage *StorageFile) GetRoot() BTreeRootPointer {
	if storage.pageManager.GetPagesCount() > 1 {
		return binary.LittleEndian.Uint64(storage.pageManager.GetMetaInfo())
	}

	return NULL_NODE
}

func (storage *StorageFile) SaveRoot(pointer BTreeRootPointer) error {
	if err := storage.pageManager.SaveChanges(); err != nil {
		return err
	}

	// update root pointer;
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, pointer)

	return storage.pageManager.SaveMetaInfo(buffer)
}

func (storage *StorageFile) Get(pointer BNodePointer) *BNode {
	return &BNode{data: storage.pageManager.GetPage(pointer)}
}

func (storage *StorageFile) Create(node *BNode) BNodePointer {
	pointer := storage.pageManager.CreatePage(node.data)

	return BNodePointer(pointer)
}

func (storage *StorageFile) Delete(pointer BNodePointer) {
	storage.pageManager.DeletePage(pointer)
}

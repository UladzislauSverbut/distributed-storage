package tree

import (
	"distributed-storage/internal/storage"
	"encoding/binary"
	"fmt"
	"os"
)

type BTreeStorage interface {
	GetRoot() BTreeRootPointer       // get root node pointer
	SaveRoot(BTreeRootPointer) error // save root node pointer

	Get(BNodePointer) *BNode    // dereference a pointer
	Create(*BNode) BNodePointer // allocate a new node
	Delete(BNodePointer)        // deallocate a node
}

type BTreeFileStorage struct {
	fs *storage.FileStorage
}

func NewBTreeFileStorage(filePath string, pageSize int) *BTreeFileStorage {
	var file *os.File
	var fs *storage.FileStorage
	var err error

	defer func() {
		if err != nil && file != nil {
			file.Close()
		}
	}()

	if file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		panic(fmt.Errorf("BTreeFileStorage can`t open data file: %w", err))
	}

	if fs, err = storage.NewFileStorage(file, pageSize); err != nil {
		panic(fmt.Errorf("BTreeFileStorage can`t read data file: %w", err))
	}

	if fs.GetPagesCount() > 1 {
		// validate root node pointer
		rootPointer := binary.LittleEndian.Uint64(fs.GetMetaInfo())

		if rootPointer > uint64(fs.GetPagesCount()) {
			panic(fmt.Errorf("BTreeFileStorage can`t read data file because content is corrupted: %w", err))
		}
	}

	return &BTreeFileStorage{fs: fs}
}

func (treeStorage *BTreeFileStorage) GetRoot() BTreeRootPointer {
	if treeStorage.fs.GetPagesCount() > 1 {
		return binary.LittleEndian.Uint64(treeStorage.fs.GetMetaInfo())
	}

	return NULL_NODE
}

func (treeStorage *BTreeFileStorage) SaveRoot(pointer BTreeRootPointer) error {
	if err := treeStorage.fs.SaveChanges(); err != nil {
		return err
	}

	// update root pointer;
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, pointer)

	return treeStorage.fs.SaveMetaInfo(buffer)
}

func (treeStorage *BTreeFileStorage) Get(pointer BNodePointer) *BNode {
	return &BNode{data: treeStorage.fs.GetPage(pointer)}
}

func (treeStorage *BTreeFileStorage) Create(node *BNode) BNodePointer {
	pointer := treeStorage.fs.CreatePage(node.data)

	return BNodePointer(pointer)
}

func (treeStorage *BTreeFileStorage) Delete(pointer BNodePointer) {
	treeStorage.fs.DeletePage(pointer)
}

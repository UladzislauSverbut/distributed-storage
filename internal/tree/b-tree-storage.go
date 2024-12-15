package tree

import (
	"distributed-storage/internal/storage"
	"encoding/binary"
	"errors"
	"os"
)

type BTreeFileStorage struct {
	fs *storage.FileStorage
}

func NewBTreeFileStorage(filePath string, pageSize int) (*BTreeFileStorage, error) {
	var file *os.File
	var fs *storage.FileStorage
	var err error

	defer func() {
		if err != nil && file != nil {
			file.Close()
		}
	}()

	if file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, err
	}

	if fs, err = storage.NewFileStorage(file, pageSize); err != nil {
		return nil, err
	}

	if fs.GetNumberOfPages() > 1 {
		// validate root node pointer
		rootPointer := binary.LittleEndian.Uint64(fs.GetMetaInfo())

		if rootPointer > uint64(fs.GetNumberOfPages()) {
			return nil, errors.New("FileSystem storage file contains invalid pointer to root node")
		}
	}

	return &BTreeFileStorage{fs: fs}, nil
}

func (treeStorage *BTreeFileStorage) Root() BNodePointer {
	if treeStorage.fs.GetNumberOfPages() > 1 {
		return binary.LittleEndian.Uint64(treeStorage.fs.GetMetaInfo())
	}

	return NULL_NODE
}

func (treeStorage *BTreeFileStorage) Get(pointer BNodePointer) *BNode {
	return &BNode{data: treeStorage.fs.GetPage(pointer)}
}

func (treeStorage *BTreeFileStorage) Create(node *BNode) BNodePointer {
	pointer := treeStorage.fs.CreatePage()
	data := treeStorage.fs.GetPage(pointer)

	copy(data, node.data)

	return BNodePointer(pointer)
}

func (treeStorage *BTreeFileStorage) Delete(pointer BNodePointer) {
	treeStorage.fs.DeletePage(pointer)
}

func (treeStorage *BTreeFileStorage) Save(tree *BTree) error {
	if err := treeStorage.fs.SavePages(); err != nil {
		return err
	}

	// update root pointer;
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, tree.root)

	return treeStorage.fs.SaveMetaInfo(buffer)
}

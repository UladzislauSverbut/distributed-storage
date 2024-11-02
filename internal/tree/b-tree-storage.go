package tree

import (
	"bytes"
	"distributed-storage/internal/storage"
	"encoding/binary"
	"errors"
	"os"
)

const STORAGE_FILE_PATH = "/var/lib/kv/data"
const STORAGE_SIGNATURE = "B_TREE_FILE_SIGN"

type BTreeFileStorage struct {
	fs *storage.FileStorage
}

// we book one file system storage page for storing pointer to the root node and other helpful information.
// we call this page as "master"
// master page structure:
// | signature | pointer to the root node | number of stored pages |
// |    16B    |              8B          |            8B          |

func NewBTreeFileStorage(pageSize int) (*BTreeFileStorage, error) {
	var file *os.File
	var fs *storage.FileStorage
	var err error

	defer func() {
		if err != nil && file != nil {
			file.Close()
		}
	}()

	if file, err = os.OpenFile(STORAGE_FILE_PATH, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, err
	}

	if fs, err = storage.NewFileStorage(file, pageSize); err != nil {
		return nil, err
	}

	if fs.GetFileSize() == 0 {
		// book first page for master page
		fs.CreatePage()
	} else {
		// verify content of master page
		if err = fs.SetNumberOfPages(1); err != nil {
			return nil, err
		}

		masterPage := fs.GetPage(storage.PagePointer(0))
		fileSignature := masterPage[0:16]
		rootPointer := binary.LittleEndian.Uint64(masterPage[16:])
		numberOfStoredPages := binary.LittleEndian.Uint64((masterPage[24:]))

		if !bytes.Equal([]byte(STORAGE_SIGNATURE), fileSignature) {
			return nil, errors.New("FileSystem storage file is corrupted")
		}

		if rootPointer > numberOfStoredPages {
			return nil, errors.New("FileSystem storage file contains invalid pointer to root node")
		}

		if err = fs.SetNumberOfPages(int(numberOfStoredPages)); err != nil {
			return nil, err
		}
	}

	return &BTreeFileStorage{fs: fs}, nil
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

	// updating master page;
	masterPage := treeStorage.fs.GetPage(storage.PagePointer(0))

	copy(masterPage[0:16], []byte(STORAGE_SIGNATURE))

	binary.LittleEndian.PutUint64(masterPage[16:24], uint64(treeStorage.fs.GetNumberOfPages()))
	binary.LittleEndian.PutUint64(masterPage[24:], uint64(tree.root))

	return treeStorage.fs.SavePage(storage.PagePointer(0))
}

package tree

import (
	"bytes"
	"distributed-storage/internal/storage"
	"encoding/binary"
	"errors"
	"os"
)

const STORAGE_FILE_PATH = "/var/lib/kv/data"
const STORAGE_SIGNATURE = "KV_SIGNATURE"

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
		if err = fs.SavePage(fs.CreatePage()); err != nil {
			return nil, err
		}
	} else {
		// verify content of master page
		masterPage := fs.GetPage(storage.PagePointer(0))
		fileSignature := masterPage[0:16]
		fileRootPointer := binary.LittleEndian.Uint64(masterPage[16:])
		fileNumberOfStoredPages := binary.LittleEndian.Uint64(masterPage[24:])

		if !bytes.Equal([]byte(STORAGE_SIGNATURE), fileSignature) {
			return nil, errors.New("FileSystem storage file is corrupted")
		}

		if fileNumberOfStoredPages < 1 || fileNumberOfStoredPages > uint64(fs.GetFileSize()/pageSize) {
			return nil, errors.New("FileSystem storage file contains invalid number of stored pages")
		}

		if fileRootPointer > fileNumberOfStoredPages {
			return nil, errors.New("FileSystem storage file contains invalid pointer to root node")
		}
	}

	return &BTreeFileStorage{fs: fs}, nil
}

func (storage *BTreeFileStorage) Get(pointer BNodePointer) *BNode {
	return &BNode{data: storage.fs.GetPage(pointer)}
}

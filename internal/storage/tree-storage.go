package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
)

const STORAGE_FILE_PATH = "/var/lib/kv/data"
const STORAGE_SIGNATURE = "KV_SIGNATURE"

type TreeStorage struct {
	fs *fileSystemStorage
}

// we book one file system storage page for storing pointer to the root node and other helpful information.
// we call this page as "master"
// master page structure:
// | signature | pointer to the root node | number of stored pages |
// |    16B    |              8B          |            8B          |

func NewTreeStorage(pageSize int) (*TreeStorage, error) {
	_, err := os.Stat(STORAGE_FILE_PATH)
	firstlyInitialized := false

	if err != nil {

		file, err := os.Create(STORAGE_FILE_PATH)

		if err != nil {
			return nil, err
		}

		file.Truncate(int64(pageSize) << 10)
		firstlyInitialized = true
	}

	fs, err := newFileSystemStorage(STORAGE_FILE_PATH, pageSize)

	if err != nil {
		return nil, err
	}

	if firstlyInitialized {
		// book first page for master page
		fs.storedPagesNumber = 1
	} else {
		// verify content of master page
		fileContent := fs.virtualMemory[0]
		fileSignature := fileContent[0:16]
		fileRootPointer := binary.LittleEndian.Uint64(fileContent[16:])
		fileNumberOfStoredPages := binary.LittleEndian.Uint64(fileContent[24:])

		if !bytes.Equal([]byte(STORAGE_SIGNATURE), fileSignature) {
			return nil, errors.New("FileSystem storage file is corrupted")
		}

		if fileNumberOfStoredPages < 1 || fileNumberOfStoredPages > uint64(fs.file.size/pageSize) {
			return nil, errors.New("FileSystem storage file contains invalid number of stored pages")
		}

		if fileRootPointer > fileNumberOfStoredPages {
			return nil, errors.New("FileSystem storage file contains invalid pointer to root node")
		}
	}

	return &TreeStorage{fs: fs}, nil
}

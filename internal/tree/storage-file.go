package tree

import (
	"distributed-storage/internal/fs"
	"encoding/binary"
	"fmt"
	"os"
)

type StorageFile struct {
	pageManager *fs.PageManager
	snapshots   map[SnapshotID]fs.PageManagerState
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

	return &StorageFile{pageManager: pageManager, snapshots: map[SnapshotID]fs.PageManagerState{}}
}

func (storage *StorageFile) GetRoot() BTreeRootPointer {
	if storage.pageManager.GetPagesCount() > 1 {
		return binary.LittleEndian.Uint64(storage.pageManager.GetMetaInfo())
	}

	return NULL_NODE
}

func (storage *StorageFile) SaveRoot(pointer BTreeRootPointer) error {
	// update root pointer;
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, pointer)

	return storage.pageManager.WriteMetaInfo(buffer)
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

func (storage *StorageFile) Flush() error {
	return storage.pageManager.WritePages()
}

func (storage *StorageFile) Snapshot() SnapshotID {
	snapshotID := SnapshotID(len(storage.snapshots))
	snapshot := storage.pageManager.GetState()

	storage.snapshots[snapshotID] = snapshot

	return snapshotID
}

func (storage *StorageFile) Restore(id SnapshotID) {
	snapshot, exist := storage.snapshots[id]

	if !exist {
		panic(fmt.Sprintf("StorageFile: snapshot with ID %d doesn`t exist", id))
	}

	storage.pageManager.ApplyState(snapshot)
}

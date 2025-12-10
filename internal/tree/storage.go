package tree

import (
	"distributed-storage/internal/backend"
	"distributed-storage/internal/pager"
	"encoding/binary"
	"fmt"
)

type SnapshotID int

type Storage struct {
	pageManager *pager.PageManager
	snapshots   map[SnapshotID]pager.PageManagerState
}

func NewStorage(backend backend.Backend, pageSize int) *Storage {
	var pageManager *pager.PageManager
	var err error

	if pageManager, err = pager.NewPageManager(backend, pageSize); err != nil {
		panic(fmt.Errorf("Storage can`t read data file: %w", err))
	}

	if pageManager.GetPagesCount() > 1 {
		// validate root node pointer
		rootPointer := binary.LittleEndian.Uint64(pageManager.GetMetaInfo())

		if rootPointer > uint64(pageManager.GetPagesCount()) {
			panic(fmt.Errorf("Storage can`t read data file because content is corrupted: %w", err))
		}
	}

	return &Storage{pageManager: pageManager, snapshots: map[SnapshotID]pager.PageManagerState{}}
}

func (storage *Storage) GetRoot() BTreeRootPointer {
	if storage.pageManager.GetPagesCount() > 1 {
		return binary.LittleEndian.Uint64(storage.pageManager.GetMetaInfo())
	}

	return NULL_NODE
}

func (storage *Storage) SaveRoot(pointer BTreeRootPointer) error {
	// update root pointer;
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, pointer)

	return storage.pageManager.WriteMetaInfo(buffer)
}

func (storage *Storage) Get(pointer BNodePointer) *BNode {
	return &BNode{data: storage.pageManager.GetPage(pointer)}
}

func (storage *Storage) Create(node *BNode) BNodePointer {
	pointer := storage.pageManager.CreatePage(node.data)

	return BNodePointer(pointer)
}

func (storage *Storage) Delete(pointer BNodePointer) {
	storage.pageManager.DeletePage(pointer)
}

func (storage *Storage) Flush() error {
	return storage.pageManager.WritePages()
}

func (storage *Storage) Snapshot() SnapshotID {
	snapshotID := SnapshotID(len(storage.snapshots))
	snapshot := storage.pageManager.GetState()

	storage.snapshots[snapshotID] = snapshot

	return snapshotID
}

func (storage *Storage) Restore(id SnapshotID) {
	snapshot, exist := storage.snapshots[id]

	if !exist {
		panic(fmt.Sprintf("Storage: snapshot with ID %d doesn`t exist", id))
	}

	storage.pageManager.ApplyState(snapshot)
}

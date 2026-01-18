package kv

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/tree"
	"encoding/binary"
	"fmt"
)

type SnapshotID int

type KeyValueStorage struct {
	pageManager *pager.PageManager
	snapshots   map[SnapshotID]pager.PageManagerState
}

func NewStorage(pageManager *pager.PageManager) *KeyValueStorage {
	return &KeyValueStorage{
		pageManager: pageManager,
		snapshots:   map[SnapshotID]pager.PageManagerState{},
	}
}

func (storage *KeyValueStorage) GetRoot() tree.TreeRootPointer {
	if storage.pageManager.PagesCount() > 1 {
		return binary.LittleEndian.Uint64(storage.pageManager.Header())
	}

	return tree.NULL_NODE
}

func (storage *KeyValueStorage) SaveRoot(pointer tree.TreeRootPointer) error {
	// update root pointer;
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, pointer)

	return storage.pageManager.SaveHeader(buffer)
}

func (storage *KeyValueStorage) Save() error {
	return storage.pageManager.SavePages()
}

func (storage *KeyValueStorage) Snapshot() SnapshotID {
	snapshotID := SnapshotID(len(storage.snapshots))
	pageManagerState := storage.pageManager.State()

	storage.snapshots[snapshotID] = pageManagerState

	return snapshotID
}

func (storage *KeyValueStorage) Restore(id SnapshotID) {
	pageManagerState, exist := storage.snapshots[id]

	if !exist {
		panic(fmt.Sprintf("KeyValueStorage: snapshot with ID %d doesn`t exist", id))
	}

	storage.pageManager.SetState(pageManagerState)
}

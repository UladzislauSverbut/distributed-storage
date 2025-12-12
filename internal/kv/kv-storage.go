package kv

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/tree"
	"fmt"
)

type SnapshotID int

type KeyValueStorage struct {
	tree.TreeStorage
	pageManager *pager.PageManager
	snapshots   map[SnapshotID]pager.PageManagerState
}

func NewStorage(pageManager *pager.PageManager, pageSize int) *KeyValueStorage {
	return &KeyValueStorage{
		TreeStorage: *tree.NewStorage(pageManager, pageSize),
		snapshots:   map[SnapshotID]pager.PageManagerState{},
	}
}

func (storage *KeyValueStorage) Snapshot() SnapshotID {
	snapshotID := SnapshotID(len(storage.snapshots))
	snapshot := storage.pageManager.GetState()

	storage.snapshots[snapshotID] = snapshot

	return snapshotID
}

func (storage *KeyValueStorage) Restore(id SnapshotID) {
	snapshot, exist := storage.snapshots[id]

	if !exist {
		panic(fmt.Sprintf("KeyValueStorage: snapshot with ID %d doesn`t exist", id))
	}

	storage.pageManager.ApplyState(snapshot)
}

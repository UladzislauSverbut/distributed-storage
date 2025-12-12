package kv

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	"distributed-storage/internal/tree"
	"encoding/binary"
	"fmt"
)

var config = tree.TreeConfig{
	PageSize:     16 * 1024, // 16KB
	MaxValueSize: 3 * 1024,  // 3KB
	MaxKeySize:   1 * 1024,  // 1KB
}

type KeyValue struct {
	tree      *tree.Tree
	parent    *KeyValue
	storage   *KeyValueStorage
	namespace []byte
}

func NewKeyValue(storage store.Storage) *KeyValue {
	pageManager, err := pager.NewPageManager(storage, config.PageSize)

	if err != nil {
		panic(fmt.Errorf("KeyValue: cant`t initialize internal storage %w", err))
	}

	keyValueStorage := NewStorage(pageManager, config.PageSize)

	return &KeyValue{
		tree:    tree.NewTree(keyValueStorage.GetRoot(), tree.NewStorage(pageManager, config.PageSize), config),
		storage: keyValueStorage,
	}
}

func WithPrefix(keyValue *KeyValue, prefix string) *KeyValue {
	namespace := []byte(prefix)
	value, err := keyValue.tree.Get(namespace)

	if err != nil {
		panic(fmt.Errorf("KeyValue: can`t find subtree %w", err))
	}

	subTreePointer := tree.NULL_NODE

	if value != nil {
		subTreePointer = binary.LittleEndian.Uint64(value)
	}

	return &KeyValue{
		namespace: namespace,
		tree:      tree.NewTree(subTreePointer, tree.NewStorage(keyValue.storage.pageManager, config.PageSize), config),
		parent:    keyValue,
		storage:   keyValue.storage,
	}
}

func (kv *KeyValue) Get(request *GetRequest) (*GetResponse, error) {
	value, err := kv.tree.Get(request.Key)

	return &GetResponse{value}, err
}

func (kv *KeyValue) Scan(request *ScanRequest) ScanResponse {
	treeScanner := tree.NewScanner(kv.tree)

	return treeScanner.Seek(request.Key, tree.GREATER_OR_EQUAL_COMPARISON)
}

func (kv *KeyValue) Set(request *SetRequest) (*SetResponse, error) {
	oldValue, err := kv.tree.Set(request.Key, request.Value)

	if err != nil {
		return &SetResponse{}, err
	}

	if err = kv.updateParent(); err != nil {
		return &SetResponse{}, err
	}

	if oldValue != nil {
		return &SetResponse{Updated: true, OldValue: oldValue}, nil
	}

	return &SetResponse{Added: true}, nil
}

func (kv *KeyValue) Delete(request *DeleteRequest) (*DeleteResponse, error) {
	oldValue, err := kv.tree.Delete(request.Key)

	if err != nil {
		return &DeleteResponse{}, err
	}

	if err = kv.updateParent(); err != nil {
		return &DeleteResponse{}, err
	}

	return &DeleteResponse{OldValue: oldValue}, nil
}

func (kv *KeyValue) updateParent() error {
	if kv.parent == nil {
		return nil
	}

	subTreeBufferedPointer := make([]byte, 8)

	binary.LittleEndian.PutUint64(subTreeBufferedPointer, kv.tree.Root())

	if _, err := kv.parent.tree.Set(kv.namespace, subTreeBufferedPointer); err != nil {
		return err
	}

	return kv.parent.updateParent()
}

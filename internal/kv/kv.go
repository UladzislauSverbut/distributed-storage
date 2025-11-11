package kv

import (
	"distributed-storage/internal/tree"
	"encoding/binary"
	"fmt"
)

var config = tree.BTreeConfig{
	PageSize:     16 * 1024, // 16KB
	MaxValueSize: 3 * 1024,  // 3KB
	MaxKeySize:   1 * 1024,  // 1KB
}

type KeyValue struct {
	tree      *tree.BTree
	parent    *KeyValue
	namespace []byte
	storage   tree.Storage
}

func NewKeyValue(filePath string) *KeyValue {
	storage := tree.NewStorageFile(filePath, config.PageSize)

	return &KeyValue{
		tree:    tree.NewBTree(storage.GetRoot(), storage, config),
		storage: storage,
	}
}

func WithPrefix(keyValue *KeyValue, prefix string) *KeyValue {
	namespace := []byte(prefix)
	value, err := keyValue.tree.Get(namespace)

	if err != nil {
		panic(fmt.Errorf("Child KeyValue can`t be created: %w", err))
	}

	subTreePointer := tree.NULL_NODE

	if value != nil {
		subTreePointer = binary.LittleEndian.Uint64(value)
	}

	return &KeyValue{
		namespace: namespace,
		tree:      tree.NewBTree(subTreePointer, keyValue.storage, config),
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

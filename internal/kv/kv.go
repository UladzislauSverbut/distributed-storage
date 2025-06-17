package kv

import (
	"distributed-storage/internal/tree"
)

var config = tree.BTreeConfig{
	PageSize:     16 * 1024, // 16KB
	MaxValueSize: 3 * 1024,  // 3KB
	MaxKeySize:   1 * 1024,  // 1KB
}

type KeyValue struct {
	tree    *tree.BTree
	storage tree.BTreeStorage
}

func NewKeyValue(filePath string) *KeyValue {
	storage := tree.NewBTreeFileStorage(filePath, config.PageSize)

	return &KeyValue{
		tree:    tree.NewBTree(storage.GetRoot(), storage, config),
		storage: storage,
	}
}

func (kv *KeyValue) Get(request *GetRequest) (*GetResponse, error) {
	value, err := kv.tree.Get(request.Key)

	return &GetResponse{value}, err
}

func (kv *KeyValue) Set(request *SetRequest) (*SetResponse, error) {
	oldValue, err := kv.tree.Set(request.Key, request.Value)

	if err != nil {
		return &SetResponse{}, err
	}

	if err = kv.storage.SaveRoot(kv.tree.Root()); err != nil {
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
		return &DeleteResponse{}, nil
	}

	if err = kv.storage.SaveRoot(kv.tree.Root()); err != nil {
		return &DeleteResponse{}, nil
	}

	return &DeleteResponse{OldValue: oldValue}, nil
}

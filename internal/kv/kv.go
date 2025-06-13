package kv

import (
	"distributed-storage/internal/tree"
)

type KeyValue struct {
	tree    *tree.BTree
	storage *tree.BTreeFileStorage
}

func NewKeyValue(filePath string) (*KeyValue, error) {
	config := tree.BTreeConfig{
		PageSize:     16 * 1024, // 16KB
		MaxValueSize: 3 * 1024,  // 3KB
		MaxKeySize:   1 * 1024,  // 1KB
	}

	storage, err := tree.NewBTreeFileStorage(filePath, config.PageSize)

	if err != nil {
		return nil, err
	}

	return &KeyValue{
		tree:    tree.NewBTree(storage.Root(), storage, config),
		storage: storage,
	}, nil
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

	if err = kv.storage.Save(kv.tree); err != nil {
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

	if err = kv.storage.Save(kv.tree); err != nil {
		return &DeleteResponse{}, nil
	}

	return &DeleteResponse{OldValue: oldValue}, nil
}

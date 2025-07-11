package kv

import (
	"distributed-storage/internal/tree"
)

type KeyValue struct {
	tree *tree.BTree
}

func (kv *KeyValue) Get(request *GetRequest) (*GetResponse, error) {
	value, err := kv.tree.Get(request.Key)

	return &GetResponse{value}, err
}

func (kv *KeyValue) Scan(request *ScanRequest) ScanResponse {
	treeScanner := tree.NewBTreeScanner(kv.tree)

	return treeScanner.Seek(request.Key, tree.GREATER_OR_EQUAL_COMPARISON)
}

func (kv *KeyValue) Set(request *SetRequest) (*SetResponse, error) {
	oldValue, err := kv.tree.Set(request.Key, request.Value)

	if err != nil {
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

	return &DeleteResponse{OldValue: oldValue}, nil
}

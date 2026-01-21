package kv

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/tree"
)

var config = tree.TreeConfig{
	PageSize:     16 * 1024, // 16KB
	MaxValueSize: 3 * 1024,  // 3KB
	MaxKeySize:   1 * 1024,  // 1KB
}

type KeyValue struct {
	tree        *tree.Tree
	parent      *KeyValue
	pageManager *pager.PageManager
}

func NewKeyValue(root pager.PagePointer, pageManager *pager.PageManager) *KeyValue {

	return &KeyValue{
		tree:        tree.NewTree(root, pageManager, config),
		pageManager: pageManager,
	}
}

func (kv *KeyValue) Root() pager.PagePointer {
	return kv.tree.Root()
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

	return &DeleteResponse{OldValue: oldValue}, nil
}

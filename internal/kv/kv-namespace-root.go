package kv

import (
	"distributed-storage/internal/tree"
)

var config = tree.BTreeConfig{
	PageSize:     16 * 1024, // 16KB
	MaxValueSize: 3 * 1024,  // 3KB
	MaxKeySize:   1 * 1024,  // 1KB
}

type RootNamespace struct {
	KeyValue
	storage tree.BTreeStorage
}

func NewRootNamespace(filePath string) *RootNamespace {
	storage := tree.NewBTreeFileStorage(filePath, config.PageSize)

	return &RootNamespace{
		KeyValue: KeyValue{tree: tree.NewBTree(storage.GetRoot(), storage, config)},
		storage:  storage,
	}
}

func (namespace *RootNamespace) Set(request *SetRequest) (response *SetResponse, err error) {
	response, err = namespace.KeyValue.Set(request)

	if err != nil {
		return
	}

	if err = namespace.Save(); err != nil {
		return &SetResponse{}, err
	}

	return
}

func (namespace *RootNamespace) Delete(request *DeleteRequest) (response *DeleteResponse, err error) {
	response, err = namespace.KeyValue.Delete(request)

	if err != nil {
		return
	}

	if err = namespace.Save(); err != nil {
		return &DeleteResponse{}, err
	}

	return
}

func (namespace *RootNamespace) Save() error {
	return namespace.storage.SaveRoot(namespace.tree.Root())
}

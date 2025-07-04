package kv

import "distributed-storage/internal/tree"

var config = tree.BTreeConfig{
	PageSize:     16 * 1024, // 16KB
	MaxValueSize: 3 * 1024,  // 3KB
	MaxKeySize:   1 * 1024,  // 1KB
}

type KeyValue struct {
	BaseKeyValue
	storage tree.BTreeStorage
}

func NewKeyValue(filePath string) *KeyValue {
	storage := tree.NewBTreeFileStorage(filePath, config.PageSize)

	return &KeyValue{
		BaseKeyValue: BaseKeyValue{tree: tree.NewBTree(storage.GetRoot(), storage, config)},
		storage:      storage,
	}
}

func (kv *KeyValue) Set(request *SetRequest) (response *SetResponse, err error) {
	response, err = kv.BaseKeyValue.Set(request)

	if err != nil {
		return
	}

	if err = kv.Save(); err != nil {
		return &SetResponse{}, err
	}

	return
}

func (kv *KeyValue) Delete(request *DeleteRequest) (response *DeleteResponse, err error) {
	response, err = kv.BaseKeyValue.Delete(request)

	if err != nil {
		return
	}

	if err = kv.Save(); err != nil {
		return &DeleteResponse{}, err
	}

	return
}

func (kv *KeyValue) Save() error {
	return kv.storage.SaveRoot(kv.tree.Root())
}

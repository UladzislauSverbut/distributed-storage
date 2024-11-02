package kv

import "distributed-storage/internal/tree"

type DistributedKV struct {
	tree    *tree.BTree
	storage *tree.BTreeFileStorage
}

func New() (*DistributedKV, error) {
	config := tree.BTreeConfig{
		PageSize:     4096,
		MaxValueSize: 3000,
		MaxKeySize:   1000,
	}

	storage, err := tree.NewBTreeFileStorage(config.PageSize)

	if err != nil {
		return nil, err
	}

	return &DistributedKV{
		tree:    tree.New(storage, config),
		storage: storage,
	}, nil
}

func (kv *DistributedKV) Get(key []byte) ([]byte, error) {
	return kv.tree.Get(key)
}

func (kv *DistributedKV) Set(key []byte, value []byte) error {
	if err := kv.tree.Set(key, value); err != nil {
		return err
	}

	return kv.storage.Save(kv.tree)
}

func (kv *DistributedKV) Delete(key []byte) error {
	if err := kv.tree.Delete(key); err != nil {
		return err
	}

	return kv.storage.Save(kv.tree)
}

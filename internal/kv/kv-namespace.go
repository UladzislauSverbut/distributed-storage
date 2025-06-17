package kv

import (
	"distributed-storage/internal/tree"
	"encoding/binary"
	"fmt"
)

type KeyValueNamespace struct {
	tree    *tree.BTree
	subTree *tree.BTree
	storage tree.BTreeStorage
	prefix  []byte
}

func NewKeyValueNamespace(kv *KeyValue, namespace string) *KeyValueNamespace {
	prefix := []byte(namespace)
	value, err := kv.tree.Get(prefix)

	if err != nil {
		panic(fmt.Errorf("KeyValueNamespace can`t be created: %w", err))
	}

	subTreePointer := tree.NULL_NODE

	if value != nil {
		subTreePointer = binary.LittleEndian.Uint64(value)
	}

	return &KeyValueNamespace{
		tree:    kv.tree,
		subTree: tree.NewBTree(subTreePointer, kv.storage, config),
		storage: kv.storage,
		prefix:  prefix,
	}
}

func (kv *KeyValueNamespace) Get(request *GetRequest) (*GetResponse, error) {
	value, err := kv.subTree.Get(request.Key)

	return &GetResponse{value}, err
}

func (kv *KeyValueNamespace) Set(request *SetRequest) (*SetResponse, error) {
	oldValue, err := kv.subTree.Set(request.Key, request.Value)

	if err != nil {
		return &SetResponse{}, err
	}

	if err = kv.saveSubTree(kv.subTree.Root()); err != nil {
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

func (kv *KeyValueNamespace) Delete(request *DeleteRequest) (*DeleteResponse, error) {
	oldValue, err := kv.subTree.Delete(request.Key)

	if err != nil {
		return &DeleteResponse{}, nil
	}

	if err = kv.saveSubTree(kv.subTree.Root()); err != nil {
		return &DeleteResponse{}, err
	}

	if err = kv.storage.SaveRoot(kv.tree.Root()); err != nil {
		return &DeleteResponse{}, nil
	}

	return &DeleteResponse{OldValue: oldValue}, nil
}

func (kv *KeyValueNamespace) saveSubTree(root tree.BNodePointer) error {
	subTreeBufferedPointer := make([]byte, 8)

	binary.LittleEndian.PutUint64(subTreeBufferedPointer, root)

	if _, err := kv.tree.Set(kv.prefix, subTreeBufferedPointer); err != nil {
		return err
	}

	return nil
}

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

type Namespace struct {
	KeyValue
	storage tree.BTreeStorage
	parent  *Namespace
	prefix  []byte
}

func NewRootNamespace(filePath string) *Namespace {
	storage := tree.NewBTreeFileStorage(filePath, config.PageSize)

	return &Namespace{
		KeyValue: KeyValue{tree: tree.NewBTree(storage.GetRoot(), storage, config)},
		storage:  storage,
	}
}

func NewChildNamespace(parent *Namespace, namespace string) *Namespace {
	prefix := []byte(namespace)
	value, err := parent.tree.Get(prefix)

	if err != nil {
		panic(fmt.Errorf("ChildNamespace can`t be created: %w", err))
	}

	subTreePointer := tree.NULL_NODE

	if value != nil {
		subTreePointer = binary.LittleEndian.Uint64(value)
	}

	return &Namespace{
		KeyValue: KeyValue{tree: tree.NewBTree(subTreePointer, parent.storage, config)},
		storage:  parent.storage,
		parent:   parent,
		prefix:   prefix,
	}
}

func (namespace *Namespace) Set(request *SetRequest) (response *SetResponse, err error) {
	if response, err = namespace.KeyValue.Set(request); err != nil {
		return
	}

	if err = namespace.Save(); err != nil {
		return &SetResponse{}, err
	}

	return
}

func (namespace *Namespace) Delete(request *DeleteRequest) (response *DeleteResponse, err error) {
	if response, err = namespace.KeyValue.Delete(request); err != nil {
		return
	}

	if err = namespace.Save(); err != nil {
		return &DeleteResponse{}, err
	}

	return
}

func (namespace *Namespace) Save() error {
	if namespace.isRoot() {
		return namespace.saveToStorage()
	}

	if err := namespace.saveToParent(); err != nil {
		return err
	}

	return namespace.parent.Save()
}

func (namespace *Namespace) saveToStorage() error {
	return namespace.storage.SaveRoot(namespace.tree.Root())
}

func (namespace *Namespace) saveToParent() error {
	subTreeBufferedPointer := make([]byte, 8)

	binary.LittleEndian.PutUint64(subTreeBufferedPointer, namespace.tree.Root())

	if _, err := namespace.parent.tree.Set(namespace.prefix, subTreeBufferedPointer); err != nil {
		return err
	}

	return nil
}

func (namespace *Namespace) isRoot() bool {
	return namespace.parent == nil
}

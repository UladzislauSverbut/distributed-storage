package kv

import (
	"distributed-storage/internal/tree"
	"encoding/binary"
	"fmt"
)

type ChildNamespace struct {
	KeyValue
	root   *RootNamespace
	prefix []byte
}

func NewChildNamespace(root *RootNamespace, namespace string) *ChildNamespace {
	prefix := []byte(namespace)
	value, err := root.tree.Get(prefix)

	if err != nil {
		panic(fmt.Errorf("ChildNamespace can`t be created: %w", err))
	}

	subTreePointer := tree.NULL_NODE

	if value != nil {
		subTreePointer = binary.LittleEndian.Uint64(value)
	}

	return &ChildNamespace{
		KeyValue: KeyValue{tree: tree.NewBTree(subTreePointer, root.storage, config)},
		root:     root,
		prefix:   prefix,
	}
}

func (namespace *ChildNamespace) Set(request *SetRequest) (response *SetResponse, err error) {
	response, err = namespace.KeyValue.Set(request)

	if err != nil {
		return
	}

	if err = namespace.Save(); err != nil {
		return &SetResponse{}, err
	}

	if err = namespace.root.Save(); err != nil {
		return &SetResponse{}, err
	}

	return
}

func (namespace *ChildNamespace) Delete(request *DeleteRequest) (response *DeleteResponse, err error) {
	response, err = namespace.KeyValue.Delete(request)

	if err != nil {
		return
	}

	if err = namespace.Save(); err != nil {
		return &DeleteResponse{}, err
	}

	if err = namespace.root.Save(); err != nil {
		return &DeleteResponse{}, err
	}

	return
}

func (namespace *ChildNamespace) Save() error {
	subTreeBufferedPointer := make([]byte, 8)

	binary.LittleEndian.PutUint64(subTreeBufferedPointer, namespace.tree.Root())

	if _, err := namespace.tree.Set(namespace.prefix, subTreeBufferedPointer); err != nil {
		return err
	}

	return nil
}

package kv

import (
	"distributed-storage/internal/tree"
	"encoding/binary"
	"fmt"
)

type KeyValueNamespace struct {
	BaseKeyValue
	root   *KeyValue
	prefix []byte
}

func NewKeyValueNamespace(root *KeyValue, namespace string) *KeyValueNamespace {
	prefix := []byte(namespace)
	value, err := root.tree.Get(prefix)

	if err != nil {
		panic(fmt.Errorf("KeyValueNamespace can`t be created: %w", err))
	}

	subTreePointer := tree.NULL_NODE

	if value != nil {
		subTreePointer = binary.LittleEndian.Uint64(value)
	}

	return &KeyValueNamespace{
		BaseKeyValue: BaseKeyValue{tree: tree.NewBTree(subTreePointer, root.storage, config)},
		root:         root,
		prefix:       prefix,
	}
}

func (kv *KeyValueNamespace) Set(request *SetRequest) (response *SetResponse, err error) {
	response, err = kv.BaseKeyValue.Set(request)

	if err != nil {
		return
	}

	if err = kv.Save(); err != nil {
		return &SetResponse{}, err
	}

	if err = kv.root.Save(); err != nil {
		return &SetResponse{}, err
	}

	return
}

func (kv *KeyValueNamespace) Delete(request *DeleteRequest) (response *DeleteResponse, err error) {
	response, err = kv.BaseKeyValue.Delete(request)

	if err != nil {
		return
	}

	if err = kv.Save(); err != nil {
		return &DeleteResponse{}, err
	}

	if err = kv.root.Save(); err != nil {
		return &DeleteResponse{}, err
	}

	return
}

func (kv *KeyValueNamespace) Save() error {
	subTreeBufferedPointer := make([]byte, 8)

	binary.LittleEndian.PutUint64(subTreeBufferedPointer, kv.tree.Root())

	if _, err := kv.tree.Set(kv.prefix, subTreeBufferedPointer); err != nil {
		return err
	}

	return nil
}

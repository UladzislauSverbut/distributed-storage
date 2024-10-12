package storage

import (
	"encoding/binary"
	"fmt"
)

type BNodeIndex = uint16

/*
	Internal Node Format

	| type (Leaf of Internal) | number of stored keys | pointers to child nodes |
	|          2B             |          2B           |    numberOfKeys * 8B    |

*/

type InternalBNode struct {
	BNode
}

func (node *InternalBNode) getChildPointer(index BNodeIndex) (BNodePointer, error) {
	if index >= node.getStoredKeysNumber() {
		return 0, fmt.Errorf("BNode doesnt store child with index %d", index)
	}

	childPointerAddress := index + HEADER_SIZE

	return binary.LittleEndian.Uint64(node.data[childPointerAddress:]), nil
}

func (node *InternalBNode) setChildPointer(index BNodeIndex, pointer BNodePointer) error {
	if index >= node.getStoredKeysNumber() {
		return fmt.Errorf("BNode doesnt store child with index %d", index)
	}

	childPointerAddress := index + HEADER_SIZE

	binary.LittleEndian.PutUint64(node.data[childPointerAddress:], pointer)

	return nil
}

func (node *InternalBNode) getSizeInBytes() uint16 {
	return HEADER_SIZE + 8*node.getStoredKeysNumber() + 2*(node.getStoredKeysNumber())
}

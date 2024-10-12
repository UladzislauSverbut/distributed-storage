package storage

import (
	"encoding/binary"
	"fmt"
)

type KeyValueIndex = uint16

/*
	Leaf Node Format

	| type (Leaf of Internal) | number of stored keys | offsets of key-value pairs |                             key-value pairs                         |
	|          2B             |          2B           |      numberOfKeys * 2B     | {keyLength 2B} {valueLength 2B} {key keyLength} {value valueLength} |

*/

type LeafBNode struct {
	BNode
}

func (node *LeafBNode) getKeyValueOffset(index KeyValueIndex) (uint16, error) {
	if index >= node.getStoredKeysNumber() {
		return 0, fmt.Errorf("BNode doesnt store key-value with index %d", index)
	}

	if index == 0 {
		return 0, nil
	}

	offsetAddress := HEADER_SIZE + node.getStoredKeysNumber()*8 + (index-1)*2

	return binary.LittleEndian.Uint16(node.data[offsetAddress:]), nil
}

func (node *LeafBNode) setKeyValueOffset(index KeyValueIndex, keyValueOffset uint16) error {
	if index >= node.getStoredKeysNumber() {
		return fmt.Errorf("BNode doesnt store key-value with index %d", index)
	}

	if index == 0 {
		return fmt.Errorf("BNode doesnt store offset for first key-value because its always 0")
	}

	offsetAddress := HEADER_SIZE + node.getStoredKeysNumber()*8 + index*2

	binary.LittleEndian.PutUint16(node.data[offsetAddress:], keyValueOffset)

	return nil
}

func (node *LeafBNode) getKeyValueAddress(index KeyValueIndex) (uint16, error) {
	keyValueOffset, err := node.getKeyValueOffset(index)

	if err != nil {
		return 0, err
	}

	return HEADER_SIZE + (8+2)*node.getStoredKeysNumber() + keyValueOffset, nil
}

func (node *LeafBNode) getKey(index KeyValueIndex) ([]byte, error) {
	KeyValueAddress, err := node.getKeyValueAddress(index)

	if err != nil {
		return nil, err
	}

	keyLength := binary.LittleEndian.Uint16(node.data[KeyValueAddress:])

	return node.data[KeyValueAddress+2+2:][:keyLength], nil
}

func (node *LeafBNode) getValue(index KeyValueIndex) ([]byte, error) {
	keyValueAddress, err := node.getKeyValueAddress(index)

	if err != nil {
		return nil, err
	}

	keyLength := binary.LittleEndian.Uint16(node.data[keyValueAddress:])
	valueLength := binary.LittleEndian.Uint16(node.data[keyValueAddress+2:])

	return node.data[keyValueAddress+2+2+keyLength:][:valueLength], nil
}

func (node *LeafBNode) getSizeInBytes() uint16 {
	// we store offset to the end of last key-value pair as size of node
	offsetAddress := HEADER_SIZE + 8*node.getStoredKeysNumber() + 2*(node.getStoredKeysNumber()-1)

	offset := binary.LittleEndian.Uint16(node.data[offsetAddress:])

	return HEADER_SIZE + (8+2)*node.getStoredKeysNumber() + offset
}

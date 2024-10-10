package storage

import (
	"encoding/binary"
	"fmt"
)

const HEADER_SIZE = 4

const (
	BNODE_INTERNAL uint16 = iota
	BNODE_LEAF
)

type BNodePointer = uint64

/*
	Node Format

	| type (Leaf of Internal) | number of stored keys | pointers to child nodes (used by Internal) | offsets of key-value pairs (used by Leaf) |                             key-value pairs                         |
	|          2B             |          2B           |            numberOfKeys * 8B               |          numberOfKeys * 2B                | {keyLength 2B} {valueLength 2B} {key keyLength} {value valueLength} |

*/

type BNode struct {
	data []byte
}

func (node *BNode) getType() uint16 {
	return binary.LittleEndian.Uint16(node.data[0:2])
}

func (node *BNode) getNumberOfStoredKeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node *BNode) setHeader(nodeType uint16, numberOfKeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], nodeType)
	binary.LittleEndian.PutUint16(node.data[2:4], numberOfKeys)
}

func (node *BNode) getChildPointer(childIndex uint16) (BNodePointer, error) {
	if childIndex >= node.getNumberOfStoredKeys() {
		return 0, fmt.Errorf("BNode doesnt store child with index %d", childIndex)
	}

	childPosition := childIndex + HEADER_SIZE

	return binary.LittleEndian.Uint64(node.data[childPosition:]), nil
}

func (node *BNode) setChildPointer(childIndex uint16, pointer BNodePointer) error {
	if childIndex >= node.getNumberOfStoredKeys() {
		return fmt.Errorf("BNode doesnt store child with index %d", childIndex)
	}

	childPosition := childIndex + HEADER_SIZE

	binary.LittleEndian.PutUint64(node.data[childPosition:], pointer)

	return nil
}

func (node *BNode) getKeyValueOffset(keyValueIndex uint16) (uint16, error) {
	if keyValueIndex >= node.getNumberOfStoredKeys() {
		return 0, fmt.Errorf("BNode doesnt store key-value with index %d", keyValueIndex)
	}

	if keyValueIndex == 0 {
		return 0, nil
	}

	offsetPosition := HEADER_SIZE + node.getNumberOfStoredKeys()*8 + (keyValueIndex-1)*2

	return binary.LittleEndian.Uint16(node.data[offsetPosition:]), nil
}

func (node *BNode) setKeyValueOffset(keyValueIndex uint16, keyValueOffset uint16) error {
	if keyValueIndex >= node.getNumberOfStoredKeys() {
		return fmt.Errorf("BNode doesnt store key-value with index %d", keyValueIndex)
	}

	if keyValueIndex == 0 {
		return fmt.Errorf("BNode doesnt store offset for first key-value because its always 0")
	}

	offsetPosition := HEADER_SIZE + node.getNumberOfStoredKeys()*8 + keyValueIndex*2

	binary.LittleEndian.PutUint16(node.data[offsetPosition:], keyValueOffset)

	return nil
}

func (node *BNode) getKeyValuePosition(keyValueIndex uint16) (uint16, error) {
	keyValueOffset, err := node.getKeyValueOffset(keyValueIndex)

	if err != nil {
		return 0, err
	}

	return HEADER_SIZE + (8+2)*node.getNumberOfStoredKeys() + keyValueOffset, nil
}

func (node *BNode) getKey(keyValueIndex uint16) ([]byte, error) {
	keyValuePosition, err := node.getKeyValuePosition(keyValueIndex)

	if err != nil {
		return nil, err
	}

	keyLength := binary.LittleEndian.Uint16(node.data[keyValuePosition:])

	return node.data[keyValuePosition+2+2:][:keyLength], nil
}

func (node *BNode) getValue(keyValueIndex uint16) ([]byte, error) {
	keyValuePosition, err := node.getKeyValuePosition(keyValueIndex)

	if err != nil {
		return nil, err
	}

	keyLength := binary.LittleEndian.Uint16(node.data[keyValuePosition:])
	valueLength := binary.LittleEndian.Uint16(node.data[keyValuePosition+2:])

	return node.data[keyValuePosition+2+2+keyLength:][:valueLength], nil
}

func (node *BNode) getSizeInBytes() uint16 {
	// we store offset to the end of last key-value pair as size of node
	offsetPosition := HEADER_SIZE + node.getNumberOfStoredKeys()*8 + (node.getNumberOfStoredKeys()-1)*2

	offset := binary.LittleEndian.Uint16(node.data[offsetPosition:])

	return HEADER_SIZE + (8+2)*node.getNumberOfStoredKeys() + offset
}

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const HEADER_SIZE = 4

const (
	BNODE_INTERNAL BNodeType = iota
	BNODE_LEAF
)

type BNodeType = uint16
type BNodePointer = uint64
type BNodeKeyValueSequenceNumber = uint16
type BNodeChildSequenceNumber = uint16

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

func (node *BNode) getStoredKeysNumber() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node *BNode) setHeader(nodeType BNodeType, numberOfKeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], nodeType)
	binary.LittleEndian.PutUint16(node.data[2:4], numberOfKeys)
}

func (node *BNode) getChildPointer(childSequenceNumber BNodeChildSequenceNumber) (BNodePointer, error) {
	if childSequenceNumber >= node.getStoredKeysNumber() {
		return 0, fmt.Errorf("BNode doesnt store child with index %d", childSequenceNumber)
	}

	childPointerAddress := childSequenceNumber + HEADER_SIZE

	return binary.LittleEndian.Uint64(node.data[childPointerAddress:]), nil
}

func (node *BNode) setChildPointer(childSequenceNumber BNodeChildSequenceNumber, pointer BNodePointer) error {
	if childSequenceNumber >= node.getStoredKeysNumber() {
		return fmt.Errorf("BNode doesnt store child with index %d", childSequenceNumber)
	}

	childPointerAddress := childSequenceNumber + HEADER_SIZE

	binary.LittleEndian.PutUint64(node.data[childPointerAddress:], pointer)

	return nil
}

func (node *BNode) getKeyValueOffset(keyValueSequenceNumber BNodeKeyValueSequenceNumber) (uint16, error) {
	if keyValueSequenceNumber >= node.getStoredKeysNumber() {
		return 0, fmt.Errorf("BNode doesnt store key-value with index %d", keyValueSequenceNumber)
	}

	if keyValueSequenceNumber == 0 {
		return 0, nil
	}

	offsetAddress := HEADER_SIZE + node.getStoredKeysNumber()*8 + (keyValueSequenceNumber-1)*2

	return binary.LittleEndian.Uint16(node.data[offsetAddress:]), nil
}

func (node *BNode) setKeyValueOffset(keyValueSequenceNumber BNodeKeyValueSequenceNumber, keyValueOffset uint16) error {
	if keyValueSequenceNumber >= node.getStoredKeysNumber() {
		return fmt.Errorf("BNode doesnt store key-value with index %d", keyValueSequenceNumber)
	}

	if keyValueSequenceNumber == 0 {
		return fmt.Errorf("BNode doesnt store offset for first key-value because its always 0")
	}

	offsetAddress := HEADER_SIZE + node.getStoredKeysNumber()*8 + keyValueSequenceNumber*2

	binary.LittleEndian.PutUint16(node.data[offsetAddress:], keyValueOffset)

	return nil
}

func (node *BNode) getKeyValueAddress(keyValueSequenceNumber BNodeKeyValueSequenceNumber) (uint16, error) {
	keyValueOffset, err := node.getKeyValueOffset(keyValueSequenceNumber)

	if err != nil {
		return 0, err
	}

	return HEADER_SIZE + (8+2)*node.getStoredKeysNumber() + keyValueOffset, nil
}

func (node *BNode) calculateKeyValueSequenceNumber(key []byte) (BNodeKeyValueSequenceNumber, error) {
	storedKeysNumber := node.getStoredKeysNumber()
	// we find the sequence number of new key-value record by comparing keys with passed key
	// position of last key that is less or equal than passed key returns
	// by default sequence number is 0 because we visited this node from the internal parent that contains the same key
	// thus first stored key is always less or equal to passed
	sequenceNumber := BNodeKeyValueSequenceNumber(0)

	for keySequenceNumber := BNodeKeyValueSequenceNumber(1); keySequenceNumber < storedKeysNumber; keySequenceNumber++ {
		storedKey, err := node.getKey(keySequenceNumber)

		if err != nil {
			return 0, err
		}

		if bytes.Compare(key, storedKey) >= 0 {
			sequenceNumber = keySequenceNumber
		} else {
			break
		}
	}

	return sequenceNumber, nil
}

func (node *BNode) getKey(keyValueSequenceNumber BNodeKeyValueSequenceNumber) ([]byte, error) {
	KeyValueAddress, err := node.getKeyValueAddress(keyValueSequenceNumber)

	if err != nil {
		return nil, err
	}

	keyLength := binary.LittleEndian.Uint16(node.data[KeyValueAddress:])

	return node.data[KeyValueAddress+2+2:][:keyLength], nil
}

func (node *BNode) getValue(keyValueSequenceNumber BNodeKeyValueSequenceNumber) ([]byte, error) {
	keyValueAddress, err := node.getKeyValueAddress(keyValueSequenceNumber)

	if err != nil {
		return nil, err
	}

	keyLength := binary.LittleEndian.Uint16(node.data[keyValueAddress:])
	valueLength := binary.LittleEndian.Uint16(node.data[keyValueAddress+2:])

	return node.data[keyValueAddress+2+2+keyLength:][:valueLength], nil
}

func (node *BNode) getSizeInBytes() uint16 {
	// we store offset of the end of last key-value pair as size of node
	offsetAddress := HEADER_SIZE + node.getStoredKeysNumber()*8 + (node.getStoredKeysNumber()-1)*2

	offset := binary.LittleEndian.Uint16(node.data[offsetAddress:])

	return HEADER_SIZE + (8+2)*node.getStoredKeysNumber() + offset
}

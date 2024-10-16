package storage

import (
	"encoding/binary"
	"fmt"
)

const HEADER_SIZE = 4

const (
	BNODE_PARENT BNodeType = iota
	BNODE_LEAF
)

type BNodeType = uint16
type BNodePointer = uint64
type BNodeKeyPosition = uint16

/*
	Node Format

	| type (Leaf of Parent) | number of stored keys | pointers to child nodes (used by Parent) | offsets of key-value pairs (used by Leaf) |                             key-value pairs                         |
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

func (node *BNode) getChildPointer(position BNodeKeyPosition) (BNodePointer, error) {
	if position >= node.getStoredKeysNumber() {
		return 0, fmt.Errorf("BNode doesn't store child pointer at position %d", position)
	}

	childPointerAddress := 8*position + HEADER_SIZE

	return binary.LittleEndian.Uint64(node.data[childPointerAddress:]), nil
}

func (node *BNode) setChildPointer(position BNodeKeyPosition, pointer BNodePointer) error {
	if position >= node.getStoredKeysNumber() {
		return fmt.Errorf("BNode doesn't store child pointer at position %d", position)
	}

	childPointerAddress := 8*position + HEADER_SIZE

	binary.LittleEndian.PutUint64(node.data[childPointerAddress:], pointer)

	return nil
}

func (node *BNode) getKey(position BNodeKeyPosition) ([]byte, error) {
	if position >= node.getStoredKeysNumber() {
		return nil, fmt.Errorf("BNode doesn't store key at position %d", position)
	}

	offset := node.getKeyValueOffset(position)
	address := node.convertKeyValueOffsetToAddress(offset)
	keyLength := binary.LittleEndian.Uint16(node.data[address:])

	return node.data[address+2+2:][:keyLength], nil
}

func (node *BNode) getValue(position BNodeKeyPosition) ([]byte, error) {
	if position >= node.getStoredKeysNumber() {
		return nil, fmt.Errorf("BNode doesn't store value at position %d", position)
	}

	offset := node.getKeyValueOffset(position)
	address := node.convertKeyValueOffsetToAddress(offset)
	keyLength := binary.LittleEndian.Uint16(node.data[address:])
	valueLength := binary.LittleEndian.Uint16(node.data[address+2:])

	return node.data[address+2+2+keyLength:][:valueLength], nil
}

func (node *BNode) appendKeyValue(key []byte, value []byte) error {
	// we find first value nil or pointer 0 that means we dont store any values for such position

	position := node.getAvailableKeyPosition()

	if position == node.getStoredKeysNumber() {
		return fmt.Errorf("couldn't append key-value because node is full")
	}

	node.setChildPointer(position, 0)
	keyValueAddress := node.convertKeyValueOffsetToAddress(node.getKeyValueOffset(position))

	binary.BigEndian.PutUint16(node.data[keyValueAddress:], uint16(len(key)))
	binary.BigEndian.PutUint16(node.data[keyValueAddress+2:], uint16(len(value)))

	copy(node.data[keyValueAddress+4:], key)
	copy(node.data[keyValueAddress+4+uint16(len(key)):], value)

	node.setKeyValueOffset(position+1, keyValueAddress+4+uint16(len(key)+len(value)))

	return nil
}

func (node *BNode) appendPointer(key []byte, pointer BNodePointer) error {
	// we find first value nil or pointer 0 that means we dont store any values for such position

	position := node.getAvailableKeyPosition()

	if position == node.getStoredKeysNumber() {
		return fmt.Errorf("couldn't append key-value because node is full")
	}

	if err := node.appendKeyValue(key, nil); err != nil {
		return err
	}

	return node.setChildPointer(position, pointer)
}

func (node *BNode) GetSizeInBytes() uint16 {
	// we store offset of the end of last key-value pair as size of node

	offset := node.getKeyValueOffset(node.getStoredKeysNumber())

	return node.convertKeyValueOffsetToAddress(offset)
}

func (node *BNode) Copy(source *BNode, from BNodeKeyPosition, to BNodeKeyPosition, quantity uint16) error {

	if from+quantity > source.getStoredKeysNumber() {
		return fmt.Errorf("couldn't copy %d values from position %d because source node has only %d keys", quantity, from, source.getStoredKeysNumber())
	}

	if to+quantity > node.getStoredKeysNumber() {
		return fmt.Errorf("couldn't copy %d values from position %d because target node has only %d keys", quantity, from, node.getStoredKeysNumber())
	}

	sourceBeginOffset := source.getKeyValueOffset(from)
	sourceEndOffset := source.getKeyValueOffset(from + quantity)
	targetBeginOffset := node.getKeyValueOffset(to)

	for shift := BNodeKeyPosition(0); shift < quantity; shift++ {
		targetCursor := to + shift
		sourceCursor := from + shift

		copiedPointer, _ := source.getChildPointer(sourceCursor)
		node.setChildPointer(targetCursor, copiedPointer)

		sourceOffset := source.getKeyValueOffset(sourceCursor + 1)
		node.setKeyValueOffset(targetCursor+1, (sourceOffset-sourceBeginOffset)+targetBeginOffset)
	}

	copy(
		node.data[node.convertKeyValueOffsetToAddress(targetBeginOffset):],
		source.data[source.convertKeyValueOffsetToAddress(sourceBeginOffset):source.convertKeyValueOffsetToAddress(sourceEndOffset)])

	return nil
}

func (node *BNode) setKeyValueOffset(position BNodeKeyPosition, keyValueOffset uint16) error {
	if position >= node.getStoredKeysNumber() {
		return fmt.Errorf("BNode doesn't store key-value with index %d", position)
	}

	if position == 0 {
		return fmt.Errorf("BNode doesn't store offset for first key-value because its always 0")
	}

	address := HEADER_SIZE + node.getStoredKeysNumber()*8 + (position-1)*2

	binary.LittleEndian.PutUint16(node.data[address:], keyValueOffset)

	return nil
}

func (node *BNode) getKeyValueOffset(position BNodeKeyPosition) uint16 {
	var offset uint16

	if position == 0 {
		offset = 0
	} else {
		address := HEADER_SIZE + node.getStoredKeysNumber()*8 + (position-1)*2
		offset = binary.LittleEndian.Uint16(node.data[address:])
	}

	return offset
}

func (node *BNode) getAvailableKeyPosition() BNodeKeyPosition {
	for position := BNodeKeyPosition(0); position < node.getStoredKeysNumber(); position++ {
		value, _ := node.getValue(position)
		pointer, _ := node.getChildPointer(position)

		if value == nil || pointer == 0 {
			return position
		}
	}

	return node.getStoredKeysNumber()
}

func (node *BNode) convertKeyValueOffsetToAddress(keyValueOffset uint16) uint16 {
	return HEADER_SIZE + (8+2)*node.getStoredKeysNumber() + keyValueOffset
}

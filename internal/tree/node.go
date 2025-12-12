package tree

import (
	"encoding/binary"
	"fmt"
)

const HEADER_SIZE = 4 // size of node header in file bytes

const NULL_NODE = NodePointer(0)

const (
	NODE_PARENT NodeType = iota
	NODE_LEAF
)

type NodeType = uint16
type NodePointer = uint64
type NodeKeyPosition = uint16

/*
	Node Format

	| type (Leaf of Parent) | number of stored keys | pointers to child nodes (used by Parent)   | offsets of key-value pairs (used by Leaf) |                             key-value pairs                         |
	|          2B           |          2B           |              numberOfKeys * 8B             |            numberOfKeys * 2B              | {keyLength 2B} {valueLength 2B} {key keyLength} {value valueLength} |

*/

type Node struct {
	data []byte
}

func (node *Node) getType() NodeType {
	return binary.LittleEndian.Uint16(node.data[0:2])
}

func (node *Node) getStoredKeysNumber() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node *Node) setHeader(nodeType NodeType, numberOfKeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], nodeType)
	binary.LittleEndian.PutUint16(node.data[2:4], numberOfKeys)
}

func (node *Node) getChildPointer(position NodeKeyPosition) NodePointer {
	if position >= node.getStoredKeysNumber() {
		panic(fmt.Sprintf("Node: couldn't find child pointer at position %d", position))
	}

	childPointerAddress := 8*position + HEADER_SIZE

	return binary.LittleEndian.Uint64(node.data[childPointerAddress:])
}

func (node *Node) setChildPointer(position NodeKeyPosition, pointer NodePointer) {
	if position >= node.getStoredKeysNumber() {
		panic(fmt.Sprintf("Node: couldn't set child pointer at position %d", position))
	}

	childPointerAddress := 8*position + HEADER_SIZE

	binary.LittleEndian.PutUint64(node.data[childPointerAddress:], pointer)
}

func (node *Node) getKey(position NodeKeyPosition) []byte {
	if node.getStoredKeysNumber() == 0 {
		return nil
	}

	if position >= node.getStoredKeysNumber() {
		panic(fmt.Sprintf("Node: couldn't get key at position %d", position))
	}

	offset := node.getKeyValueOffset(position)
	address := node.convertKeyValueOffsetToAddress(offset)
	keyLength := binary.LittleEndian.Uint16(node.data[address:])

	return node.data[address+2+2:][:keyLength]
}

func (node *Node) getValue(position NodeKeyPosition) []byte {
	if position >= node.getStoredKeysNumber() {
		panic(fmt.Sprintf("Node: couldn't get value at position %d", position))
	}

	offset := node.getKeyValueOffset(position)
	address := node.convertKeyValueOffsetToAddress(offset)
	keyLength := binary.LittleEndian.Uint16(node.data[address:])
	valueLength := binary.LittleEndian.Uint16(node.data[address+2:])

	return node.data[address+2+2+keyLength:][:valueLength]
}

func (node *Node) appendKeyValue(key []byte, value []byte) {
	position := node.getAvailableKeyPosition()

	if position == node.getStoredKeysNumber() {
		panic("Node: couldn't append key-value because node is full")
	}

	node.setChildPointer(position, 0)
	keyValueOffset := node.getKeyValueOffset(position)
	keyValueAddress := node.convertKeyValueOffsetToAddress(keyValueOffset)

	binary.LittleEndian.PutUint16(node.data[keyValueAddress:], uint16(len(key)))
	binary.LittleEndian.PutUint16(node.data[keyValueAddress+2:], uint16(len(value)))

	copy(node.data[keyValueAddress+4:], key)
	copy(node.data[keyValueAddress+4+uint16(len(key)):], value)

	node.setKeyValueOffset(position+1, keyValueOffset+4+uint16(len(key)+len(value)))
}

func (node *Node) appendPointer(key []byte, pointer NodePointer) {
	position := node.getAvailableKeyPosition()

	if position == node.getStoredKeysNumber() {
		panic("Node: couldn't append key-value because node is full")
	}

	node.appendKeyValue(key, nil)
	node.setChildPointer(position, pointer)
}

func (node *Node) size() uint16 {
	// we store offset of the end of last key-value pair as size of node

	offset := node.getKeyValueOffset(node.getStoredKeysNumber())

	return node.convertKeyValueOffsetToAddress(offset)
}

func (node *Node) copy(source *Node, from NodeKeyPosition, to NodeKeyPosition, quantity uint16) {

	if from+quantity > source.getStoredKeysNumber() {
		panic(fmt.Sprintf("Node: couldn't copy %d values from position %d because source node has only %d keys", quantity, from, source.getStoredKeysNumber()))
	}

	if to+quantity > node.getStoredKeysNumber() {
		panic(fmt.Sprintf("Node couldn't copy %d values from position %d because target node has only %d keys", quantity, from, node.getStoredKeysNumber()))
	}

	sourceBeginOffset := source.getKeyValueOffset(from)
	sourceEndOffset := source.getKeyValueOffset(from + quantity)
	targetBeginOffset := node.getKeyValueOffset(to)

	for shift := NodeKeyPosition(0); shift < quantity; shift++ {
		targetCursor := to + shift
		sourceCursor := from + shift

		copiedPointer := source.getChildPointer(sourceCursor)
		node.setChildPointer(targetCursor, copiedPointer)

		sourceOffset := source.getKeyValueOffset(sourceCursor + 1)
		node.setKeyValueOffset(targetCursor+1, (sourceOffset-sourceBeginOffset)+targetBeginOffset)
	}

	copy(
		node.data[node.convertKeyValueOffsetToAddress(targetBeginOffset):],
		source.data[source.convertKeyValueOffsetToAddress(sourceBeginOffset):source.convertKeyValueOffsetToAddress(sourceEndOffset)])
}

func (node *Node) setKeyValueOffset(position NodeKeyPosition, keyValueOffset uint16) {
	if position > node.getStoredKeysNumber() {
		panic(fmt.Sprintf("Node: couldn't set key-value with index %d", position))
	}

	if position == 0 {
		panic("Node: couldn't store offset for first key-value because its always 0")
	}

	address := HEADER_SIZE + node.getStoredKeysNumber()*8 + (position-1)*2

	binary.LittleEndian.PutUint16(node.data[address:], keyValueOffset)
}

func (node *Node) getKeyValueOffset(position NodeKeyPosition) uint16 {
	var offset uint16

	if position == 0 {
		offset = 0
	} else {
		address := HEADER_SIZE + node.getStoredKeysNumber()*8 + (position-1)*2
		offset = binary.LittleEndian.Uint16(node.data[address:])
	}

	return offset
}

func (node *Node) getAvailableKeyPosition() NodeKeyPosition {
	for position := NodeKeyPosition(0); position < node.getStoredKeysNumber(); position++ {
		key := node.getKey(position)

		if len(key) == 0 {
			return position
		}
	}

	return node.getStoredKeysNumber()
}

func (node *Node) convertKeyValueOffsetToAddress(keyValueOffset uint16) uint16 {
	return HEADER_SIZE + (8+2)*node.getStoredKeysNumber() + keyValueOffset
}

package storage

import (
	"encoding/binary"
)

const HEADER_SIZE = 4

const (
	BNODE_INTERNAL BNodeType = iota
	BNODE_LEAF
)

type BNodeType = uint16
type BNodePointer = uint64

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

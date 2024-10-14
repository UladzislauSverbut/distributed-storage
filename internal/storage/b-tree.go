package storage

import "bytes"

const BTREE_PAGE_SIZE = 4096

type BTree struct {
}

func (tree *BTree) insertKeyValue(node *BNode, position BNodeKeyPosition, key []byte, value []byte) *BNode {
	newNode := &BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	newNode.SetHeader(BNODE_LEAF, node.GetStoredKeysNumber()+1)

	newNode.Copy(node, 0, 0, position)
	newNode.AppendKeyValue(key, value)
	newNode.Copy(node, position, position+1, node.GetStoredKeysNumber()-position)

	return newNode
}

func (tree *BTree) getLessOrEqualKeyPosition(node *BNode, key []byte) BNodeKeyPosition {
	storedKeysNumber := node.GetStoredKeysNumber()
	// we find the position of last key that is less or equal than passed key
	// by default sequence number is 0 because we visited this node from the parent that contains the same key
	// thus first stored key is always less or equal to passed
	position := BNodeKeyPosition(0)

	for keyPosition := BNodeKeyPosition(1); keyPosition < storedKeysNumber; keyPosition++ {
		storedKey, _ := node.GetKey(keyPosition)

		if bytes.Compare(key, storedKey) >= 0 {
			position = keyPosition
		} else {
			break
		}
	}

	return position
}

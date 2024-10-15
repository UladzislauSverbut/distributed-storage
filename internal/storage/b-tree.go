package storage

import (
	"bytes"
	"log"
)

const BTREE_PAGE_SIZE = 4096

type BTreeStorageAdapter interface {
	Get(BNodePointer) *BNode    // dereference a pointer
	Create(*BNode) BNodePointer // allocate a new page
	Delete(BNodePointer)        // deallocate a page
}

type BTree struct {
	storage BTreeStorageAdapter
	root    BNode
}

func (tree *BTree) setKeyValue(node *BNode, key []byte, value []byte) *BNode {
	keyPosition := tree.getLessOrEqualKeyPosition(node, key)

	switch node.GetType() {
	case BNODE_LEAF:
		{
			storedKeyAtSamePosition, _ := node.GetKey(keyPosition)

			if bytes.Equal(key, storedKeyAtSamePosition) {
				return tree.updateLeafKeyValue(node, keyPosition, key, value)
			} else {
				return tree.insertLeafKeyValue(node, keyPosition, key, value)
			}
		}
	case BNODE_PARENT:
		{
			return tree.insertParentKeyValue(node, keyPosition, key, value)
		}
	default:
		log.Printf("Unsupported node type %s", node.GetType())
		return node
	}

}

func (tree *BTree) updateLeafKeyValue(node *BNode, position BNodeKeyPosition, key []byte, value []byte) *BNode {
	newNode := &BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	newNode.SetHeader(BNODE_LEAF, node.GetStoredKeysNumber())

	newNode.Copy(node, 0, 0, position-1)
	newNode.AppendKeyValue(key, value)
	newNode.Copy(node, position+1, position+1, node.GetStoredKeysNumber()-position)

	return newNode
}

func (tree *BTree) insertLeafKeyValue(node *BNode, position BNodeKeyPosition, key []byte, value []byte) *BNode {
	newNode := &BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	newNode.SetHeader(BNODE_LEAF, node.GetStoredKeysNumber()+1)

	newNode.Copy(node, 0, 0, position)
	newNode.AppendKeyValue(key, value)
	newNode.Copy(node, position, position+1, node.GetStoredKeysNumber()-position)

	return newNode
}

func (tree *BTree) insertParentKeyValue(parent *BNode, position BNodeKeyPosition, key []byte, value []byte) *BNode {
	childPointer, _ := parent.GetChildPointer(position)
	child := tree.storage.Get(childPointer)

	tree.storage.Delete(childPointer)

	child = tree.insertLeafKeyValue(child, position, key, value)

	var newParentChildren []*BNode

	if child.GetSizeInBytes() > BTREE_PAGE_SIZE {
		newParentChildren = tree.splitNode(child)
	} else {
		newParentChildren = []*BNode{child}
	}

	return tree.replaceParentChild(parent, newParentChildren, position)
}

func (tree *BTree) replaceParentChild(parent *BNode, children []*BNode, childPosition BNodeKeyPosition) *BNode {
	newNode := &BNode{data: make([]byte, BTREE_PAGE_SIZE)}

	newNode.SetHeader(BNODE_PARENT, parent.GetStoredKeysNumber()-1+uint16(len(children)))
	newNode.Copy(parent, 0, 0, childPosition)

	for _, child := range children {
		firstChildStoredKey, _ := child.GetKey(BNodeKeyPosition(0))
		newNode.AppendPointer(firstChildStoredKey, tree.storage.Create(child))
	}

	newNode.Copy(parent, childPosition+1, childPosition+uint16(len(children)), newNode.GetStoredKeysNumber()-childPosition)

	return newNode
}

func (tree *BTree) splitNode(node *BNode) []*BNode {
	keysNumber := node.GetStoredKeysNumber()
	storedKeysForFirstNode := keysNumber / 2
	storedKeysForSecondNode := keysNumber - storedKeysForFirstNode

	firstNode := &BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	secondNode := &BNode{data: make([]byte, BTREE_PAGE_SIZE)}

	firstNode.SetHeader(node.GetType(), storedKeysForFirstNode)
	secondNode.SetHeader(node.GetType(), storedKeysForSecondNode)

	firstNode.Copy(node, 0, 0, storedKeysForFirstNode)
	secondNode.Copy(node, storedKeysForFirstNode, 0, storedKeysForSecondNode)

	if firstNode.GetSizeInBytes() > BTREE_PAGE_SIZE {
		splittedNodes := tree.splitNode(firstNode)

		thirdNode := secondNode
		firstNode := splittedNodes[0]
		secondNode = splittedNodes[1]

		firstNode.data = firstNode.data[0:BTREE_PAGE_SIZE]

		return []*BNode{firstNode, secondNode, thirdNode}
	} else {
		firstNode.data = firstNode.data[0:BTREE_PAGE_SIZE]

		return []*BNode{firstNode, secondNode}
	}
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

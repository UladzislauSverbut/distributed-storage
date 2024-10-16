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
		{
			log.Printf("Unsupported node type %s", node.GetType())
			return node
		}
	}

}

func (tree *BTree) deleteKeyValue(node *BNode, key []byte) *BNode {
	switch node.GetType() {
	case BNODE_LEAF:
		{
			return tree.deleteLeafKeyValue(node, key)
		}
	case BNODE_PARENT:
		{
			return tree.deleteParentKeyValue(node, key)
		}
	default:
		{
			log.Printf("Unsupported node type %s", node.GetType())
			return node
		}
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

func (tree *BTree) deleteLeafKeyValue(node *BNode, key []byte) *BNode {
	keyPosition := tree.getLessOrEqualKeyPosition(node, key)
	storedKey, _ := node.GetKey(keyPosition)

	if bytes.Compare(key, storedKey) != 0 {
		return node
	}

	newNode := &BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	newNode.SetHeader(BNODE_LEAF, node.GetStoredKeysNumber()-1)

	newNode.Copy(node, 0, 0, keyPosition)
	newNode.Copy(node, keyPosition+1, keyPosition, node.GetStoredKeysNumber()-(keyPosition+1))

	return newNode
}

func (tree *BTree) insertParentKeyValue(parent *BNode, position BNodeKeyPosition, key []byte, value []byte) *BNode {
	childPointer, _ := parent.GetChildPointer(position)
	updatedChild := tree.insertLeafKeyValue(tree.storage.Get(childPointer), position, key, value)

	tree.storage.Delete(childPointer)

	var newParentChildren []*BNode

	if updatedChild.GetSizeInBytes() > BTREE_PAGE_SIZE {
		newParentChildren = tree.splitNode(updatedChild)
	} else {
		newParentChildren = []*BNode{updatedChild}
	}

	return tree.replaceParentChildren(parent, newParentChildren, position, 1)
}

func (tree *BTree) deleteParentKeyValue(node *BNode, key []byte) *BNode {
	keyPosition := tree.getLessOrEqualKeyPosition(node, key)
	childPointer, _ := node.GetChildPointer(keyPosition)

	child := tree.storage.Get(childPointer)
	updatedChild := tree.deleteKeyValue(child, key)

	if child.GetStoredKeysNumber() == updatedChild.GetStoredKeysNumber() {
		return node
	}

	tree.storage.Delete(childPointer)

	return tree.mergeParentChildren(node, updatedChild, keyPosition)
}

func (tree *BTree) replaceParentChildren(parent *BNode, children []*BNode, position BNodeKeyPosition, quantity uint16) *BNode {
	newNode := &BNode{data: make([]byte, BTREE_PAGE_SIZE)}

	newNode.SetHeader(BNODE_PARENT, parent.GetStoredKeysNumber()-quantity+uint16(len(children)))
	newNode.Copy(parent, 0, 0, position)

	for _, child := range children {
		firstChildStoredKey, _ := child.GetKey(BNodeKeyPosition(0))
		newNode.AppendPointer(firstChildStoredKey, tree.storage.Create(child))
	}

	newNode.Copy(parent, position+quantity, position+uint16(len(children)), newNode.GetStoredKeysNumber()-position)

	return newNode
}

func (tree *BTree) mergeParentChildren(node *BNode, newChild *BNode, position BNodeKeyPosition) *BNode {
	if newChild.GetSizeInBytes() < BTREE_PAGE_SIZE/4 {
		if position > 0 {
			leftChildPointer, _ := node.GetChildPointer(position - 1)
			leftChild := tree.storage.Get(leftChildPointer)

			if leftChild.GetSizeInBytes()+newChild.GetSizeInBytes() < BTREE_PAGE_SIZE-HEADER_SIZE {
				tree.storage.Delete(leftChildPointer)

				return tree.replaceParentChildren(node, []*BNode{tree.mergeNodes(leftChild, newChild)}, position-1, 2)
			}
		}

		if position < node.GetStoredKeysNumber()-1 {
			rightChildPointer, _ := node.GetChildPointer(position - 1)
			rightChild := tree.storage.Get(rightChildPointer)

			if rightChild.GetSizeInBytes()+newChild.GetSizeInBytes() < BTREE_PAGE_SIZE-HEADER_SIZE {
				tree.storage.Delete(rightChildPointer)

				return tree.replaceParentChildren(node, []*BNode{tree.mergeNodes(newChild, rightChild)}, position, 2)
			}
		}
	}

	return tree.replaceParentChildren(node, []*BNode{newChild}, position, 1)
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

func (tree *BTree) mergeNodes(first *BNode, second *BNode) *BNode {
	mergedNode := &BNode{data: make([]byte, BTREE_PAGE_SIZE)}

	mergedNode.SetHeader(first.GetType(), first.GetStoredKeysNumber()+first.GetStoredKeysNumber())

	mergedNode.Copy(first, 0, 0, first.GetStoredKeysNumber())
	mergedNode.Copy(second, first.GetStoredKeysNumber(), first.GetStoredKeysNumber(), second.GetStoredKeysNumber())

	return mergedNode
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

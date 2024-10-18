package storage

import (
	"bytes"
	"fmt"
	"log"
)

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

type BTreeStorageAdapter interface {
	Get(BNodePointer) *BNode    // dereference a pointer
	Create(*BNode) BNodePointer // allocate a new page
	Delete(BNodePointer)        // deallocate a page
}

type BTree struct {
	storage BTreeStorageAdapter
	root    BNodePointer
}

func (tree *BTree) Set(key []byte, value []byte) error {
	if len(key) > BTREE_MAX_KEY_SIZE {
		return fmt.Errorf("BTree supports only keys within the size %d", BTREE_MAX_KEY_SIZE)
	}

	if len(value) > BTREE_MAX_VAL_SIZE {
		return fmt.Errorf("BTree supports only values within the size %d", BTREE_MAX_VAL_SIZE)
	}

	if tree.root == BNodePointer(0) {
		rootNode := &BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		rootNode.setHeader(BNODE_LEAF, 1)

		rootNode.appendKeyValue(key, value)

		tree.root = tree.storage.Create(rootNode)

		return nil
	}

	rootNode := tree.storage.Get(tree.root)

	tree.storage.Delete(tree.root)

	rootNode = tree.setKeyValue(rootNode, key, value)

	if rootNode.getSizeInBytes() > BTREE_PAGE_SIZE {
		splittedNodes := tree.splitNode(rootNode)

		rootNode = &BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		rootNode.setHeader(BNODE_PARENT, uint16(len(splittedNodes)))

		for _, child := range splittedNodes {
			firstStoredKey, _ := child.getKey(BNodeKeyPosition(0))
			rootNode.appendPointer(firstStoredKey, tree.storage.Create(child))
		}
	}

	tree.root = tree.storage.Create(rootNode)

	return nil
}

func (tree *BTree) Delete(key []byte) error {
	if len(key) > BTREE_MAX_KEY_SIZE {
		return fmt.Errorf("BTree supports only keys within the size %d", BTREE_MAX_KEY_SIZE)
	}

	if tree.root == BNodePointer(0) {
		return nil
	}

	rootNode := tree.storage.Get(tree.root)
	updatedRootNode := tree.deleteKeyValue(rootNode, key)

	if rootNode == updatedRootNode {
		return nil
	}

	tree.storage.Delete(tree.root)

	if updatedRootNode.getType() == BNODE_PARENT && updatedRootNode.getStoredKeysNumber() == 1 {
		firstChild, _ := updatedRootNode.getChildPointer(BNodeKeyPosition(0))
		updatedRootNode = tree.storage.Get(firstChild)
	}

	tree.root = tree.storage.Create(updatedRootNode)

	return nil
}

func (tree *BTree) setKeyValue(node *BNode, key []byte, value []byte) *BNode {
	keyPosition := tree.getLessOrEqualKeyPosition(node, key)
	switch node.getType() {
	case BNODE_LEAF:
		{
			storedKeyAtSamePosition, _ := node.getKey(keyPosition)

			if bytes.Equal(key, storedKeyAtSamePosition) {
				return tree.updateLeafKeyValue(node, keyPosition, key, value)
			} else {
				return tree.insertLeafKeyValue(node, keyPosition, key, value)
			}
		}
	case BNODE_PARENT:
		{
			return tree.setParentKeyValue(node, keyPosition, key, value)
		}
	default:
		{
			log.Printf("Unsupported node type %d", node.getType())
			return node
		}
	}
}

func (tree *BTree) deleteKeyValue(node *BNode, key []byte) *BNode {
	switch node.getType() {
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
			log.Printf("Unsupported node type %d", node.getType())
			return node
		}
	}
}

func (tree *BTree) updateLeafKeyValue(node *BNode, position BNodeKeyPosition, key []byte, value []byte) *BNode {
	newNode := &BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	newNode.setHeader(BNODE_LEAF, node.getStoredKeysNumber())

	newNode.copy(node, 0, 0, position)
	newNode.appendKeyValue(key, value)
	newNode.copy(node, position+1, position+1, node.getStoredKeysNumber()-(position+1))

	return newNode
}

func (tree *BTree) insertLeafKeyValue(node *BNode, position BNodeKeyPosition, key []byte, value []byte) *BNode {
	newNode := &BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	newNode.setHeader(BNODE_LEAF, node.getStoredKeysNumber()+1)

	newNode.copy(node, 0, 0, position+1)
	newNode.appendKeyValue(key, value)
	newNode.copy(node, position+1, position+2, node.getStoredKeysNumber()-(position+1))

	return newNode
}

func (tree *BTree) deleteLeafKeyValue(node *BNode, key []byte) *BNode {
	position := tree.getLessOrEqualKeyPosition(node, key)
	storedKey, _ := node.getKey(position)

	if !bytes.Equal(key, storedKey) {
		return node
	}

	newNode := &BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	newNode.setHeader(BNODE_LEAF, node.getStoredKeysNumber()-1)

	newNode.copy(node, 0, 0, position)
	newNode.copy(node, position+1, position, node.getStoredKeysNumber()-(position+1))

	return newNode
}

func (tree *BTree) setParentKeyValue(parent *BNode, position BNodeKeyPosition, key []byte, value []byte) *BNode {
	childPointer, _ := parent.getChildPointer(position)
	updatedChild := tree.setKeyValue(tree.storage.Get(childPointer), key, value)

	tree.storage.Delete(childPointer)

	var newParentChildren []*BNode

	if updatedChild.getSizeInBytes() > BTREE_PAGE_SIZE {
		newParentChildren = tree.splitNode(updatedChild)
	} else {
		newParentChildren = []*BNode{updatedChild}
	}

	return tree.replaceParentChildren(parent, newParentChildren, position, 1)
}

func (tree *BTree) deleteParentKeyValue(node *BNode, key []byte) *BNode {
	keyPosition := tree.getLessOrEqualKeyPosition(node, key)
	childPointer, _ := node.getChildPointer(keyPosition)
	child := tree.storage.Get(childPointer)
	updatedChild := tree.deleteKeyValue(child, key)

	if child == updatedChild {
		return node
	}

	tree.storage.Delete(childPointer)

	return tree.mergeParentChildren(node, updatedChild, keyPosition)
}

func (tree *BTree) replaceParentChildren(parent *BNode, children []*BNode, position BNodeKeyPosition, quantity uint16) *BNode {
	newNode := &BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	newNode.setHeader(BNODE_PARENT, parent.getStoredKeysNumber()-quantity+uint16(len(children)))
	newNode.copy(parent, 0, 0, position)

	for _, child := range children {
		firstChildStoredKey, _ := child.getKey(BNodeKeyPosition(0))
		newNode.appendPointer(firstChildStoredKey, tree.storage.Create(child))
	}

	newNode.copy(parent, position+quantity, position+uint16(len(children)), parent.getStoredKeysNumber()-(position+quantity))

	return newNode
}

func (tree *BTree) mergeParentChildren(node *BNode, newChild *BNode, position BNodeKeyPosition) *BNode {
	if newChild.getSizeInBytes() < BTREE_PAGE_SIZE/4 {
		if position > 0 {
			leftChildPointer, _ := node.getChildPointer(position - 1)
			leftChild := tree.storage.Get(leftChildPointer)

			if leftChild.getSizeInBytes()+newChild.getSizeInBytes() < BTREE_PAGE_SIZE-HEADER_SIZE {
				tree.storage.Delete(leftChildPointer)

				return tree.replaceParentChildren(node, []*BNode{tree.mergeNodes(leftChild, newChild)}, position-1, 2)
			}
		}

		if position < node.getStoredKeysNumber()-1 {
			rightChildPointer, _ := node.getChildPointer(position + 1)
			rightChild := tree.storage.Get(rightChildPointer)

			if rightChild.getSizeInBytes()+newChild.getSizeInBytes() < BTREE_PAGE_SIZE-HEADER_SIZE {
				tree.storage.Delete(rightChildPointer)

				return tree.replaceParentChildren(node, []*BNode{tree.mergeNodes(newChild, rightChild)}, position, 2)
			}
		}
	}

	return tree.replaceParentChildren(node, []*BNode{newChild}, position, 1)
}

func (tree *BTree) splitNode(node *BNode) []*BNode {
	keysNumber := node.getStoredKeysNumber()
	storedKeysForFirstNode := keysNumber - 1
	storedKeysForSecondNode := keysNumber - storedKeysForFirstNode

	firstNode := &BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	secondNode := &BNode{data: make([]byte, BTREE_PAGE_SIZE)}

	firstNode.setHeader(node.getType(), storedKeysForFirstNode)
	secondNode.setHeader(node.getType(), storedKeysForSecondNode)

	firstNode.copy(node, 0, 0, storedKeysForFirstNode)
	secondNode.copy(node, storedKeysForFirstNode, 0, storedKeysForSecondNode)

	if firstNode.getSizeInBytes() > BTREE_PAGE_SIZE {
		splittedNodes := tree.splitNode(firstNode)

		thirdNode := secondNode
		firstNode := splittedNodes[0]
		secondNode = splittedNodes[1]

		firstNode.data = firstNode.data[:BTREE_PAGE_SIZE]

		return []*BNode{firstNode, secondNode, thirdNode}
	} else {
		firstNode.data = firstNode.data[:BTREE_PAGE_SIZE]

		return []*BNode{firstNode, secondNode}
	}
}

func (tree *BTree) mergeNodes(first *BNode, second *BNode) *BNode {
	mergedNode := &BNode{data: make([]byte, BTREE_PAGE_SIZE)}

	mergedNode.setHeader(first.getType(), first.getStoredKeysNumber()+first.getStoredKeysNumber())

	mergedNode.copy(first, 0, 0, first.getStoredKeysNumber())
	mergedNode.copy(second, first.getStoredKeysNumber(), first.getStoredKeysNumber(), second.getStoredKeysNumber())

	return mergedNode
}

func (tree *BTree) getLessOrEqualKeyPosition(node *BNode, key []byte) BNodeKeyPosition {
	storedKeysNumber := node.getStoredKeysNumber()
	// we find the position of last key that is less or equal than passed key
	// by default sequence number is 0 because we visited this node from the parent that contains the same key
	// thus first stored key is always less or equal to passed
	position := BNodeKeyPosition(0)

	for keyPosition := BNodeKeyPosition(1); keyPosition < storedKeysNumber; keyPosition++ {
		storedKey, _ := node.getKey(keyPosition)

		if bytes.Compare(key, storedKey) >= 0 {
			position = keyPosition
		} else {
			break
		}
	}

	return position
}

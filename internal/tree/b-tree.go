package tree

import (
	"bytes"
	"fmt"
	"log"
)

type BTreeRootPointer = BNodePointer

type BTreeConfig struct {
	PageSize     int
	MaxKeySize   int
	MaxValueSize int
}
type BTree struct {
	storage Storage
	root    BNodePointer
	config  BTreeConfig
}

func NewBTree(root BTreeRootPointer, storage Storage, config BTreeConfig) *BTree {
	return &BTree{
		root:    root,
		storage: storage,
		config:  config,
	}
}

func (tree *BTree) Get(key []byte) ([]byte, error) {
	if len(key) > tree.config.MaxKeySize {
		return nil, fmt.Errorf("BTree supports only keys within the size %d", tree.config.MaxKeySize)
	}

	if tree.root == NULL_NODE {
		return nil, nil
	}

	return tree.getKeyValue(tree.storage.Get(tree.root), key), nil
}

func (tree *BTree) Set(key []byte, value []byte) ([]byte, error) {
	if len(value) > tree.config.MaxValueSize {
		return nil, fmt.Errorf("BTree supports only values within the size %d", tree.config.MaxValueSize)
	}

	if len(key) > tree.config.MaxKeySize {
		return nil, fmt.Errorf("BTree supports only keys within the size %d", tree.config.MaxKeySize)
	}

	if tree.root == NULL_NODE {
		rootNode := &BNode{data: make([]byte, tree.config.PageSize)}
		rootNode.setHeader(BNODE_LEAF, 1)
		rootNode.appendKeyValue(key, value)

		tree.root = tree.storage.Create(rootNode)

		return nil, nil
	}

	rootNode := tree.storage.Get(tree.root)
	rootNode, oldValue := tree.setKeyValue(rootNode, key, value)

	if int(rootNode.size()) > tree.config.PageSize {
		splitNodes := tree.splitNode(rootNode)

		rootNode = &BNode{data: make([]byte, tree.config.PageSize)}
		rootNode.setHeader(BNODE_PARENT, uint16(len(splitNodes)))

		for _, child := range splitNodes {
			firstStoredKey := child.getKey(BNodeKeyPosition(0))
			rootNode.appendPointer(firstStoredKey, tree.storage.Create(child))
		}
	}

	tree.storage.Delete(tree.root)
	tree.root = tree.storage.Create(rootNode)

	return oldValue, nil
}

func (tree *BTree) Delete(key []byte) ([]byte, error) {
	if len(key) > tree.config.MaxKeySize {
		return nil, fmt.Errorf("BTree supports only keys within the size %d", tree.config.MaxKeySize)
	}

	if tree.root == NULL_NODE {
		return nil, nil
	}

	rootNode := tree.storage.Get(tree.root)
	updatedRootNode, oldValue := tree.deleteKeyValue(rootNode, key)

	if rootNode == updatedRootNode {
		return oldValue, nil
	}

	tree.storage.Delete(tree.root)

	if updatedRootNode.getType() == BNODE_PARENT && updatedRootNode.getStoredKeysNumber() == 1 {
		firstChild := updatedRootNode.getChildPointer(BNodeKeyPosition(0))
		tree.root = firstChild
	} else {
		tree.root = tree.storage.Create(updatedRootNode)
	}

	return oldValue, nil
}

func (tree *BTree) Root() BTreeRootPointer {
	return tree.root
}

func (tree *BTree) getKeyValue(node *BNode, key []byte) []byte {
	keyPosition := tree.getLessOrEqualKeyPosition(node, key)
	switch node.getType() {
	case BNODE_LEAF:
		{
			storedKey := node.getKey(keyPosition)

			if bytes.Equal(key, storedKey) {
				return node.getValue(keyPosition)
			} else {
				return nil
			}
		}
	case BNODE_PARENT:
		{
			return tree.getKeyValue(tree.storage.Get(node.getChildPointer(keyPosition)), key)
		}
	default:
		{
			log.Printf("Unsupported node type %d", node.getType())
			return nil
		}
	}
}

func (tree *BTree) setKeyValue(node *BNode, key []byte, value []byte) (*BNode, []byte) {
	keyPosition := tree.getLessOrEqualKeyPosition(node, key)
	switch node.getType() {
	case BNODE_LEAF:
		{
			storedKeyAtSamePosition := node.getKey(keyPosition)

			if bytes.Equal(key, storedKeyAtSamePosition) {
				return tree.updateLeafKeyValue(node, keyPosition, key, value), node.getValue(keyPosition)
			} else {
				return tree.insertLeafKeyValue(node, keyPosition, key, value), nil
			}
		}
	case BNODE_PARENT:
		{
			return tree.setParentKeyValue(node, keyPosition, key, value)
		}
	default:
		{
			log.Printf("Unsupported node type %d", node.getType())
			return node, nil
		}
	}
}

func (tree *BTree) deleteKeyValue(node *BNode, key []byte) (*BNode, []byte) {
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
			return node, nil
		}
	}
}

func (tree *BTree) updateLeafKeyValue(node *BNode, position BNodeKeyPosition, key []byte, value []byte) *BNode {
	newNode := &BNode{data: make([]byte, 2*tree.config.PageSize)}
	newNode.setHeader(BNODE_LEAF, node.getStoredKeysNumber())

	newNode.copy(node, 0, 0, position)
	newNode.appendKeyValue(key, value)
	newNode.copy(node, position+1, position+1, node.getStoredKeysNumber()-(position+1))

	return newNode
}

func (tree *BTree) insertLeafKeyValue(node *BNode, position BNodeKeyPosition, key []byte, value []byte) *BNode {
	newNode := &BNode{data: make([]byte, 2*tree.config.PageSize)}
	newNode.setHeader(BNODE_LEAF, node.getStoredKeysNumber()+1)

	if node.getStoredKeysNumber() > 0 {
		newNode.copy(node, 0, 0, position+1)
		newNode.appendKeyValue(key, value)
		newNode.copy(node, position+1, position+2, node.getStoredKeysNumber()-(position+1))
	} else {
		newNode.appendKeyValue(key, value)
	}

	return newNode
}

func (tree *BTree) deleteLeafKeyValue(node *BNode, key []byte) (*BNode, []byte) {
	position := tree.getLessOrEqualKeyPosition(node, key)
	storedKey := node.getKey(position)

	if !bytes.Equal(key, storedKey) {
		return node, nil
	}

	newNode := &BNode{data: make([]byte, tree.config.PageSize)}
	newNode.setHeader(BNODE_LEAF, node.getStoredKeysNumber()-1)

	newNode.copy(node, 0, 0, position)
	newNode.copy(node, position+1, position, node.getStoredKeysNumber()-(position+1))

	return newNode, node.getValue(position)
}

func (tree *BTree) setParentKeyValue(parent *BNode, position BNodeKeyPosition, key []byte, value []byte) (*BNode, []byte) {
	childPointer := parent.getChildPointer(position)
	updatedChild, oldValue := tree.setKeyValue(tree.storage.Get(childPointer), key, value)

	tree.storage.Delete(childPointer)

	var newParentChildren []*BNode

	if int(updatedChild.size()) > tree.config.PageSize {
		newParentChildren = tree.splitNode(updatedChild)
	} else {
		newParentChildren = []*BNode{updatedChild}
	}

	return tree.replaceParentChildren(parent, newParentChildren, position, 1), oldValue
}

func (tree *BTree) deleteParentKeyValue(node *BNode, key []byte) (*BNode, []byte) {
	keyPosition := tree.getLessOrEqualKeyPosition(node, key)
	childPointer := node.getChildPointer(keyPosition)
	child := tree.storage.Get(childPointer)
	updatedChild, oldValue := tree.deleteKeyValue(child, key)

	if child == updatedChild {
		return node, oldValue
	}

	if updatedChild.getStoredKeysNumber() == 0 {
		return tree.deleteParentChild(node, keyPosition), oldValue
	}

	return tree.mergeParentChildren(node, updatedChild, keyPosition), oldValue
}

func (tree *BTree) replaceParentChildren(parent *BNode, children []*BNode, position BNodeKeyPosition, quantity uint16) *BNode {
	newNode := &BNode{data: make([]byte, 2*tree.config.PageSize)}

	newNode.setHeader(BNODE_PARENT, parent.getStoredKeysNumber()-quantity+uint16(len(children)))
	newNode.copy(parent, 0, 0, position)

	for _, child := range children {
		firstChildStoredKey := child.getKey(BNodeKeyPosition(0))
		newNode.appendPointer(firstChildStoredKey, tree.storage.Create(child))
	}

	newNode.copy(parent, position+quantity, position+uint16(len(children)), parent.getStoredKeysNumber()-(position+quantity))

	return newNode
}

func (tree *BTree) mergeParentChildren(node *BNode, newChild *BNode, position BNodeKeyPosition) *BNode {
	defer tree.storage.Delete(node.getChildPointer(position))

	if int(newChild.size()) < tree.config.PageSize/4 {
		if position > 0 {
			leftChildPointer := node.getChildPointer(position - 1)
			leftChild := tree.storage.Get(leftChildPointer)

			if int(leftChild.size()+newChild.size()) < tree.config.PageSize-HEADER_SIZE {
				updatedParent := tree.replaceParentChildren(node, []*BNode{tree.mergeNodes(leftChild, newChild)}, position-1, 2)

				tree.storage.Delete(leftChildPointer)

				return updatedParent
			}
		}

		if position < node.getStoredKeysNumber()-1 {
			rightChildPointer := node.getChildPointer(position + 1)
			rightChild := tree.storage.Get(rightChildPointer)

			if int(rightChild.size()+newChild.size()) < tree.config.PageSize-HEADER_SIZE {
				updatedParent := tree.replaceParentChildren(node, []*BNode{tree.mergeNodes(newChild, rightChild)}, position, 2)

				tree.storage.Delete(rightChildPointer)

				return updatedParent
			}
		}
	}
	return tree.replaceParentChildren(node, []*BNode{newChild}, position, 1)
}

func (tree *BTree) deleteParentChild(parent *BNode, position BNodeKeyPosition) *BNode {
	newNode := &BNode{data: make([]byte, tree.config.PageSize)}

	tree.storage.Delete(parent.getChildPointer(position))

	newNode.setHeader(BNODE_PARENT, parent.getStoredKeysNumber()-1)
	newNode.copy(parent, 0, 0, position)
	newNode.copy(parent, position+1, position, parent.getStoredKeysNumber()-(position+1))

	return newNode
}

func (tree *BTree) splitNode(node *BNode) []*BNode {
	storedKeysNumber := node.getStoredKeysNumber()
	splitChildPosition := storedKeysNumber - 1

	for int(node.size()-node.getKeyValueOffset(splitChildPosition-1)) <= tree.config.PageSize/2 && splitChildPosition > 1 {
		splitChildPosition--
	}

	firstNode := &BNode{data: make([]byte, tree.config.PageSize)}
	secondNode := &BNode{data: make([]byte, tree.config.PageSize)}

	firstNode.setHeader(node.getType(), splitChildPosition)
	secondNode.setHeader(node.getType(), storedKeysNumber-splitChildPosition)

	firstNode.copy(node, 0, 0, splitChildPosition)
	secondNode.copy(node, splitChildPosition, 0, storedKeysNumber-splitChildPosition)

	return []*BNode{firstNode, secondNode}
}

func (tree *BTree) mergeNodes(first *BNode, second *BNode) *BNode {
	mergedNode := &BNode{data: make([]byte, tree.config.PageSize)}

	mergedNode.setHeader(first.getType(), first.getStoredKeysNumber()+second.getStoredKeysNumber())

	mergedNode.copy(first, 0, 0, first.getStoredKeysNumber())
	mergedNode.copy(second, 0, first.getStoredKeysNumber(), second.getStoredKeysNumber())

	return mergedNode
}

func (tree *BTree) getLessOrEqualKeyPosition(node *BNode, key []byte) BNodeKeyPosition {
	storedKeysNumber := node.getStoredKeysNumber()
	// we find the position of last key that is less or equal than passed key
	// by default sequence number is 0 because we visited this node from the parent that contains the same key
	// thus first stored key is always less or equal to passed
	position := BNodeKeyPosition(0)

	for keyPosition := BNodeKeyPosition(1); keyPosition < storedKeysNumber; keyPosition++ {
		storedKey := node.getKey(keyPosition)

		if bytes.Compare(key, storedKey) >= 0 {
			position = keyPosition
		} else {
			break
		}
	}

	return position
}

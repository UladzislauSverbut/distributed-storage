package tree

import (
	"bytes"
	"fmt"
	"log"
)

type TreeRootPointer = NodePointer
type TreeVersion = uint64

type TreeConfig struct {
	PageSize     int
	MaxKeySize   int
	MaxValueSize int
}
type Tree struct {
	storage *TreeStorage
	root    NodePointer
	config  TreeConfig
}

func NewTree(root TreeRootPointer, storage *TreeStorage, config TreeConfig) *Tree {
	return &Tree{
		root:    root,
		storage: storage,
		config:  config,
	}
}

func (tree *Tree) Get(key []byte) ([]byte, error) {
	if len(key) > tree.config.MaxKeySize {
		return nil, fmt.Errorf("Tree: supports only keys within the size %d", tree.config.MaxKeySize)
	}

	if tree.root == NULL_NODE {
		return nil, nil
	}

	return tree.getKeyValue(tree.storage.Get(tree.root), key), nil
}

func (tree *Tree) Set(key []byte, value []byte) ([]byte, error) {
	if len(value) > tree.config.MaxValueSize {
		return nil, fmt.Errorf("Tree: supports only values within the size %d", tree.config.MaxValueSize)
	}

	if len(key) > tree.config.MaxKeySize {
		return nil, fmt.Errorf("Tree: supports only keys within the size %d", tree.config.MaxKeySize)
	}

	if tree.root == NULL_NODE {
		rootNode := &Node{data: make([]byte, tree.config.PageSize)}
		rootNode.setHeader(NODE_LEAF, 1)
		rootNode.appendKeyValue(key, value)

		tree.root = tree.storage.Create(rootNode)

		return nil, nil
	}

	rootNode := tree.storage.Get(tree.root)
	rootNode, oldValue := tree.setKeyValue(rootNode, key, value)

	if int(rootNode.size()) > tree.config.PageSize {
		splitNodes := tree.splitNode(rootNode)

		rootNode = &Node{data: make([]byte, tree.config.PageSize)}
		rootNode.setHeader(NODE_PARENT, uint16(len(splitNodes)))

		for _, child := range splitNodes {
			firstStoredKey := child.getKey(NodeKeyPosition(0))
			rootNode.appendPointer(firstStoredKey, tree.storage.Create(child))
		}
	}

	tree.storage.Delete(tree.root)
	tree.root = tree.storage.Create(rootNode)

	return oldValue, nil
}

func (tree *Tree) Delete(key []byte) ([]byte, error) {
	if len(key) > tree.config.MaxKeySize {
		return nil, fmt.Errorf("Tree supports only keys within the size %d", tree.config.MaxKeySize)
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

	if updatedRootNode.getType() == NODE_PARENT && updatedRootNode.getStoredKeysNumber() == 1 {
		firstChild := updatedRootNode.getChildPointer(NodeKeyPosition(0))
		tree.root = firstChild
	} else {
		tree.root = tree.storage.Create(updatedRootNode)
	}

	return oldValue, nil
}

func (tree *Tree) Root() TreeRootPointer {
	return tree.root
}

func (tree *Tree) getKeyValue(node *Node, key []byte) []byte {
	keyPosition := tree.getLessOrEqualKeyPosition(node, key)
	switch node.getType() {
	case NODE_LEAF:
		{
			storedKey := node.getKey(keyPosition)

			if bytes.Equal(key, storedKey) {
				return node.getValue(keyPosition)
			} else {
				return nil
			}
		}
	case NODE_PARENT:
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

func (tree *Tree) setKeyValue(node *Node, key []byte, value []byte) (*Node, []byte) {
	keyPosition := tree.getLessOrEqualKeyPosition(node, key)
	switch node.getType() {
	case NODE_LEAF:
		{
			storedKeyAtSamePosition := node.getKey(keyPosition)

			if bytes.Equal(key, storedKeyAtSamePosition) {
				return tree.updateLeafKeyValue(node, keyPosition, key, value), node.getValue(keyPosition)
			} else {
				return tree.insertLeafKeyValue(node, keyPosition, key, value), nil
			}
		}
	case NODE_PARENT:
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

func (tree *Tree) deleteKeyValue(node *Node, key []byte) (*Node, []byte) {
	switch node.getType() {
	case NODE_LEAF:
		{
			return tree.deleteLeafKeyValue(node, key)
		}
	case NODE_PARENT:
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

func (tree *Tree) updateLeafKeyValue(node *Node, position NodeKeyPosition, key []byte, value []byte) *Node {
	newNode := &Node{data: make([]byte, 2*tree.config.PageSize)}
	newNode.setHeader(NODE_LEAF, node.getStoredKeysNumber())

	newNode.copy(node, 0, 0, position)
	newNode.appendKeyValue(key, value)
	newNode.copy(node, position+1, position+1, node.getStoredKeysNumber()-(position+1))

	return newNode
}

func (tree *Tree) insertLeafKeyValue(node *Node, position NodeKeyPosition, key []byte, value []byte) *Node {
	newNode := &Node{data: make([]byte, 2*tree.config.PageSize)}
	newNode.setHeader(NODE_LEAF, node.getStoredKeysNumber()+1)

	if node.getStoredKeysNumber() > 0 {
		newNode.copy(node, 0, 0, position+1)
		newNode.appendKeyValue(key, value)
		newNode.copy(node, position+1, position+2, node.getStoredKeysNumber()-(position+1))
	} else {
		newNode.appendKeyValue(key, value)
	}

	return newNode
}

func (tree *Tree) deleteLeafKeyValue(node *Node, key []byte) (*Node, []byte) {
	position := tree.getLessOrEqualKeyPosition(node, key)
	storedKey := node.getKey(position)

	if !bytes.Equal(key, storedKey) {
		return node, nil
	}

	newNode := &Node{data: make([]byte, tree.config.PageSize)}
	newNode.setHeader(NODE_LEAF, node.getStoredKeysNumber()-1)

	newNode.copy(node, 0, 0, position)
	newNode.copy(node, position+1, position, node.getStoredKeysNumber()-(position+1))

	return newNode, node.getValue(position)
}

func (tree *Tree) setParentKeyValue(parent *Node, position NodeKeyPosition, key []byte, value []byte) (*Node, []byte) {
	childPointer := parent.getChildPointer(position)
	updatedChild, oldValue := tree.setKeyValue(tree.storage.Get(childPointer), key, value)

	tree.storage.Delete(childPointer)

	var newParentChildren []*Node

	if int(updatedChild.size()) > tree.config.PageSize {
		newParentChildren = tree.splitNode(updatedChild)
	} else {
		newParentChildren = []*Node{updatedChild}
	}

	return tree.replaceParentChildren(parent, newParentChildren, position, 1), oldValue
}

func (tree *Tree) deleteParentKeyValue(node *Node, key []byte) (*Node, []byte) {
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

func (tree *Tree) replaceParentChildren(parent *Node, children []*Node, position NodeKeyPosition, quantity uint16) *Node {
	newNode := &Node{data: make([]byte, 2*tree.config.PageSize)}

	newNode.setHeader(NODE_PARENT, parent.getStoredKeysNumber()-quantity+uint16(len(children)))
	newNode.copy(parent, 0, 0, position)

	for _, child := range children {
		firstChildStoredKey := child.getKey(NodeKeyPosition(0))
		newNode.appendPointer(firstChildStoredKey, tree.storage.Create(child))
	}

	newNode.copy(parent, position+quantity, position+uint16(len(children)), parent.getStoredKeysNumber()-(position+quantity))

	return newNode
}

func (tree *Tree) mergeParentChildren(node *Node, newChild *Node, position NodeKeyPosition) *Node {
	defer tree.storage.Delete(node.getChildPointer(position))

	if int(newChild.size()) < tree.config.PageSize/4 {
		if position > 0 {
			leftChildPointer := node.getChildPointer(position - 1)
			leftChild := tree.storage.Get(leftChildPointer)

			if int(leftChild.size()+newChild.size()) < tree.config.PageSize-HEADER_SIZE {
				updatedParent := tree.replaceParentChildren(node, []*Node{tree.mergeNodes(leftChild, newChild)}, position-1, 2)

				tree.storage.Delete(leftChildPointer)

				return updatedParent
			}
		}

		if position < node.getStoredKeysNumber()-1 {
			rightChildPointer := node.getChildPointer(position + 1)
			rightChild := tree.storage.Get(rightChildPointer)

			if int(rightChild.size()+newChild.size()) < tree.config.PageSize-HEADER_SIZE {
				updatedParent := tree.replaceParentChildren(node, []*Node{tree.mergeNodes(newChild, rightChild)}, position, 2)

				tree.storage.Delete(rightChildPointer)

				return updatedParent
			}
		}
	}
	return tree.replaceParentChildren(node, []*Node{newChild}, position, 1)
}

func (tree *Tree) deleteParentChild(parent *Node, position NodeKeyPosition) *Node {
	newNode := &Node{data: make([]byte, tree.config.PageSize)}

	tree.storage.Delete(parent.getChildPointer(position))

	newNode.setHeader(NODE_PARENT, parent.getStoredKeysNumber()-1)
	newNode.copy(parent, 0, 0, position)
	newNode.copy(parent, position+1, position, parent.getStoredKeysNumber()-(position+1))

	return newNode
}

func (tree *Tree) splitNode(node *Node) []*Node {
	storedKeysNumber := node.getStoredKeysNumber()
	splitChildPosition := storedKeysNumber - 1

	for int(node.size()-node.getKeyValueOffset(splitChildPosition-1)) <= tree.config.PageSize/2 && splitChildPosition > 1 {
		splitChildPosition--
	}

	firstNode := &Node{data: make([]byte, tree.config.PageSize)}
	secondNode := &Node{data: make([]byte, tree.config.PageSize)}

	firstNode.setHeader(node.getType(), splitChildPosition)
	secondNode.setHeader(node.getType(), storedKeysNumber-splitChildPosition)

	firstNode.copy(node, 0, 0, splitChildPosition)
	secondNode.copy(node, splitChildPosition, 0, storedKeysNumber-splitChildPosition)

	return []*Node{firstNode, secondNode}
}

func (tree *Tree) mergeNodes(first *Node, second *Node) *Node {
	mergedNode := &Node{data: make([]byte, tree.config.PageSize)}

	mergedNode.setHeader(first.getType(), first.getStoredKeysNumber()+second.getStoredKeysNumber())

	mergedNode.copy(first, 0, 0, first.getStoredKeysNumber())
	mergedNode.copy(second, 0, first.getStoredKeysNumber(), second.getStoredKeysNumber())

	return mergedNode
}

func (tree *Tree) getLessOrEqualKeyPosition(node *Node, key []byte) NodeKeyPosition {
	storedKeysNumber := node.getStoredKeysNumber()
	// we find the position of last key that is less or equal than passed key
	// by default sequence number is 0 because we visited this node from the parent that contains the same key
	// thus first stored key is always less or equal to passed
	position := NodeKeyPosition(0)

	for keyPosition := NodeKeyPosition(1); keyPosition < storedKeysNumber; keyPosition++ {
		storedKey := node.getKey(keyPosition)

		if bytes.Compare(key, storedKey) >= 0 {
			position = keyPosition
		} else {
			break
		}
	}

	return position
}

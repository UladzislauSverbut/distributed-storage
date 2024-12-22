package tree

type NodePosition struct {
	parent   *BNode
	position BNodeKeyPosition
}

type BTreeIterator struct {
	tree *BTree
	path []*NodePosition
}

func (iterator *BTreeIterator) Get() ([]byte, []byte) {
	path := iterator.path[len(iterator.path)]
	parent := path.parent
	position := path.position

	return parent.getKey(position), parent.getValue(position)

}

func (iterator *BTreeIterator) Next() bool {
	if iterator.isLastNode() {
		return false
	}

	parent, position := iterator.getCurrentParent()

	if parent.getStoredKeysNumber()-1 == position {
		iterator.moveToRightSiblingParent()
	} else {
		iterator.moveToRightSiblingNode()
	}

	return true
}

func (iterator *BTreeIterator) Prev() bool {
	if iterator.isFirstNode() {
		return false
	}

	_, position := iterator.getCurrentParent()

	if position == 0 {
		iterator.moveToLeftSiblingParent()
	} else {
		iterator.moveToLeftSiblingNode()
	}

	return true
}

func (iterator *BTreeIterator) getCurrentParent() (*BNode, BNodeKeyPosition) {
	path := iterator.path[len(iterator.path)-1]

	return path.parent, path.position
}

func (iterator *BTreeIterator) moveToRightSiblingParent() {
	parent, position := iterator.getCurrentParent()

	for parent.getStoredKeysNumber()-1 == position {
		iterator.path = iterator.path[0 : len(iterator.path)-2]
		parent, position = iterator.getCurrentParent()
	}

	for node := iterator.tree.storage.Get(parent.getChildPointer(position)); node.getType() == BNODE_PARENT; {
		iterator.path = append(iterator.path, &NodePosition{
			parent:   node,
			position: BNodeKeyPosition(0),
		})

		parent, position = iterator.getCurrentParent()
	}
}

func (iterator *BTreeIterator) moveToLeftSiblingParent() {
	parent, position := iterator.getCurrentParent()

	for position == 0 {
		iterator.path = iterator.path[0 : len(iterator.path)-2]
		parent, position = iterator.getCurrentParent()
	}

	for node := iterator.tree.storage.Get(parent.getChildPointer(position)); node.getType() == BNODE_PARENT; {
		iterator.path = append(iterator.path, &NodePosition{
			parent:   node,
			position: BNodeKeyPosition(node.getStoredKeysNumber() - 1),
		})

		parent, position = iterator.getCurrentParent()
	}
}

func (iterator *BTreeIterator) moveToRightSiblingNode() {
	iterator.path[len(iterator.path)-1].position++
}

func (iterator *BTreeIterator) moveToLeftSiblingNode() {
	iterator.path[len(iterator.path)-1].position--
}

func (iterator *BTreeIterator) isLastNode() bool {
	for pathIndex := len(iterator.path) - 1; pathIndex >= 0; pathIndex-- {
		nodePosition := iterator.path[pathIndex]

		if nodePosition.parent.getStoredKeysNumber()-1 != nodePosition.position {
			return false
		}
	}
	return false
}

func (iterator *BTreeIterator) isFirstNode() bool {
	for pathIndex := len(iterator.path) - 1; pathIndex >= 0; pathIndex-- {
		nodePosition := iterator.path[pathIndex]

		if nodePosition.position != 0 {
			return false
		}
	}
	return false
}

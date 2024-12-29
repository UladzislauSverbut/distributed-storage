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
	path := iterator.path[len(iterator.path)-1]
	parent := path.parent
	position := path.position

	return parent.getKey(position), parent.getValue(position)
}

func (iterator *BTreeIterator) Next() {
	if !iterator.HasNext() {
		return
	}

	parent, position := iterator.getCurrentParent()

	if parent.getStoredKeysNumber()-1 == position {
		iterator.moveToRightSiblingParent()
	} else {
		iterator.moveToRightSiblingNode()
	}
}

func (iterator *BTreeIterator) Prev() {
	if !iterator.HasPrev() {
		return
	}

	_, position := iterator.getCurrentParent()

	if position == 0 {
		iterator.moveToLeftSiblingParent()
	} else {
		iterator.moveToLeftSiblingNode()
	}
}

func (iterator *BTreeIterator) HasNext() bool {
	for pathIndex := len(iterator.path) - 1; pathIndex >= 0; pathIndex-- {
		nodePosition := iterator.path[pathIndex]

		if nodePosition.parent.getStoredKeysNumber()-1 != nodePosition.position {
			return true
		}
	}
	return false
}

func (iterator *BTreeIterator) HasPrev() bool {
	for pathIndex := len(iterator.path) - 1; pathIndex >= 0; pathIndex-- {
		nodePosition := iterator.path[pathIndex]

		if nodePosition.position != 0 {
			return true
		}
	}
	return false
}

func (iterator *BTreeIterator) getCurrentParent() (*BNode, BNodeKeyPosition) {
	path := iterator.path[len(iterator.path)-1]

	return path.parent, path.position
}

func (iterator *BTreeIterator) moveToRightSiblingParent() {
	parent, position := iterator.getCurrentParent()

	for parent.getStoredKeysNumber()-1 == position {
		parent, position = iterator.moveToPreviousParent()
	}

	_, position = iterator.moveToRightSiblingNode()

	for parent.getType() == BNODE_PARENT {
		parent = iterator.tree.storage.Get(parent.getChildPointer(position))
		position = BNodeKeyPosition(0)

		iterator.path = append(iterator.path, &NodePosition{
			parent:   parent,
			position: position,
		})
	}
}

func (iterator *BTreeIterator) moveToLeftSiblingParent() {
	parent, position := iterator.getCurrentParent()

	for position == 0 {
		iterator.path = iterator.path[0 : len(iterator.path)-1]
		parent, position = iterator.getCurrentParent()
	}

	_, position = iterator.moveToLeftSiblingNode()

	for parent.getType() == BNODE_PARENT {
		parent = iterator.tree.storage.Get(parent.getChildPointer(position))
		position = parent.getStoredKeysNumber() - 1

		iterator.path = append(iterator.path, &NodePosition{
			parent:   parent,
			position: position,
		})
	}
}

func (iterator *BTreeIterator) moveToRightSiblingNode() (*BNode, BNodeKeyPosition) {
	iterator.path[len(iterator.path)-1].position++

	return iterator.getCurrentParent()
}

func (iterator *BTreeIterator) moveToLeftSiblingNode() (*BNode, BNodeKeyPosition) {
	iterator.path[len(iterator.path)-1].position--

	return iterator.getCurrentParent()
}

func (iterator *BTreeIterator) moveToPreviousParent() (*BNode, BNodeKeyPosition) {
	iterator.path = iterator.path[0 : len(iterator.path)-1]

	return iterator.getCurrentParent()
}

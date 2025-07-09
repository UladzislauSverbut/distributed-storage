package tree

type NodePosition struct {
	parent   *BNode
	position BNodeKeyPosition
}

type BTreeCursor struct {
	tree *BTree
	path []*NodePosition
}

func (cursor *BTreeCursor) Current() ([]byte, []byte) {
	path := cursor.path[len(cursor.path)-1]
	parent := path.parent
	position := path.position

	return parent.getKey(position), parent.getValue(position)
}

func (cursor *BTreeCursor) Next() ([]byte, []byte) {
	if !cursor.HasNext() {
		return nil, nil
	}

	parent, position := cursor.getCurrentParent()

	if parent.getStoredKeysNumber()-1 == position {
		cursor.moveToRightSiblingParent()
	} else {
		cursor.moveToRightSiblingNode()
	}

	return cursor.Current()
}

func (cursor *BTreeCursor) Prev() ([]byte, []byte) {
	if !cursor.HasPrev() {
		return nil, nil
	}

	_, position := cursor.getCurrentParent()

	if position == 0 {
		cursor.moveToLeftSiblingParent()
	} else {
		cursor.moveToLeftSiblingNode()
	}

	return cursor.Next()
}

func (cursor *BTreeCursor) HasNext() bool {
	for pathIndex := len(cursor.path) - 1; pathIndex >= 0; pathIndex-- {
		nodePosition := cursor.path[pathIndex]

		if nodePosition.parent.getStoredKeysNumber()-1 != nodePosition.position {
			return true
		}
	}
	return false
}

func (cursor *BTreeCursor) HasPrev() bool {
	for pathIndex := len(cursor.path) - 1; pathIndex >= 0; pathIndex-- {
		nodePosition := cursor.path[pathIndex]

		if nodePosition.position != 0 {
			return true
		}
	}
	return false
}

func (cursor *BTreeCursor) getCurrentParent() (*BNode, BNodeKeyPosition) {
	path := cursor.path[len(cursor.path)-1]

	return path.parent, path.position
}

func (cursor *BTreeCursor) moveToRightSiblingParent() {
	parent, position := cursor.getCurrentParent()

	for parent.getStoredKeysNumber()-1 == position {
		parent, position = cursor.moveToPreviousParent()
	}

	_, position = cursor.moveToRightSiblingNode()

	for parent.getType() == BNODE_PARENT {
		parent = cursor.tree.storage.Get(parent.getChildPointer(position))
		position = BNodeKeyPosition(0)

		cursor.path = append(cursor.path, &NodePosition{
			parent:   parent,
			position: position,
		})
	}
}

func (cursor *BTreeCursor) moveToLeftSiblingParent() {
	parent, position := cursor.getCurrentParent()

	for position == 0 {
		cursor.path = cursor.path[0 : len(cursor.path)-1]
		parent, position = cursor.getCurrentParent()
	}

	_, position = cursor.moveToLeftSiblingNode()

	for parent.getType() == BNODE_PARENT {
		parent = cursor.tree.storage.Get(parent.getChildPointer(position))
		position = parent.getStoredKeysNumber() - 1

		cursor.path = append(cursor.path, &NodePosition{
			parent:   parent,
			position: position,
		})
	}
}

func (cursor *BTreeCursor) moveToRightSiblingNode() (*BNode, BNodeKeyPosition) {
	cursor.path[len(cursor.path)-1].position++

	return cursor.getCurrentParent()
}

func (cursor *BTreeCursor) moveToLeftSiblingNode() (*BNode, BNodeKeyPosition) {
	cursor.path[len(cursor.path)-1].position--

	return cursor.getCurrentParent()
}

func (cursor *BTreeCursor) moveToPreviousParent() (*BNode, BNodeKeyPosition) {
	cursor.path = cursor.path[0 : len(cursor.path)-1]

	return cursor.getCurrentParent()
}

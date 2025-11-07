package tree

type NodePosition struct {
	parent   *BNode
	position BNodeKeyPosition
}

type Cursor struct {
	tree *BTree
	path []*NodePosition
}

func (cursor *Cursor) Current() ([]byte, []byte) {
	path := cursor.path[len(cursor.path)-1]
	parent := path.parent
	position := path.position

	return parent.getKey(position), parent.getValue(position)
}

func (cursor *Cursor) Next() ([]byte, []byte) {
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

func (cursor *Cursor) Prev() ([]byte, []byte) {
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

func (cursor *Cursor) HasNext() bool {
	for pathIndex := len(cursor.path) - 1; pathIndex >= 0; pathIndex-- {
		nodePosition := cursor.path[pathIndex]

		if nodePosition.parent.getStoredKeysNumber()-1 != nodePosition.position {
			return true
		}
	}
	return false
}

func (cursor *Cursor) HasPrev() bool {
	for pathIndex := len(cursor.path) - 1; pathIndex >= 0; pathIndex-- {
		nodePosition := cursor.path[pathIndex]

		if nodePosition.position != 0 {
			return true
		}
	}
	return false
}

func (cursor *Cursor) getCurrentParent() (*BNode, BNodeKeyPosition) {
	path := cursor.path[len(cursor.path)-1]

	return path.parent, path.position
}

func (cursor *Cursor) moveToRightSiblingParent() {
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

func (cursor *Cursor) moveToLeftSiblingParent() {
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

func (cursor *Cursor) moveToRightSiblingNode() (*BNode, BNodeKeyPosition) {
	cursor.path[len(cursor.path)-1].position++

	return cursor.getCurrentParent()
}

func (cursor *Cursor) moveToLeftSiblingNode() (*BNode, BNodeKeyPosition) {
	cursor.path[len(cursor.path)-1].position--

	return cursor.getCurrentParent()
}

func (cursor *Cursor) moveToPreviousParent() (*BNode, BNodeKeyPosition) {
	cursor.path = cursor.path[0 : len(cursor.path)-1]

	return cursor.getCurrentParent()
}

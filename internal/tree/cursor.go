package tree

type NodePosition struct {
	parent   *Node
	position NodeKeyPosition
}

type Cursor struct {
	tree *Tree
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

func (cursor *Cursor) getCurrentParent() (*Node, NodeKeyPosition) {
	path := cursor.path[len(cursor.path)-1]

	return path.parent, path.position
}

func (cursor *Cursor) moveToRightSiblingParent() {
	parent, position := cursor.getCurrentParent()

	for parent.getStoredKeysNumber()-1 == position {
		parent, position = cursor.moveToPreviousParent()
	}

	_, position = cursor.moveToRightSiblingNode()

	for parent.getType() == NODE_PARENT {
		parent = &Node{data: cursor.tree.pageManager.Page(parent.getChildPointer(position))}

		position = NodeKeyPosition(0)

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

	for parent.getType() == NODE_PARENT {
		parent = &Node{data: cursor.tree.pageManager.Page(parent.getChildPointer(position))}

		position = parent.getStoredKeysNumber() - 1

		cursor.path = append(cursor.path, &NodePosition{
			parent:   parent,
			position: position,
		})
	}
}

func (cursor *Cursor) moveToRightSiblingNode() (*Node, NodeKeyPosition) {
	cursor.path[len(cursor.path)-1].position++

	return cursor.getCurrentParent()
}

func (cursor *Cursor) moveToLeftSiblingNode() (*Node, NodeKeyPosition) {
	cursor.path[len(cursor.path)-1].position--

	return cursor.getCurrentParent()
}

func (cursor *Cursor) moveToPreviousParent() (*Node, NodeKeyPosition) {
	cursor.path = cursor.path[0 : len(cursor.path)-1]

	return cursor.getCurrentParent()
}

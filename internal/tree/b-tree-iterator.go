package tree

type BTreeExplorer struct {
	tree *BTree
}

func (explorer *BTreeExplorer) Seek(key []byte) *BTreeIterator {
	tree := explorer.tree
	iterator := &BTreeIterator{tree: tree}

	for nodePointer := tree.root; nodePointer != NULL_NODE; {
		node := tree.storage.Get(nodePointer)
		lessOrEqualNodePointer := tree.getLessOrEqualKeyPosition(node, key)

		iterator.path = append(iterator.path, node)
		iterator.position = append(iterator.position, lessOrEqualNodePointer)

		if node.getType() == BNODE_PARENT {
			nodePointer = node.getChildPointer(lessOrEqualNodePointer)
		} else {
			nodePointer = NULL_NODE
		}
	}

	return iterator
}

type BTreeIterator struct {
	tree     *BTree
	path     []*BNode
	position []uint16
}

func (tree *BTreeIterator) Value() ([]byte, []byte) {
	return nil, nil
}

func (iterator *BTreeIterator) Next() {

}

func (iterator *BTreeIterator) Prev() {
	iterator.moveCursorToLeftSibling(len(iterator.path) - 1)
}

func (iterator *BTreeIterator) moveCursorToLeftSibling(level int) {
	switch {
	case iterator.position[level] > 0:
		{
			iterator.position[level]--
		}
	case level > 0:
		{
			iterator.moveCursorToLeftSibling(level - 1)
		}
	default:
		return
	}

	if level+1 < len(iterator.position) {
		node := iterator.path[level]
		childNode := iterator.tree.storage.Get(node.getChildPointer(iterator.position[level]))

		iterator.path[level+1] = childNode
		iterator.position[level+1] = childNode.getStoredKeysNumber() - 1
	}
}

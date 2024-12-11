package tree

type BTreeIterator struct {
	tree     *BTree
	path     []BNode
	position []uint16
}

func (iterator *BTreeIterator) Value() ([]byte, []byte) {
	return nil, nil
}

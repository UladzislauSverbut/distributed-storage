package tree

import (
	"bytes"
	"fmt"
)

const (
	LESS_OR_EQUAL_COMPARISON    = -2
	LESS_COMPARISON             = -1
	GREATER_OR_EQUAL_COMPARISON = 1
	GREATER_COMPARISON          = 2
)

type BTreeScanner struct {
	tree *BTree
}

func NewBTreeScanner(tree *BTree) *BTreeScanner {
	return &BTreeScanner{tree: tree}
}

func (scanner *BTreeScanner) Seek(key []byte, compareStrategy int) (cursor *BTreeCursor) {
	cursor = scanner.seekLessOrEqual(key)
	foundKey, _ := cursor.Current()

	if scanner.compareKeys(key, foundKey, compareStrategy) {
		return
	}

	switch {
	case compareStrategy > 0:
		{
			if cursor.HasNext() {
				cursor.Next()
			} else {
				cursor = nil
			}
		}

	case compareStrategy < 0:
		{

			if cursor.HasPrev() {
				cursor.Prev()
			} else {
				cursor = nil
			}
		}
	}

	return
}

func (scanner *BTreeScanner) seekLessOrEqual(key []byte) *BTreeCursor {
	tree := scanner.tree
	cursor := &BTreeCursor{tree: tree}

	for parentPointer := tree.root; parentPointer != NULL_NODE; {
		parent := tree.storage.Get(parentPointer)
		lessOrEqualNodePointer := tree.getLessOrEqualKeyPosition(parent, key)

		cursor.path = append(cursor.path, &NodePosition{parent, lessOrEqualNodePointer})

		if parent.getType() == BNODE_PARENT {
			parentPointer = parent.getChildPointer(lessOrEqualNodePointer)
		} else {
			parentPointer = NULL_NODE
		}
	}

	return cursor
}

func (scanner *BTreeScanner) compareKeys(key []byte, foundKey []byte, compareStrategy int) bool {
	compareResult := bytes.Compare(foundKey, key)

	switch compareStrategy {
	case GREATER_OR_EQUAL_COMPARISON:
		return compareResult >= 0
	case GREATER_COMPARISON:
		return compareResult > 0
	case LESS_COMPARISON:
		return compareResult < 0
	case LESS_OR_EQUAL_COMPARISON:
		return compareResult <= 0
	default:
		panic(fmt.Sprintf("BTreeScanner doesn`t support comparison strategy %d", compareStrategy))
	}
}

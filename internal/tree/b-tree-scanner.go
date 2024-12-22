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

func (scanner *BTreeScanner) Seek(key []byte, compareStrategy int) (iterator *BTreeIterator) {
	iterator = scanner.seekLessOrEqual(key)
	foundKey, _ := iterator.Get()

	if scanner.compareKeys(key, foundKey, compareStrategy) {
		return
	}

	switch {
	case compareStrategy > 0:
		{
			if iterator.HasNext() {
				iterator.Next()
			} else {
				iterator = nil
			}
		}

	case compareStrategy < 0:
		{

			if iterator.HasPrev() {
				iterator.Prev()
			} else {
				iterator = nil
			}
		}
	}

	return
}

func (scanner *BTreeScanner) seekLessOrEqual(key []byte) *BTreeIterator {
	tree := scanner.tree
	iterator := &BTreeIterator{tree: tree}

	for parentPointer := tree.root; parentPointer != NULL_NODE; {
		parent := tree.storage.Get(parentPointer)
		lessOrEqualNodePointer := tree.getLessOrEqualKeyPosition(parent, key)

		iterator.path = append(iterator.path, &NodePosition{parent, lessOrEqualNodePointer})

		if parent.getType() == BNODE_PARENT {
			parentPointer = parent.getChildPointer(lessOrEqualNodePointer)
		} else {
			parentPointer = NULL_NODE
		}
	}

	return iterator
}

func (scanner *BTreeScanner) compareKeys(key []byte, foundKey []byte, compareStrategy int) bool {
	compareResult := bytes.Compare(key, foundKey)

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
		panic(fmt.Sprintf("Explorer doesn`t support comparison strategy %d", compareStrategy))
	}
}

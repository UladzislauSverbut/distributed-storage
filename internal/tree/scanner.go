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

type Scanner struct {
	tree *Tree
}

func NewScanner(tree *Tree) *Scanner {
	return &Scanner{tree: tree}
}

func (scanner *Scanner) Seek(key []byte, compareStrategy int) (cursor *Cursor) {
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

func (scanner *Scanner) seekLessOrEqual(key []byte) *Cursor {
	tree := scanner.tree
	cursor := &Cursor{tree: tree}

	for parentPointer := tree.root; parentPointer != NULL_NODE; {
		parent := &Node{data: tree.allocator.Page(parentPointer)}

		lessOrEqualNodePointer := tree.getLessOrEqualKeyPosition(parent, key)

		cursor.path = append(cursor.path, &NodePosition{parent, lessOrEqualNodePointer})

		if parent.getType() == NODE_PARENT {
			parentPointer = parent.getChildPointer(lessOrEqualNodePointer)
		} else {
			parentPointer = NULL_NODE
		}
	}

	return cursor
}

func (scanner *Scanner) compareKeys(key []byte, foundKey []byte, compareStrategy int) bool {
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
		panic(fmt.Sprintf("Scanner: comparison strategy %d is not supported", compareStrategy))
	}
}

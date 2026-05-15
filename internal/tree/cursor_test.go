package tree

import (
	"bytes"
	"fmt"
	"testing"
)

// --- Cursor.Prev ---

func TestCursor_Prev_NilCursor_ReturnsNil(t *testing.T) {
	var c *Cursor
	k, v := c.Prev()
	if k != nil || v != nil {
		t.Errorf("expected nil, nil from nil cursor; got %q, %q", k, v)
	}
}

func TestCursor_Prev_EmptyCursor_ReturnsNil(t *testing.T) {
	c := &Cursor{}
	k, v := c.Prev()
	if k != nil || v != nil {
		t.Errorf("expected nil, nil from empty-path cursor; got %q, %q", k, v)
	}
}

func TestCursor_Prev_AtFirstElement_ReturnsNil(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})

	cursor := NewScanner(tr).Seek([]byte("a"), GREATER_OR_EQUAL_COMPARISON)
	k, v := cursor.Prev()
	if k != nil || v != nil {
		t.Errorf("expected nil, nil at first element; got %q, %q", k, v)
	}
}

// Prev() moves the cursor one step left and then one step right via Next(),
// so the cursor ends up at the original position and returns that position's
// key-value pair.
func TestCursor_Prev_WhenHasPrev_ReturnsCurrent(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})

	cursor := NewScanner(tr).Seek([]byte("b"), GREATER_OR_EQUAL_COMPARISON)
	k, v := cursor.Prev()
	if !bytes.Equal(k, []byte("b")) {
		t.Errorf("expected key %q from Prev, got %q", "b", k)
	}
	if !bytes.Equal(v, []byte("2")) {
		t.Errorf("expected value %q from Prev, got %q", "2", v)
	}
}

func TestCursor_Prev_CursorPositionUnchangedAfterCall(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})

	cursor := NewScanner(tr).Seek([]byte("c"), GREATER_OR_EQUAL_COMPARISON)
	cursor.Prev()

	k, _ := cursor.Current()
	if !bytes.Equal(k, []byte("c")) {
		t.Errorf("expected cursor to remain at %q after Prev, got %q", "c", k)
	}
}

// Verify that multiple Prev calls keep the cursor at the same position each time.
func TestCursor_Prev_MultipleCalls_CursorStaysAtSamePosition(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})

	cursor := NewScanner(tr).Seek([]byte("b"), GREATER_OR_EQUAL_COMPARISON)
	for i := 0; i < 3; i++ {
		cursor.Prev()
	}
	k, _ := cursor.Current()
	if !bytes.Equal(k, []byte("b")) {
		t.Errorf("expected cursor to remain at %q after repeated Prev, got %q", "b", k)
	}
}

// --- Cursor.HasNext / HasPrev edge cases ---

func TestCursor_HasNext_FalseAtLastKey(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"x", "1"}, {"y", "2"}, {"z", "3"}})

	cursor := NewScanner(tr).Seek([]byte("z"), GREATER_OR_EQUAL_COMPARISON)
	if cursor.HasNext() {
		t.Error("expected HasNext=false at last key")
	}
}

func TestCursor_HasPrev_TrueAfterNextAdvance(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})

	cursor := NewScanner(tr).Seek(nil, GREATER_OR_EQUAL_COMPARISON)
	if cursor.HasPrev() {
		t.Fatal("expected HasPrev=false at first element before advancing")
	}
	cursor.Next() // advance to "b"
	if !cursor.HasPrev() {
		t.Error("expected HasPrev=true after advancing past first element")
	}
}

// --- Cursor across a split tree (multi-level) ---

func TestCursor_HasNext_FalseAtLastKeyAfterSplit(t *testing.T) {
	tr := newTestTree()
	for i := 1; i <= 50; i++ {
		treeSet(t, tr, fmt.Sprintf("k%03d", i), "v")
	}
	cursor := NewScanner(tr).Seek([]byte("k050"), GREATER_OR_EQUAL_COMPARISON)
	if cursor.HasNext() {
		t.Error("expected HasNext=false at last key of a split tree")
	}
}

func TestCursor_HasPrev_FalseAtFirstKeyAfterSplit(t *testing.T) {
	tr := newTestTree()
	for i := 1; i <= 50; i++ {
		treeSet(t, tr, fmt.Sprintf("k%03d", i), "v")
	}
	cursor := NewScanner(tr).Seek([]byte("k001"), GREATER_OR_EQUAL_COMPARISON)
	if cursor.HasPrev() {
		t.Error("expected HasPrev=false at first key of a split tree")
	}
}

func TestCursor_Next_TraversesAllKeysAfterSplit(t *testing.T) {
	tr := newTestTree()
	const n = 50
	for i := 1; i <= n; i++ {
		treeSet(t, tr, fmt.Sprintf("k%03d", i), "v")
	}
	cursor := NewScanner(tr).Seek(nil, GREATER_OR_EQUAL_COMPARISON)
	count := 0
	for k, v := cursor.Current(); v != nil; k, v = cursor.Next() {
		_ = k
		count++
	}
	if count != n {
		t.Errorf("expected %d keys traversed, got %d", n, count)
	}
}

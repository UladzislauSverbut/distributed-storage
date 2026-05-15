package tree

import (
	"bytes"
	"fmt"
	"testing"
)

// seedTree inserts the given key-value pairs (keys must be pre-sorted or in
// any order; the tree handles ordering internally).
func seedTree(t *testing.T, tr *Tree, kvs [][2]string) {
	t.Helper()
	for _, kv := range kvs {
		treeSet(t, tr, kv[0], kv[1])
	}
}

// allKeys drains a cursor forwards from its current position, returning all
// keys in order.  The cursor must not be nil.
func allKeys(cursor *Cursor) []string {
	var keys []string
	for k, v := cursor.Current(); v != nil; k, v = cursor.Next() {
		keys = append(keys, string(k))
	}
	return keys
}

// --- Cursor.Empty ---

func TestCursor_Empty_NilCursor(t *testing.T) {
	var c *Cursor
	if !c.Empty() {
		t.Error("expected nil cursor to be Empty")
	}
}

func TestCursor_Empty_EmptyPath(t *testing.T) {
	c := &Cursor{}
	if !c.Empty() {
		t.Error("expected cursor with empty path to be Empty")
	}
}

func TestCursor_Empty_False_AfterSeek(t *testing.T) {
	tr := newTestTree()
	treeSet(t, tr, "a", "1")

	scanner := NewScanner(tr)
	cursor := scanner.Seek([]byte("a"), GREATER_OR_EQUAL_COMPARISON)
	if cursor.Empty() {
		t.Error("expected non-empty cursor after Seek found a match")
	}
}

// --- Cursor.Current ---

func TestCursor_Current_NilCursor_ReturnsNil(t *testing.T) {
	var c *Cursor
	k, v := c.Current()
	if k != nil || v != nil {
		t.Errorf("expected nil, nil from nil cursor; got %v, %v", k, v)
	}
}

func TestCursor_Current_ReturnsKeyAndValue(t *testing.T) {
	tr := newTestTree()
	treeSet(t, tr, "foo", "bar")

	scanner := NewScanner(tr)
	cursor := scanner.Seek([]byte("foo"), GREATER_OR_EQUAL_COMPARISON)
	if cursor == nil {
		t.Fatal("expected non-nil cursor")
	}

	k, v := cursor.Current()
	if !bytes.Equal(k, []byte("foo")) {
		t.Errorf("expected key %q, got %q", "foo", k)
	}
	if !bytes.Equal(v, []byte("bar")) {
		t.Errorf("expected value %q, got %q", "bar", v)
	}
}

// --- Cursor.HasNext / HasPrev ---

func TestCursor_HasNext_FalseOnSingleElement(t *testing.T) {
	tr := newTestTree()
	treeSet(t, tr, "only", "v")

	cursor := NewScanner(tr).Seek([]byte("only"), GREATER_OR_EQUAL_COMPARISON)
	if cursor.HasNext() {
		t.Error("expected HasNext==false on single-element tree")
	}
}

func TestCursor_HasNext_TrueWhenMoreFollow(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})

	cursor := NewScanner(tr).Seek([]byte("a"), GREATER_OR_EQUAL_COMPARISON)
	if !cursor.HasNext() {
		t.Error("expected HasNext==true when positioned at first of three elements")
	}
}

func TestCursor_HasPrev_FalseAtFirstElement(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})

	cursor := NewScanner(tr).Seek([]byte("a"), GREATER_OR_EQUAL_COMPARISON)
	if cursor.HasPrev() {
		t.Error("expected HasPrev==false at first element")
	}
}

func TestCursor_HasPrev_TrueAfterAdvancing(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})

	cursor := NewScanner(tr).Seek([]byte("b"), GREATER_OR_EQUAL_COMPARISON)
	if !cursor.HasPrev() {
		t.Error("expected HasPrev==true when not at first element")
	}
}

// --- Cursor.Next ---

func TestCursor_Next_AdvancesToNextKey(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})

	cursor := NewScanner(tr).Seek([]byte("a"), GREATER_OR_EQUAL_COMPARISON)
	k, v := cursor.Next()
	if !bytes.Equal(k, []byte("b")) {
		t.Errorf("expected key %q after Next, got %q", "b", k)
	}
	if !bytes.Equal(v, []byte("2")) {
		t.Errorf("expected value %q after Next, got %q", "2", v)
	}
}

func TestCursor_Next_AtLastElement_ReturnsNil(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"b", "2"}})

	cursor := NewScanner(tr).Seek([]byte("b"), GREATER_OR_EQUAL_COMPARISON)
	k, v := cursor.Next()
	if k != nil || v != nil {
		t.Errorf("expected nil, nil from Next at last element; got %q, %q", k, v)
	}
}

func TestCursor_Iterate_ForwardAll(t *testing.T) {
	tr := newTestTree()
	ordered := [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}, {"e", "5"}}
	seedTree(t, tr, ordered)

	cursor := NewScanner(tr).Seek(nil, GREATER_OR_EQUAL_COMPARISON)
	got := allKeys(cursor)

	want := []string{"a", "b", "c", "d", "e"}
	if len(got) != len(want) {
		t.Fatalf("expected %d keys, got %d: %v", len(want), len(got), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("position %d: expected %q, got %q", i, w, got[i])
		}
	}
}

// --- Scanner.Seek ---

func TestScanner_Seek_EmptyTree_GreaterOrEqual_ReturnsNil(t *testing.T) {
	tr := newTestTree()
	cursor := NewScanner(tr).Seek([]byte("k"), GREATER_OR_EQUAL_COMPARISON)
	if cursor != nil {
		t.Error("expected nil cursor from Seek on empty tree with GREATER_OR_EQUAL")
	}
}

func TestScanner_Seek_ExactMatch_GreaterOrEqual(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"c", "3"}, {"e", "5"}})

	cursor := NewScanner(tr).Seek([]byte("c"), GREATER_OR_EQUAL_COMPARISON)
	if cursor == nil {
		t.Fatal("expected non-nil cursor")
	}
	k, _ := cursor.Current()
	if !bytes.Equal(k, []byte("c")) {
		t.Errorf("expected key %q, got %q", "c", k)
	}
}

func TestScanner_Seek_BetweenKeys_GreaterOrEqual_FindsNext(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"c", "3"}, {"e", "5"}})

	// "b" is not in the tree; GREATER_OR_EQUAL should land on "c".
	cursor := NewScanner(tr).Seek([]byte("b"), GREATER_OR_EQUAL_COMPARISON)
	if cursor == nil {
		t.Fatal("expected non-nil cursor")
	}
	k, _ := cursor.Current()
	if !bytes.Equal(k, []byte("c")) {
		t.Errorf("expected key %q, got %q", "c", k)
	}
}

func TestScanner_Seek_BeyondAllKeys_GreaterOrEqual_ReturnsNil(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"c", "3"}, {"e", "5"}})

	// "z" > all keys, no element satisfies >= "z".
	cursor := NewScanner(tr).Seek([]byte("z"), GREATER_OR_EQUAL_COMPARISON)
	if cursor != nil {
		t.Errorf("expected nil cursor for key beyond all stored keys, got cursor at %q",
			func() string { k, _ := cursor.Current(); return string(k) }())
	}
}

func TestScanner_Seek_NilKey_GreaterOrEqual_StartsAtFirst(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"b", "2"}, {"d", "4"}, {"f", "6"}})

	// nil key is less than any stored key, so GREATER_OR_EQUAL returns the first.
	cursor := NewScanner(tr).Seek(nil, GREATER_OR_EQUAL_COMPARISON)
	if cursor == nil {
		t.Fatal("expected non-nil cursor")
	}
	k, _ := cursor.Current()
	if !bytes.Equal(k, []byte("b")) {
		t.Errorf("expected first key %q, got %q", "b", k)
	}
}

func TestScanner_Seek_ExactMatch_LessOrEqual(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"c", "3"}, {"e", "5"}})

	cursor := NewScanner(tr).Seek([]byte("c"), LESS_OR_EQUAL_COMPARISON)
	if cursor == nil {
		t.Fatal("expected non-nil cursor")
	}
	k, _ := cursor.Current()
	if !bytes.Equal(k, []byte("c")) {
		t.Errorf("expected key %q, got %q", "c", k)
	}
}

func TestScanner_Seek_BetweenKeys_LessOrEqual_FindsPrev(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"c", "3"}, {"e", "5"}})

	// "b" is not in the tree; LESS_OR_EQUAL should land on "a".
	cursor := NewScanner(tr).Seek([]byte("b"), LESS_OR_EQUAL_COMPARISON)
	if cursor == nil {
		t.Fatal("expected non-nil cursor")
	}
	k, _ := cursor.Current()
	if !bytes.Equal(k, []byte("a")) {
		t.Errorf("expected key %q, got %q", "a", k)
	}
}

func TestScanner_Seek_LargestKey_GreaterOrEqual_FindsIt(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"c", "3"}, {"e", "5"}})

	cursor := NewScanner(tr).Seek([]byte("e"), GREATER_OR_EQUAL_COMPARISON)
	if cursor == nil {
		t.Fatal("expected non-nil cursor")
	}
	k, _ := cursor.Current()
	if !bytes.Equal(k, []byte("e")) {
		t.Errorf("expected key %q, got %q", "e", k)
	}
}

func TestScanner_Seek_GreaterComparison_ExactMatch_AdvancesOne(t *testing.T) {
	tr := newTestTree()
	seedTree(t, tr, [][2]string{{"a", "1"}, {"c", "3"}, {"e", "5"}})

	// GREATER_COMPARISON on "c" should land on "e" (strictly greater).
	cursor := NewScanner(tr).Seek([]byte("c"), GREATER_COMPARISON)
	if cursor == nil {
		t.Fatal("expected non-nil cursor")
	}
	k, _ := cursor.Current()
	if !bytes.Equal(k, []byte("e")) {
		t.Errorf("expected key %q, got %q", "e", k)
	}
}

func TestScanner_Seek_ScanAll_IteratesEntireTree(t *testing.T) {
	tr := newTestTree()
	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for _, k := range keys {
		treeSet(t, tr, k, k)
	}

	cursor := NewScanner(tr).Seek(nil, GREATER_OR_EQUAL_COMPARISON)
	got := allKeys(cursor)

	if len(got) != len(keys) {
		t.Fatalf("expected %d keys, got %d: %v", len(keys), len(got), got)
	}
	for i, want := range keys {
		if got[i] != want {
			t.Errorf("position %d: expected %q, got %q", i, want, got[i])
		}
	}
}

func TestScanner_Seek_GreaterOrEqual_AfterNodeSplit(t *testing.T) {
	tr := newTestTree()
	const n = 200
	for i := 1; i <= n; i++ {
		treeSet(t, tr, fmt.Sprintf("key%04d", i), fmt.Sprintf("val%04d", i))
	}

	// Seek from the middle of the range and count remaining elements.
	startKey := []byte("key0101")
	cursor := NewScanner(tr).Seek(startKey, GREATER_OR_EQUAL_COMPARISON)
	if cursor == nil {
		t.Fatal("expected non-nil cursor after split")
	}
	k, _ := cursor.Current()
	if !bytes.Equal(k, startKey) {
		t.Errorf("expected start key %q, got %q", startKey, k)
	}

	count := 0
	for k2, v := cursor.Current(); v != nil; k2, v = cursor.Next() {
		_ = k2
		count++
	}
	// keys 101..200 = 100 keys
	if count != 100 {
		t.Errorf("expected 100 keys from key0101 onwards, got %d", count)
	}
}

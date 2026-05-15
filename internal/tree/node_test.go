package tree

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// newTestLeafNode allocates a Node with NODE_LEAF type and the given key
// capacity.  The data buffer is sized generously so that test keys and values
// fit without overflow.
func newTestLeafNode(capacity uint16) *Node {
	dataSize := HEADER_SIZE + int(capacity)*10 + int(capacity)*128
	n := &Node{data: make([]byte, dataSize)}
	n.setHeader(NODE_LEAF, capacity)
	return n
}

// newTestParentNode allocates a Node with NODE_PARENT type and the given key
// capacity.
func newTestParentNode(capacity uint16) *Node {
	dataSize := HEADER_SIZE + int(capacity)*10 + int(capacity)*128
	n := &Node{data: make([]byte, dataSize)}
	n.setHeader(NODE_PARENT, capacity)
	return n
}

// assertPanics fails the test if fn does not panic.
func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("expected panic but function did not panic")
		}
	}()
	fn()
}

// --- setHeader / getType / getStoredKeysNumber ---

func TestNode_SetHeader_GetType_Leaf(t *testing.T) {
	n := newTestLeafNode(3)
	if n.getType() != NODE_LEAF {
		t.Errorf("expected NODE_LEAF, got %d", n.getType())
	}
}

func TestNode_SetHeader_GetType_Parent(t *testing.T) {
	n := newTestParentNode(3)
	if n.getType() != NODE_PARENT {
		t.Errorf("expected NODE_PARENT, got %d", n.getType())
	}
}

func TestNode_GetStoredKeysNumber_ReturnsCapacity(t *testing.T) {
	n := newTestLeafNode(5)
	if n.getStoredKeysNumber() != 5 {
		t.Errorf("expected 5, got %d", n.getStoredKeysNumber())
	}
}

// --- appendKeyValue / getKey / getValue ---

func TestNode_AppendKeyValue_GetKey(t *testing.T) {
	n := newTestLeafNode(3)
	n.appendKeyValue([]byte("hello"), []byte("world"))

	got := n.getKey(0)
	if !bytes.Equal(got, []byte("hello")) {
		t.Errorf("expected key %q, got %q", "hello", got)
	}
}

func TestNode_AppendKeyValue_GetValue(t *testing.T) {
	n := newTestLeafNode(3)
	n.appendKeyValue([]byte("hello"), []byte("world"))

	got := n.getValue(0)
	if !bytes.Equal(got, []byte("world")) {
		t.Errorf("expected value %q, got %q", "world", got)
	}
}

func TestNode_AppendKeyValue_MultipleKeys_GetAllKeyValues(t *testing.T) {
	n := newTestLeafNode(3)
	pairs := [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}}
	for _, p := range pairs {
		n.appendKeyValue([]byte(p[0]), []byte(p[1]))
	}
	for i, p := range pairs {
		if got := n.getKey(NodeKeyPosition(i)); !bytes.Equal(got, []byte(p[0])) {
			t.Errorf("position %d: expected key %q, got %q", i, p[0], got)
		}
		if got := n.getValue(NodeKeyPosition(i)); !bytes.Equal(got, []byte(p[1])) {
			t.Errorf("position %d: expected value %q, got %q", i, p[1], got)
		}
	}
}

func TestNode_AppendKeyValue_Full_Panics(t *testing.T) {
	n := newTestLeafNode(2)
	n.appendKeyValue([]byte("a"), []byte("1"))
	n.appendKeyValue([]byte("b"), []byte("2"))
	assertPanics(t, func() {
		n.appendKeyValue([]byte("c"), []byte("3"))
	})
}

func TestNode_GetKey_EmptyNode_ReturnsNil(t *testing.T) {
	// Node with capacity 0 — getStoredKeysNumber() == 0 triggers the nil guard.
	n := &Node{data: make([]byte, HEADER_SIZE)}
	n.setHeader(NODE_LEAF, 0)
	if k := n.getKey(0); k != nil {
		t.Errorf("expected nil for empty node, got %q", k)
	}
}

func TestNode_GetKey_OutOfRange_Panics(t *testing.T) {
	n := newTestLeafNode(1)
	n.appendKeyValue([]byte("x"), []byte("y"))
	assertPanics(t, func() {
		n.getKey(1) // valid range is [0, 0]
	})
}

func TestNode_GetValue_OutOfRange_Panics(t *testing.T) {
	n := newTestLeafNode(1)
	n.appendKeyValue([]byte("x"), []byte("y"))
	assertPanics(t, func() {
		n.getValue(1) // valid range is [0, 0]
	})
}

// --- getChildPointer / setChildPointer ---

func TestNode_SetChildPointer_GetChildPointer(t *testing.T) {
	n := newTestParentNode(3)
	// Populate a slot so getChildPointer can read it.
	n.appendKeyValue([]byte("k"), nil)
	n.setChildPointer(0, 42)

	if got := n.getChildPointer(0); got != 42 {
		t.Errorf("expected child pointer 42, got %d", got)
	}
}

func TestNode_SetChildPointer_MultipleSlots(t *testing.T) {
	n := newTestParentNode(3)
	for i := 0; i < 3; i++ {
		n.appendKeyValue([]byte("k"), nil)
	}
	n.setChildPointer(0, 10)
	n.setChildPointer(1, 20)
	n.setChildPointer(2, 30)

	if n.getChildPointer(0) != 10 {
		t.Errorf("expected 10 at position 0, got %d", n.getChildPointer(0))
	}
	if n.getChildPointer(1) != 20 {
		t.Errorf("expected 20 at position 1, got %d", n.getChildPointer(1))
	}
	if n.getChildPointer(2) != 30 {
		t.Errorf("expected 30 at position 2, got %d", n.getChildPointer(2))
	}
}

func TestNode_GetChildPointer_OutOfRange_Panics(t *testing.T) {
	n := newTestParentNode(2)
	assertPanics(t, func() {
		n.getChildPointer(2) // valid range is [0, 1]
	})
}

func TestNode_SetChildPointer_OutOfRange_Panics(t *testing.T) {
	n := newTestParentNode(2)
	assertPanics(t, func() {
		n.setChildPointer(2, 99) // valid range is [0, 1]
	})
}

// --- appendPointer ---

func TestNode_AppendPointer_SetsKeyAndChildPointer(t *testing.T) {
	n := newTestParentNode(2)
	n.appendPointer([]byte("pivot"), NodePointer(77))

	if got := n.getKey(0); !bytes.Equal(got, []byte("pivot")) {
		t.Errorf("expected key %q, got %q", "pivot", got)
	}
	if got := n.getChildPointer(0); got != 77 {
		t.Errorf("expected child pointer 77, got %d", got)
	}
}

// --- size ---

func TestNode_Size_EmptySlots_EqualsHeader(t *testing.T) {
	// With no key-value data appended, the stored size should equal the header
	// plus the pointer/offset sections (no data bytes yet).
	n := newTestLeafNode(3)
	want := uint16(HEADER_SIZE + 3*10) // 4 + 30 = 34
	if got := n.size(); got != want {
		t.Errorf("expected size %d, got %d", want, got)
	}
}

func TestNode_Size_IncreasesAfterAppend(t *testing.T) {
	// size() reads the end-marker offset that is only written when the LAST
	// slot of the node is filled.  Use capacity=1 so a single append triggers
	// the update.
	n := newTestLeafNode(1)
	before := n.size()
	n.appendKeyValue([]byte("hello"), []byte("world"))
	if n.size() <= before {
		t.Errorf("expected size to increase after append: before=%d after=%d", before, n.size())
	}
}

func TestNode_Size_ReflectsAllAppendedData(t *testing.T) {
	n := newTestLeafNode(2)
	n.appendKeyValue([]byte("ab"), []byte("cd")) // 4+2+2 = 8 data bytes
	n.appendKeyValue([]byte("ef"), []byte("gh")) // 4+2+2 = 8 data bytes
	// Total data = 16 bytes; size = HEADER_SIZE + 2*10 + 16 = 40
	want := uint16(HEADER_SIZE + 2*10 + 16)
	if got := n.size(); got != want {
		t.Errorf("expected size %d, got %d", want, got)
	}
}

// --- getAvailableKeyPosition ---

func TestNode_GetAvailableKeyPosition_EmptyNode_ReturnsZero(t *testing.T) {
	n := newTestLeafNode(3)
	if pos := n.getAvailableKeyPosition(); pos != 0 {
		t.Errorf("expected position 0, got %d", pos)
	}
}

func TestNode_GetAvailableKeyPosition_PartiallyFilled(t *testing.T) {
	n := newTestLeafNode(3)
	n.appendKeyValue([]byte("a"), []byte("1"))
	if pos := n.getAvailableKeyPosition(); pos != 1 {
		t.Errorf("expected position 1 after one append, got %d", pos)
	}
}

func TestNode_GetAvailableKeyPosition_Full_ReturnsCapacity(t *testing.T) {
	n := newTestLeafNode(3)
	n.appendKeyValue([]byte("a"), []byte("1"))
	n.appendKeyValue([]byte("b"), []byte("2"))
	n.appendKeyValue([]byte("c"), []byte("3"))
	if pos := n.getAvailableKeyPosition(); pos != 3 {
		t.Errorf("expected position 3 (capacity) when full, got %d", pos)
	}
}

// --- getKeyValueOffset / setKeyValueOffset ---

func TestNode_GetKeyValueOffset_PositionZero_AlwaysZero(t *testing.T) {
	n := newTestLeafNode(3)
	n.appendKeyValue([]byte("x"), []byte("y"))
	if off := n.getKeyValueOffset(0); off != 0 {
		t.Errorf("expected offset 0 at position 0, got %d", off)
	}
}

func TestNode_SetKeyValueOffset_PositionZero_Panics(t *testing.T) {
	n := newTestLeafNode(3)
	assertPanics(t, func() {
		n.setKeyValueOffset(0, 10)
	})
}

func TestNode_SetKeyValueOffset_BeyondCapacity_Panics(t *testing.T) {
	n := newTestLeafNode(2) // capacity 2; valid setKeyValueOffset positions: 1, 2
	assertPanics(t, func() {
		n.setKeyValueOffset(3, 10) // 3 > capacity 2
	})
}

// --- convertKeyValueOffsetToAddress ---

func TestNode_ConvertKeyValueOffsetToAddress_ZeroOffset(t *testing.T) {
	n := newTestLeafNode(3)
	// Formula: HEADER_SIZE + (8+2)*capacity + offset = 4 + 30 + 0 = 34
	want := uint16(HEADER_SIZE + (8+2)*3 + 0)
	if got := n.convertKeyValueOffsetToAddress(0); got != want {
		t.Errorf("expected address %d, got %d", want, got)
	}
}

func TestNode_ConvertKeyValueOffsetToAddress_NonZeroOffset(t *testing.T) {
	n := newTestLeafNode(2)
	// HEADER_SIZE + (8+2)*2 + 6 = 4 + 20 + 6 = 30
	want := uint16(HEADER_SIZE + (8+2)*2 + 6)
	if got := n.convertKeyValueOffsetToAddress(6); got != want {
		t.Errorf("expected address %d, got %d", want, got)
	}
}

// --- copy ---

func TestNode_Copy_FullRange_AllKeysTransferred(t *testing.T) {
	src := newTestLeafNode(3)
	src.appendKeyValue([]byte("a"), []byte("1"))
	src.appendKeyValue([]byte("b"), []byte("2"))
	src.appendKeyValue([]byte("c"), []byte("3"))

	dst := newTestLeafNode(3)
	dst.copy(src, 0, 0, 3)

	for i, want := range []string{"a", "b", "c"} {
		if got := dst.getKey(NodeKeyPosition(i)); !bytes.Equal(got, []byte(want)) {
			t.Errorf("position %d: expected key %q, got %q", i, want, got)
		}
	}
}

func TestNode_Copy_PartialRange_CorrectKeysTransferred(t *testing.T) {
	src := newTestLeafNode(3)
	src.appendKeyValue([]byte("a"), []byte("1"))
	src.appendKeyValue([]byte("b"), []byte("2"))
	src.appendKeyValue([]byte("c"), []byte("3"))

	dst := newTestLeafNode(3)
	// Copy src[1..2] → dst[0..1].
	dst.copy(src, 1, 0, 2)

	if got := dst.getKey(0); !bytes.Equal(got, []byte("b")) {
		t.Errorf("expected key %q at dst[0], got %q", "b", got)
	}
	if got := dst.getKey(1); !bytes.Equal(got, []byte("c")) {
		t.Errorf("expected key %q at dst[1], got %q", "c", got)
	}
}

func TestNode_Copy_SourceOutOfRange_Panics(t *testing.T) {
	src := newTestLeafNode(3)
	src.appendKeyValue([]byte("a"), []byte("1"))

	dst := newTestLeafNode(3)
	assertPanics(t, func() {
		dst.copy(src, 3, 0, 1) // from=3, quantity=1 → 3+1=4 > 3
	})
}

func TestNode_Copy_DestOutOfRange_Panics(t *testing.T) {
	src := newTestLeafNode(3)
	src.appendKeyValue([]byte("a"), []byte("1"))
	src.appendKeyValue([]byte("b"), []byte("2"))
	src.appendKeyValue([]byte("c"), []byte("3"))

	dst := newTestLeafNode(3)
	assertPanics(t, func() {
		dst.copy(src, 0, 3, 1) // to=3, quantity=1 → 3+1=4 > 3
	})
}

func TestNode_Copy_PreservesChildPointers(t *testing.T) {
	src := newTestParentNode(2)
	src.appendKeyValue([]byte("k1"), nil)
	src.appendKeyValue([]byte("k2"), nil)
	src.setChildPointer(0, 100)
	src.setChildPointer(1, 200)

	dst := newTestParentNode(2)
	dst.copy(src, 0, 0, 2)

	if got := dst.getChildPointer(0); got != 100 {
		t.Errorf("expected child pointer 100 at dst[0], got %d", got)
	}
	if got := dst.getChildPointer(1); got != 200 {
		t.Errorf("expected child pointer 200 at dst[1], got %d", got)
	}
}

// --- header encoding is little-endian ---

func TestNode_HeaderEncoding_IsLittleEndian(t *testing.T) {
	n := newTestLeafNode(0)
	// setHeader writes type=NODE_LEAF(1) and numberOfKeys=0.
	// NODE_LEAF == 1 in little-endian: data[0]=1, data[1]=0.
	if n.data[0] != 1 || n.data[1] != 0 {
		t.Errorf("expected little-endian type bytes [1 0], got [%d %d]", n.data[0], n.data[1])
	}
}

func TestNode_GetStoredKeysNumber_LargeValue(t *testing.T) {
	n := &Node{data: make([]byte, HEADER_SIZE)}
	binary.LittleEndian.PutUint16(n.data[2:4], 1000)
	if got := n.getStoredKeysNumber(); got != 1000 {
		t.Errorf("expected 1000, got %d", got)
	}
}

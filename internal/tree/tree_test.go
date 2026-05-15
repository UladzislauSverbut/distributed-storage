package tree

import (
	"bytes"
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	"fmt"
	"testing"
)

const treePageSize = 4096
const treeMaxKeySize = 64
const treeMaxValueSize = 256

func newTestTree() *Tree {
	storage := store.NewMemoryStorage(treePageSize * 512)
	//Pager start with 1 page, because one page is always reserved internal purpose
	p := pager.NewPager(storage, 1, treePageSize)
	return NewTree(NULL_NODE, p, TreeConfig{
		PageSize:     treePageSize,
		MaxKeySize:   treeMaxKeySize,
		MaxValueSize: treeMaxValueSize,
	})
}

func treeSet(t *testing.T, tr *Tree, key, value string) {
	t.Helper()
	if _, err := tr.Set([]byte(key), []byte(value)); err != nil {
		t.Fatalf("Set(%q, %q): %v", key, value, err)
	}
}

func treeGet(t *testing.T, tr *Tree, key string) []byte {
	t.Helper()
	v, err := tr.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return v
}

// --- Root ---

func TestTree_Root_IsNullBeforeAnySet(t *testing.T) {
	tr := newTestTree()
	if tr.Root() != pager.NULL_PAGE {
		t.Errorf("expected NULL_PAGE root before any Set, got %d", tr.Root())
	}
}

func TestTree_Root_IsNonNullAfterFirstSet(t *testing.T) {
	tr := newTestTree()
	treeSet(t, tr, "k", "v")
	if tr.Root() == pager.NULL_PAGE {
		t.Error("expected non-null root after first Set")
	}
}

// --- Get ---

func TestTree_Get_EmptyTree_ReturnsNil(t *testing.T) {
	tr := newTestTree()
	got, err := tr.Get([]byte("any"))
	if err != nil {
		t.Fatalf("Get on empty tree: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil from empty tree, got %v", got)
	}
}

func TestTree_Get_ExistingKey_ReturnsValue(t *testing.T) {
	tr := newTestTree()
	treeSet(t, tr, "hello", "world")

	got := treeGet(t, tr, "hello")
	if !bytes.Equal(got, []byte("world")) {
		t.Errorf("expected %q, got %q", "world", got)
	}
}

func TestTree_Get_NonExistingKey_ReturnsNil(t *testing.T) {
	tr := newTestTree()
	treeSet(t, tr, "hello", "world")

	got := treeGet(t, tr, "missing")
	if got != nil {
		t.Errorf("expected nil for missing key, got %q", got)
	}
}

func TestTree_Get_KeyTooLarge_ReturnsError(t *testing.T) {
	tr := newTestTree()
	bigKey := make([]byte, treeMaxKeySize+1)
	_, err := tr.Get(bigKey)
	if err == nil {
		t.Fatal("expected error for key exceeding MaxKeySize")
	}
}

// --- Set ---

func TestTree_Set_InsertNewKey_ReturnsNilOldValue(t *testing.T) {
	tr := newTestTree()
	old, err := tr.Set([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if old != nil {
		t.Errorf("expected nil old value on insert, got %q", old)
	}
}

func TestTree_Set_UpdateExistingKey_ReturnsOldValue(t *testing.T) {
	tr := newTestTree()
	treeSet(t, tr, "k", "v1")

	old, err := tr.Set([]byte("k"), []byte("v2"))
	if err != nil {
		t.Fatalf("Set (update): %v", err)
	}
	if !bytes.Equal(old, []byte("v1")) {
		t.Errorf("expected old value %q, got %q", "v1", old)
	}
}

func TestTree_Set_UpdateExistingKey_GetReturnsNewValue(t *testing.T) {
	tr := newTestTree()
	treeSet(t, tr, "k", "v1")
	treeSet(t, tr, "k", "v2")

	got := treeGet(t, tr, "k")
	if !bytes.Equal(got, []byte("v2")) {
		t.Errorf("expected updated value %q, got %q", "v2", got)
	}
}

func TestTree_Set_KeyTooLarge_ReturnsError(t *testing.T) {
	tr := newTestTree()
	bigKey := make([]byte, treeMaxKeySize+1)
	_, err := tr.Set(bigKey, []byte("v"))
	if err == nil {
		t.Fatal("expected error for key exceeding MaxKeySize")
	}
}

func TestTree_Set_ValueTooLarge_ReturnsError(t *testing.T) {
	tr := newTestTree()
	bigVal := make([]byte, treeMaxValueSize+1)
	_, err := tr.Set([]byte("k"), bigVal)
	if err == nil {
		t.Fatal("expected error for value exceeding MaxValueSize")
	}
}

// --- Delete ---

func TestTree_Delete_EmptyTree_ReturnsNil(t *testing.T) {
	tr := newTestTree()
	old, err := tr.Delete([]byte("k"))
	if err != nil {
		t.Fatalf("Delete on empty tree: %v", err)
	}
	if old != nil {
		t.Errorf("expected nil from empty tree, got %v", old)
	}
}

func TestTree_Delete_ExistingKey_ReturnsOldValue(t *testing.T) {
	tr := newTestTree()
	treeSet(t, tr, "k", "v")
	treeSet(t, tr, "k2", "v2") // keep the tree non-empty after deletion

	old, err := tr.Delete([]byte("k"))
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !bytes.Equal(old, []byte("v")) {
		t.Errorf("expected old value %q, got %q", "v", old)
	}
}

func TestTree_Delete_ExistingKey_GetReturnsNil(t *testing.T) {
	tr := newTestTree()
	treeSet(t, tr, "k", "v")
	treeSet(t, tr, "k2", "v2")

	if _, err := tr.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got := treeGet(t, tr, "k")
	if got != nil {
		t.Errorf("expected nil after delete, got %q", got)
	}
}

func TestTree_Delete_NonExistingKey_ReturnsNil(t *testing.T) {
	tr := newTestTree()
	treeSet(t, tr, "k", "v")

	old, err := tr.Delete([]byte("missing"))
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if old != nil {
		t.Errorf("expected nil for non-existing key, got %v", old)
	}
}

func TestTree_Delete_KeyTooLarge_ReturnsError(t *testing.T) {
	tr := newTestTree()
	bigKey := make([]byte, treeMaxKeySize+1)
	_, err := tr.Delete(bigKey)
	if err == nil {
		t.Fatal("expected error for key exceeding MaxKeySize")
	}
}

// Get after deleting the only key is safe because getKeyValue guards against
// an empty root node (returns nil on storedKeysNumber == 0).
func TestTree_Delete_AllKeys_GetReturnsNil(t *testing.T) {
	tr := newTestTree()
	treeSet(t, tr, "k", "v")

	if _, err := tr.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got := treeGet(t, tr, "k")
	if got != nil {
		t.Errorf("expected nil after deleting last key, got %q", got)
	}
}

// --- Multi-key scenarios ---

func TestTree_MultipleKeys_AllRetrievable(t *testing.T) {
	tr := newTestTree()
	pairs := map[string]string{
		"alpha":   "1",
		"beta":    "2",
		"gamma":   "3",
		"delta":   "4",
		"epsilon": "5",
	}
	for k, v := range pairs {
		treeSet(t, tr, k, v)
	}
	for k, want := range pairs {
		got := treeGet(t, tr, k)
		if !bytes.Equal(got, []byte(want)) {
			t.Errorf("key %q: expected %q, got %q", k, want, got)
		}
	}
}

func TestTree_Delete_OneOfManyKeys_OthersRemain(t *testing.T) {
	tr := newTestTree()
	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for _, k := range keys {
		treeSet(t, tr, k, k+"-value")
	}

	if _, err := tr.Delete([]byte("ccc")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if got := treeGet(t, tr, "ccc"); got != nil {
		t.Errorf("expected nil for deleted key, got %q", got)
	}
	for _, k := range []string{"aaa", "bbb", "ddd", "eee"} {
		if got := treeGet(t, tr, k); !bytes.Equal(got, []byte(k+"-value")) {
			t.Errorf("key %q: expected %q, got %q", k, k+"-value", got)
		}
	}
}

// TestTree_NodeSplit inserts enough keys to force internal B-tree page splits
// and verifies all entries remain retrievable.
func TestTree_NodeSplit_AllKeysRetrievable(t *testing.T) {
	tr := newTestTree()
	const n = 200

	for i := 1; i <= n; i++ {
		k := fmt.Sprintf("key%04d", i)
		v := fmt.Sprintf("val%04d", i)
		treeSet(t, tr, k, v)
	}

	for i := 1; i <= n; i++ {
		k := fmt.Sprintf("key%04d", i)
		want := fmt.Sprintf("val%04d", i)
		got := treeGet(t, tr, k)
		if !bytes.Equal(got, []byte(want)) {
			t.Errorf("after splits: key %q expected %q, got %q", k, want, got)
		}
	}
}

func TestTree_NodeSplit_UpdatesAfterSplits(t *testing.T) {
	tr := newTestTree()
	const n = 200

	for i := 1; i <= n; i++ {
		treeSet(t, tr, fmt.Sprintf("key%04d", i), fmt.Sprintf("val%04d", i))
	}

	// Update every other key.
	for i := 1; i <= n; i += 2 {
		k := fmt.Sprintf("key%04d", i)
		treeSet(t, tr, k, "updated")
	}

	for i := 1; i <= n; i++ {
		k := fmt.Sprintf("key%04d", i)
		got := treeGet(t, tr, k)
		var want string
		if i%2 == 1 {
			want = "updated"
		} else {
			want = fmt.Sprintf("val%04d", i)
		}
		if !bytes.Equal(got, []byte(want)) {
			t.Errorf("key %q: expected %q, got %q", k, want, got)
		}
	}
}

func TestTree_NodeSplit_DeletesAfterSplits(t *testing.T) {
	tr := newTestTree()
	const n = 200

	for i := 1; i <= n; i++ {
		treeSet(t, tr, fmt.Sprintf("key%04d", i), fmt.Sprintf("val%04d", i))
	}

	// Delete all but one key so the tree is never fully emptied during deletion.
	for i := 1; i < n; i++ {
		k := fmt.Sprintf("key%04d", i)
		old, err := tr.Delete([]byte(k))
		if err != nil {
			t.Fatalf("Delete(%q): %v", k, err)
		}
		if old == nil {
			t.Errorf("Delete(%q): expected non-nil old value", k)
		}
	}

	// The last remaining key must still be retrievable.
	lastKey := fmt.Sprintf("key%04d", n)
	got := treeGet(t, tr, lastKey)
	if !bytes.Equal(got, []byte(fmt.Sprintf("val%04d", n))) {
		t.Errorf("last key %q: expected val, got %q", lastKey, got)
	}
}

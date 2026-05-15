package kv

import (
	"bytes"
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	"testing"
)

func newTestKV() *KeyValue {
	pageSize := 16 * 1024
	storage := store.NewMemoryStorage(pageSize * 100)
	// pagesCount=1 reserves page 0, so first allocated page gets pointer 1, avoiding NULL_PAGE collision
	p := pager.NewPager(storage, 1, pageSize)
	return NewKeyValue(pager.NULL_PAGE, p)
}

// --- NewKeyValue / Root ---

func TestNewKeyValue_RootIsNullInitially(t *testing.T) {
	kv := newTestKV()
	if kv.Root() != pager.NULL_PAGE {
		t.Errorf("expected NULL_PAGE root, got %v", kv.Root())
	}
}

func TestNewKeyValue_RootNonNullAfterSet(t *testing.T) {
	kv := newTestKV()
	if _, err := kv.Set(&SetRequest{Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if kv.Root() == pager.NULL_PAGE {
		t.Error("expected non-null root after first Set")
	}
}

// --- Get ---

func TestKeyValue_Get_Nonexistent_ReturnsNilValue(t *testing.T) {
	kv := newTestKV()
	resp, err := kv.Get(&GetRequest{Key: []byte("missing")})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if resp.Value != nil {
		t.Errorf("expected nil value for missing key, got %v", resp.Value)
	}
}

func TestKeyValue_Get_ExistingKey_ReturnsValue(t *testing.T) {
	kv := newTestKV()
	if _, err := kv.Set(&SetRequest{Key: []byte("hello"), Value: []byte("world")}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	resp, err := kv.Get(&GetRequest{Key: []byte("hello")})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(resp.Value, []byte("world")) {
		t.Errorf("expected 'world', got %q", resp.Value)
	}
}

func TestKeyValue_Get_AfterMultipleSets(t *testing.T) {
	kv := newTestKV()
	entries := map[string]string{
		"alpha": "1",
		"beta":  "2",
		"gamma": "3",
	}
	for k, v := range entries {
		if _, err := kv.Set(&SetRequest{Key: []byte(k), Value: []byte(v)}); err != nil {
			t.Fatalf("Set(%q) failed: %v", k, err)
		}
	}
	for k, want := range entries {
		resp, err := kv.Get(&GetRequest{Key: []byte(k)})
		if err != nil {
			t.Fatalf("Get(%q) failed: %v", k, err)
		}
		if !bytes.Equal(resp.Value, []byte(want)) {
			t.Errorf("Get(%q): expected %q, got %q", k, want, resp.Value)
		}
	}
}

// --- Set ---

func TestKeyValue_Set_NewKey_AddedTrue(t *testing.T) {
	kv := newTestKV()
	resp, err := kv.Set(&SetRequest{Key: []byte("new"), Value: []byte("val")})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if !resp.Added {
		t.Error("expected Added=true for new key")
	}
	if resp.Updated {
		t.Error("expected Updated=false for new key")
	}
	if resp.OldValue != nil {
		t.Errorf("expected OldValue=nil for new key, got %v", resp.OldValue)
	}
}

func TestKeyValue_Set_ExistingKey_UpdatedTrue(t *testing.T) {
	kv := newTestKV()
	if _, err := kv.Set(&SetRequest{Key: []byte("key"), Value: []byte("old")}); err != nil {
		t.Fatalf("first Set failed: %v", err)
	}
	resp, err := kv.Set(&SetRequest{Key: []byte("key"), Value: []byte("new")})
	if err != nil {
		t.Fatalf("second Set failed: %v", err)
	}
	if !resp.Updated {
		t.Error("expected Updated=true when overwriting existing key")
	}
	if resp.Added {
		t.Error("expected Added=false when overwriting existing key")
	}
	if !bytes.Equal(resp.OldValue, []byte("old")) {
		t.Errorf("expected OldValue='old', got %q", resp.OldValue)
	}
}

func TestKeyValue_Set_UpdatedValue_GetReturnsNew(t *testing.T) {
	kv := newTestKV()
	kv.Set(&SetRequest{Key: []byte("k"), Value: []byte("v1")})
	kv.Set(&SetRequest{Key: []byte("k"), Value: []byte("v2")})
	resp, err := kv.Get(&GetRequest{Key: []byte("k")})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(resp.Value, []byte("v2")) {
		t.Errorf("expected updated value 'v2', got %q", resp.Value)
	}
}

// --- Delete ---

func TestKeyValue_Delete_Existing_ReturnsOldValue(t *testing.T) {
	kv := newTestKV()
	kv.Set(&SetRequest{Key: []byte("gone"), Value: []byte("bye")})
	resp, err := kv.Delete(&DeleteRequest{Key: []byte("gone")})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !bytes.Equal(resp.OldValue, []byte("bye")) {
		t.Errorf("expected OldValue='bye', got %q", resp.OldValue)
	}
}

func TestKeyValue_Delete_Existing_GetReturnsNil(t *testing.T) {
	kv := newTestKV()
	kv.Set(&SetRequest{Key: []byte("gone"), Value: []byte("bye")})
	kv.Delete(&DeleteRequest{Key: []byte("gone")})
	resp, err := kv.Get(&GetRequest{Key: []byte("gone")})
	if err != nil {
		t.Fatalf("Get after Delete failed: %v", err)
	}
	if resp.Value != nil {
		t.Errorf("expected nil after delete, got %v", resp.Value)
	}
}

func TestKeyValue_Delete_Nonexistent_OldValueNil(t *testing.T) {
	kv := newTestKV()
	resp, err := kv.Delete(&DeleteRequest{Key: []byte("phantom")})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if resp.OldValue != nil {
		t.Errorf("expected nil OldValue for missing key, got %v", resp.OldValue)
	}
}

// --- Scan ---

func TestKeyValue_Scan_EmptyTree_CursorIsEmpty(t *testing.T) {
	kv := newTestKV()
	cursor := kv.Scan(&ScanRequest{Key: []byte("any")})
	if !cursor.Empty() {
		t.Error("expected empty cursor on empty KV")
	}
}

func TestKeyValue_Scan_ExactKeyMatch(t *testing.T) {
	kv := newTestKV()
	kv.Set(&SetRequest{Key: []byte("b"), Value: []byte("B")})
	kv.Set(&SetRequest{Key: []byte("a"), Value: []byte("A")})

	cursor := kv.Scan(&ScanRequest{Key: []byte("b")})
	if cursor.Empty() {
		t.Fatal("expected non-empty cursor")
	}
	k, v := cursor.Current()
	if !bytes.Equal(k, []byte("b")) {
		t.Errorf("expected key 'b', got %q", k)
	}
	if !bytes.Equal(v, []byte("B")) {
		t.Errorf("expected value 'B', got %q", v)
	}
}

func TestKeyValue_Scan_SeeksToFirstGreaterOrEqual(t *testing.T) {
	kv := newTestKV()
	kv.Set(&SetRequest{Key: []byte("a"), Value: []byte("1")})
	kv.Set(&SetRequest{Key: []byte("c"), Value: []byte("3")})
	kv.Set(&SetRequest{Key: []byte("e"), Value: []byte("5")})

	// Seek to "b" — should land on "c" (first key >= "b")
	cursor := kv.Scan(&ScanRequest{Key: []byte("b")})
	if cursor.Empty() {
		t.Fatal("expected non-empty cursor")
	}
	k, _ := cursor.Current()
	if !bytes.Equal(k, []byte("c")) {
		t.Errorf("expected first key >= 'b' to be 'c', got %q", k)
	}
}

func TestKeyValue_Scan_BeyondAllKeys_EmptyCursor(t *testing.T) {
	kv := newTestKV()
	kv.Set(&SetRequest{Key: []byte("a"), Value: []byte("1")})
	kv.Set(&SetRequest{Key: []byte("b"), Value: []byte("2")})

	cursor := kv.Scan(&ScanRequest{Key: []byte("z")})
	if !cursor.Empty() {
		t.Error("expected empty cursor when seeking beyond all keys")
	}
}

func TestKeyValue_Scan_IterateAll(t *testing.T) {
	kv := newTestKV()
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		kv.Set(&SetRequest{Key: []byte(k), Value: []byte(k + "_val")})
	}

	cursor := kv.Scan(&ScanRequest{Key: []byte("a")})
	collected := []string{}
	// Termination: Next() returns (nil, nil) when there is no next element;
	// Empty() stays false while the cursor holds its last position.
	for k, v := cursor.Current(); v != nil; k, v = cursor.Next() {
		collected = append(collected, string(k))
	}

	if len(collected) != len(keys) {
		t.Fatalf("expected %d keys, got %d: %v", len(keys), len(collected), collected)
	}
	for i, want := range keys {
		if collected[i] != want {
			t.Errorf("at index %d: expected %q, got %q", i, want, collected[i])
		}
	}
}

func TestKeyValue_Scan_HasNext_HasPrev(t *testing.T) {
	kv := newTestKV()
	kv.Set(&SetRequest{Key: []byte("a"), Value: []byte("1")})
	kv.Set(&SetRequest{Key: []byte("b"), Value: []byte("2")})
	kv.Set(&SetRequest{Key: []byte("c"), Value: []byte("3")})

	cursor := kv.Scan(&ScanRequest{Key: []byte("a")})
	if cursor.HasPrev() {
		t.Error("expected HasPrev=false at first key")
	}
	if !cursor.HasNext() {
		t.Error("expected HasNext=true at first of three keys")
	}

	cursor.Next()
	if !cursor.HasPrev() {
		t.Error("expected HasPrev=true after advancing")
	}
}

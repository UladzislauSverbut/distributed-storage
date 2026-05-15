package helpers

import (
	"testing"
)

func intLess(a, b int) bool { return a < b }

func TestNewMinMap_IsEmpty(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	if m.Len() != 0 {
		t.Errorf("expected empty MinMap, got len=%d", m.Len())
	}
}

func TestMinMap_Add_Single(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	m.Add(1, "hello")
	if m.Len() != 1 {
		t.Errorf("expected 1 entry, got %d", m.Len())
	}
	vals, ok := m.Get(1)
	if !ok || len(vals) != 1 || vals[0] != "hello" {
		t.Errorf("unexpected values for key 1: %v ok=%v", vals, ok)
	}
}

func TestMinMap_Add_DuplicateKey_Appends(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	m.Add(1, "a")
	m.Add(1, "b")
	vals, ok := m.Get(1)
	if !ok || len(vals) != 2 {
		t.Errorf("expected 2 values for key 1, got %v ok=%v", vals, ok)
	}
	if m.Len() != 1 {
		t.Errorf("expected 1 unique key, got %d", m.Len())
	}
}

func TestMinMap_AddMultiple(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	vals, ok := m.AddMultiple(5, []string{"x", "y", "z"})
	if !ok || len(vals) != 3 {
		t.Errorf("expected 3 values, got %v ok=%v", vals, ok)
	}
}

func TestMinMap_AddMultiple_AppendToExisting(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	m.Add(2, "existing")
	m.AddMultiple(2, []string{"new1", "new2"})
	vals, _ := m.Get(2)
	if len(vals) != 3 {
		t.Errorf("expected 3 values after AddMultiple on existing key, got %d", len(vals))
	}
}

func TestMinMap_Get_Nonexistent(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	_, ok := m.Get(42)
	if ok {
		t.Error("expected ok=false for missing key")
	}
}

func TestMinMap_PeekMin_Empty(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	_, _, ok := m.PeekMin()
	if ok {
		t.Error("expected ok=false for empty MinMap")
	}
}

func TestMinMap_PeekMin_ReturnsMinKey(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	m.Add(3, "c")
	m.Add(1, "a")
	m.Add(2, "b")
	key, _, ok := m.PeekMin()
	if !ok || key != 1 {
		t.Errorf("expected min key=1, got key=%d ok=%v", key, ok)
	}
	if m.Len() != 3 {
		t.Error("PeekMin should not remove the element")
	}
}

func TestMinMap_PeekMin_ReturnsValues(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	m.Add(1, "first")
	_, vals, ok := m.PeekMin()
	if !ok || len(vals) != 1 || vals[0] != "first" {
		t.Errorf("unexpected PeekMin values: %v ok=%v", vals, ok)
	}
}

func TestMinMap_PopMin_Empty(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	_, _, ok := m.PopMin()
	if ok {
		t.Error("expected ok=false for empty MinMap")
	}
}

func TestMinMap_PopMin_RemovesMinKey(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	m.Add(2, "b")
	m.Add(1, "a")
	key, vals, ok := m.PopMin()
	if !ok || key != 1 {
		t.Errorf("expected key=1, got key=%d ok=%v", key, ok)
	}
	if len(vals) != 1 || vals[0] != "a" {
		t.Errorf("unexpected values: %v", vals)
	}
	if m.Len() != 1 {
		t.Errorf("expected 1 key remaining, got %d", m.Len())
	}
	if _, ok := m.Get(1); ok {
		t.Error("key 1 should be gone after PopMin")
	}
}

func TestMinMap_PopMin_AscendingOrder(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	for _, k := range []int{5, 3, 1, 4, 2} {
		m.Add(k, "v")
	}
	prev := -1
	for m.Len() > 0 {
		key, _, ok := m.PopMin()
		if !ok || key <= prev {
			t.Errorf("expected ascending order: got key=%d after prev=%d", key, prev)
		}
		prev = key
	}
}

func TestMinMap_RemoveKey_Existing(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	m.Add(1, "a")
	m.Add(2, "b")
	removed := m.RemoveKey(1)
	if !removed {
		t.Error("expected RemoveKey to return true")
	}
	if m.Len() != 1 {
		t.Errorf("expected 1 key remaining, got %d", m.Len())
	}
	if _, ok := m.Get(1); ok {
		t.Error("key 1 should be gone after RemoveKey")
	}
}

func TestMinMap_RemoveKey_Nonexistent(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	if m.RemoveKey(99) {
		t.Error("expected RemoveKey to return false for missing key")
	}
}

func TestMinMap_RemoveKey_UpdatesMinKey(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	m.Add(1, "a")
	m.Add(2, "b")
	m.RemoveKey(1)
	key, _, ok := m.PeekMin()
	if !ok || key != 2 {
		t.Errorf("expected new min key=2 after removing 1, got key=%d ok=%v", key, ok)
	}
}

func TestMinMap_Len_Sequence(t *testing.T) {
	m := NewMinMap[int, string](intLess)
	if m.Len() != 0 {
		t.Error("expected 0")
	}
	m.Add(1, "a")
	if m.Len() != 1 {
		t.Error("expected 1")
	}
	m.Add(1, "b") // same key
	if m.Len() != 1 {
		t.Error("expected 1 (same key appends)")
	}
	m.Add(2, "c")
	if m.Len() != 2 {
		t.Error("expected 2")
	}
	m.PopMin()
	if m.Len() != 1 {
		t.Error("expected 1 after PopMin")
	}
}

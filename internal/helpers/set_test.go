package helpers

import (
	"testing"
)

func TestNewSet_Empty(t *testing.T) {
	s := NewSet[int]()
	if s.Has(1) {
		t.Error("expected empty set to not contain 1")
	}
	if vals := s.Values(); len(vals) != 0 {
		t.Errorf("expected empty values, got %v", vals)
	}
}

func TestNewSet_WithInitialElements(t *testing.T) {
	s := NewSet(1, 2, 3)
	for _, v := range []int{1, 2, 3} {
		if !s.Has(v) {
			t.Errorf("expected set to contain %d", v)
		}
	}
	if len(s.Values()) != 3 {
		t.Errorf("expected 3 values, got %d", len(s.Values()))
	}
}

func TestSet_Add_Single(t *testing.T) {
	s := NewSet[string]()
	s.Add("hello")
	if !s.Has("hello") {
		t.Error("expected set to contain 'hello'")
	}
}

func TestSet_Add_Duplicate(t *testing.T) {
	s := NewSet[int]()
	s.Add(42)
	s.Add(42)
	if len(s.Values()) != 1 {
		t.Errorf("expected 1 element after duplicate Add, got %d", len(s.Values()))
	}
}

func TestSet_Add_ToZeroValueSet(t *testing.T) {
	var s Set[int] // nil map inside
	s.Add(1)
	if !s.Has(1) {
		t.Error("expected set to contain 1 after Add on zero-value Set")
	}
}

func TestSet_Pop_ReturnsElement(t *testing.T) {
	s := NewSet(10)
	val, ok := s.Pop()
	if !ok {
		t.Error("expected ok=true")
	}
	if val != 10 {
		t.Errorf("expected 10, got %d", val)
	}
	if s.Has(10) {
		t.Error("element should be removed after Pop")
	}
}

func TestSet_Pop_EmptySet(t *testing.T) {
	s := NewSet[int]()
	_, ok := s.Pop()
	if ok {
		t.Error("expected ok=false for empty set")
	}
}

func TestSet_Remove_Existing(t *testing.T) {
	s := NewSet(1, 2, 3)
	s.Remove(2)
	if s.Has(2) {
		t.Error("element 2 should be removed")
	}
	if !s.Has(1) || !s.Has(3) {
		t.Error("other elements should still be present")
	}
}

func TestSet_Remove_Nonexistent(t *testing.T) {
	s := NewSet(1)
	s.Remove(99) // should not panic
	if !s.Has(1) {
		t.Error("existing element should be unaffected")
	}
}

func TestSet_Remove_ZeroValueSet(t *testing.T) {
	var s Set[int]
	s.Remove(1) // should not panic
}

func TestSet_Has_Present(t *testing.T) {
	s := NewSet("a", "b")
	if !s.Has("a") {
		t.Error("expected Has('a')=true")
	}
}

func TestSet_Has_Absent(t *testing.T) {
	s := NewSet("a")
	if s.Has("z") {
		t.Error("expected Has('z')=false")
	}
}

func TestSet_Has_ZeroValueSet(t *testing.T) {
	var s Set[int]
	if s.Has(1) {
		t.Error("expected Has on nil-map Set to return false")
	}
}

func TestSet_Values_Empty(t *testing.T) {
	s := NewSet[int]()
	if v := s.Values(); len(v) != 0 {
		t.Errorf("expected empty slice, got %v", v)
	}
}

func TestSet_Values_NonEmpty(t *testing.T) {
	s := NewSet(1, 2, 3)
	v := s.Values()
	if len(v) != 3 {
		t.Fatalf("expected 3 values, got %d", len(v))
	}
	seen := map[int]bool{}
	for _, x := range v {
		seen[x] = true
	}
	for _, want := range []int{1, 2, 3} {
		if !seen[want] {
			t.Errorf("missing value %d in Values()", want)
		}
	}
}

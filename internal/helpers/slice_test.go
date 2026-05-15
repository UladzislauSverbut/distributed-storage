package helpers

import (
	"bytes"
	"testing"
)

func TestClone_Empty(t *testing.T) {
	result := Clone([]int{})
	if len(result) != 0 {
		t.Errorf("expected empty slice, got %v", result)
	}
}

func TestClone_NonEmpty(t *testing.T) {
	src := []byte{1, 2, 3}
	dst := Clone(src)
	if !bytes.Equal(src, dst) {
		t.Errorf("expected %v, got %v", src, dst)
	}
	src[0] = 99
	if dst[0] == 99 {
		t.Error("Clone should produce an independent copy")
	}
}

func TestJoinFunc_Empty(t *testing.T) {
	result := JoinFunc([]int{}, func(i int) string { return "" }, ",")
	if result != "" {
		t.Errorf("expected empty string, got %q", result)
	}
}

func TestJoinFunc_Single(t *testing.T) {
	result := JoinFunc([]string{"hello"}, func(s string) string { return s }, ",")
	if result != "hello" {
		t.Errorf("expected 'hello', got %q", result)
	}
}

func TestJoinFunc_Multiple(t *testing.T) {
	result := JoinFunc([]string{"a", "b", "c"}, func(s string) string { return s }, "-")
	if result != "a-b-c" {
		t.Errorf("expected 'a-b-c', got %q", result)
	}
}

func TestIsZero_AllZero(t *testing.T) {
	if !IsZero([]byte{0, 0, 0}) {
		t.Error("expected all-zero slice to be zero")
	}
}

func TestIsZero_HasNonZero(t *testing.T) {
	if IsZero([]byte{0, 1, 0}) {
		t.Error("expected non-all-zero slice to not be zero")
	}
}

func TestIsZero_Empty(t *testing.T) {
	if !IsZero([]int{}) {
		t.Error("expected empty slice to be zero")
	}
}

func TestReadFromSegments_SingleSegment(t *testing.T) {
	seg := []byte{1, 2, 3, 4, 5}
	result := ReadFromSegments([][]byte{seg}, 1, 3)
	if !bytes.Equal(result, []byte{2, 3, 4}) {
		t.Errorf("expected [2 3 4], got %v", result)
	}
}

func TestReadFromSegments_AcrossSegments(t *testing.T) {
	seg1 := []byte{1, 2, 3}
	seg2 := []byte{4, 5, 6}
	result := ReadFromSegments([][]byte{seg1, seg2}, 2, 3)
	if !bytes.Equal(result, []byte{3, 4, 5}) {
		t.Errorf("expected [3 4 5], got %v", result)
	}
}

func TestReadFromSegments_OffsetBeyondFirstSegment(t *testing.T) {
	seg1 := []byte{1, 2}
	seg2 := []byte{10, 20, 30}
	result := ReadFromSegments([][]byte{seg1, seg2}, 2, 2)
	if !bytes.Equal(result, []byte{10, 20}) {
		t.Errorf("expected [10 20], got %v", result)
	}
}

func TestReadFromSegments_FromStart(t *testing.T) {
	seg := []byte{5, 6, 7}
	result := ReadFromSegments([][]byte{seg}, 0, 3)
	if !bytes.Equal(result, seg) {
		t.Errorf("expected %v, got %v", seg, result)
	}
}

func TestWriteToSegments_SingleSegment(t *testing.T) {
	seg := make([]byte, 5)
	WriteToSegments([][]byte{seg}, 1, []byte{9, 8})
	if seg[1] != 9 || seg[2] != 8 {
		t.Errorf("unexpected segment content: %v", seg)
	}
}

func TestWriteToSegments_AcrossSegments(t *testing.T) {
	seg1 := make([]byte, 3)
	seg2 := make([]byte, 3)
	WriteToSegments([][]byte{seg1, seg2}, 2, []byte{1, 2, 3})
	if seg1[2] != 1 || seg2[0] != 2 || seg2[1] != 3 {
		t.Errorf("unexpected content: seg1=%v seg2=%v", seg1, seg2)
	}
}

func TestWriteToSegments_OffsetBeyondFirstSegment(t *testing.T) {
	seg1 := make([]byte, 2)
	seg2 := make([]byte, 3)
	WriteToSegments([][]byte{seg1, seg2}, 2, []byte{42})
	if seg2[0] != 42 {
		t.Errorf("expected seg2[0]=42, got %d", seg2[0])
	}
}

func TestSplitBy_NoSeparator(t *testing.T) {
	parts := SplitBy([]byte("hello"), '|')
	if len(parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(parts))
	}
	if !bytes.Equal(parts[0], []byte("hello")) {
		t.Errorf("expected 'hello', got %s", parts[0])
	}
}

func TestSplitBy_WithSeparator(t *testing.T) {
	parts := SplitBy([]byte("a|b|c"), '|')
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}
	if string(parts[0]) != "a" || string(parts[1]) != "b" || string(parts[2]) != "c" {
		t.Errorf("unexpected parts: %v", parts)
	}
}

func TestSplitBy_EmptyInput(t *testing.T) {
	parts := SplitBy([]byte{}, '|')
	if len(parts) != 1 || len(parts[0]) != 0 {
		t.Errorf("expected 1 empty part, got %v", parts)
	}
}

func TestSplitBy_SeparatorAtStart(t *testing.T) {
	parts := SplitBy([]byte("|x"), '|')
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}
	if len(parts[0]) != 0 {
		t.Errorf("expected first part to be empty, got %v", parts[0])
	}
}

func TestFlatten_Empty(t *testing.T) {
	result := Flatten([][]int{})
	if len(result) != 0 {
		t.Errorf("expected empty slice, got %v", result)
	}
}

func TestFlatten_SingleSlice(t *testing.T) {
	result := Flatten([][]int{{1, 2, 3}})
	if len(result) != 3 || result[0] != 1 || result[2] != 3 {
		t.Errorf("unexpected result: %v", result)
	}
}

func TestFlatten_MultipleSlices(t *testing.T) {
	result := Flatten([][]int{{1, 2}, {3}, {4, 5}})
	expected := []int{1, 2, 3, 4, 5}
	if len(result) != len(expected) {
		t.Fatalf("expected %d elements, got %d", len(expected), len(result))
	}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("at index %d: expected %d, got %d", i, v, result[i])
		}
	}
}

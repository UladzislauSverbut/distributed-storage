package pager

import (
	"reflect"
	"testing"
)

// --- NewPageList ---

func TestNewPageList_Empty(t *testing.T) {
	l := NewPageList()
	if !l.Empty() {
		t.Error("expected empty list")
	}
}

func TestNewPageList_WithIntervals(t *testing.T) {
	l := NewPageList(PageInterval{Start: 1, End: 3}, PageInterval{Start: 7, End: 9})
	if l.Empty() {
		t.Error("expected non-empty list")
	}
	if pages := l.Pages(); len(pages) != 2 {
		t.Fatalf("expected 2 intervals, got %d", len(pages))
	}
}

// --- Empty ---

func TestPageList_Empty_AfterAdd(t *testing.T) {
	l := NewPageList()
	l.Add(1)
	if l.Empty() {
		t.Error("expected non-empty after Add")
	}
}

func TestPageList_Empty_AfterPopLast(t *testing.T) {
	l := NewPageList(PageInterval{Start: 5, End: 5})
	l.Pop()
	if !l.Empty() {
		t.Error("expected empty after popping only page")
	}
}

// --- Has ---

func TestPageList_Has_PresentInInterval(t *testing.T) {
	l := NewPageList(PageInterval{Start: 3, End: 7})
	for _, p := range []PagePointer{3, 4, 5, 6, 7} {
		if !l.Has(p) {
			t.Errorf("expected Has(%d) == true", p)
		}
	}
}

func TestPageList_Has_Absent(t *testing.T) {
	l := NewPageList(PageInterval{Start: 3, End: 7})
	for _, p := range []PagePointer{0, 2, 8, 100} {
		if l.Has(p) {
			t.Errorf("expected Has(%d) == false", p)
		}
	}
}

func TestPageList_Has_EmptyList(t *testing.T) {
	l := NewPageList()
	if l.Has(0) {
		t.Error("expected Has(0) == false on empty list")
	}
}

// --- Add ---

func TestPageList_Add_Single(t *testing.T) {
	l := NewPageList()
	l.Add(5)
	if !l.Has(5) {
		t.Error("expected Has(5) after Add(5)")
	}
}

func TestPageList_Add_Idempotent(t *testing.T) {
	l := NewPageList()
	l.Add(5)
	l.Add(5)
	pages := l.Pages()
	if len(pages) != 1 || pages[0] != (PageInterval{Start: 5, End: 5}) {
		t.Errorf("expected single interval [5,5] after duplicate add, got %v", pages)
	}
}

func TestPageList_Add_ExtendsIntervalRight(t *testing.T) {
	l := NewPageList(PageInterval{Start: 3, End: 5})
	l.Add(6)
	pages := l.Pages()
	if len(pages) != 1 || pages[0] != (PageInterval{Start: 3, End: 6}) {
		t.Errorf("expected [3,6], got %v", pages)
	}
}

func TestPageList_Add_ExtendsIntervalLeft(t *testing.T) {
	l := NewPageList(PageInterval{Start: 3, End: 5})
	l.Add(2)
	pages := l.Pages()
	if len(pages) != 1 || pages[0] != (PageInterval{Start: 2, End: 5}) {
		t.Errorf("expected [2,5], got %v", pages)
	}
}

func TestPageList_Add_MergesAdjacentIntervals(t *testing.T) {
	l := NewPageList(PageInterval{Start: 1, End: 3}, PageInterval{Start: 5, End: 7})
	l.Add(4) // fills the gap between [1,3] and [5,7]
	pages := l.Pages()
	if len(pages) != 1 || pages[0] != (PageInterval{Start: 1, End: 7}) {
		t.Errorf("expected merged [1,7], got %v", pages)
	}
}

func TestPageList_Add_InsertsNewDisjointInterval(t *testing.T) {
	l := NewPageList(PageInterval{Start: 1, End: 3})
	l.Add(10)
	pages := l.Pages()
	if len(pages) != 2 {
		t.Fatalf("expected 2 intervals, got %d: %v", len(pages), pages)
	}
	if pages[1] != (PageInterval{Start: 10, End: 10}) {
		t.Errorf("expected second interval [10,10], got %v", pages[1])
	}
}

func TestPageList_Add_InsertsBefore(t *testing.T) {
	l := NewPageList(PageInterval{Start: 10, End: 15})
	l.Add(5) // should be inserted before the existing interval
	pages := l.Pages()
	if len(pages) != 2 || pages[0] != (PageInterval{Start: 5, End: 5}) {
		t.Errorf("expected [5,5] as first interval, got %v", pages)
	}
}

// --- Pop ---

func TestPageList_Pop_EmptyReturnsFalse(t *testing.T) {
	l := NewPageList()
	page, ok := l.Pop()
	if ok {
		t.Errorf("expected ok=false from empty list, got page=%d", page)
	}
	if page != NULL_PAGE {
		t.Errorf("expected NULL_PAGE, got %d", page)
	}
}

func TestPageList_Pop_SinglePage(t *testing.T) {
	l := NewPageList(PageInterval{Start: 5, End: 5})
	page, ok := l.Pop()
	if !ok {
		t.Fatal("expected ok=true")
	}
	if page != 5 {
		t.Errorf("expected page 5, got %d", page)
	}
	if !l.Empty() {
		t.Error("expected empty list after popping only page")
	}
}

func TestPageList_Pop_ReturnsSmallestPage(t *testing.T) {
	l := NewPageList(PageInterval{Start: 2, End: 5})
	page, _ := l.Pop()
	if page != 2 {
		t.Errorf("expected smallest page 2, got %d", page)
	}
}

func TestPageList_Pop_ShrinksInterval(t *testing.T) {
	l := NewPageList(PageInterval{Start: 2, End: 4})
	l.Pop()
	pages := l.Pages()
	if len(pages) != 1 || pages[0] != (PageInterval{Start: 3, End: 4}) {
		t.Errorf("expected [3,4] after pop, got %v", pages)
	}
}

func TestPageList_Pop_ExhaustsAllPages(t *testing.T) {
	l := NewPageList(PageInterval{Start: 1, End: 3})
	for want := PagePointer(1); want <= 3; want++ {
		got, ok := l.Pop()
		if !ok || got != want {
			t.Errorf("Pop %d: expected (%d, true), got (%d, %v)", want, want, got, ok)
		}
	}
	if !l.Empty() {
		t.Error("expected empty list after exhausting all pages")
	}
}

func TestPageList_Pop_AcrossMultipleIntervals(t *testing.T) {
	l := NewPageList(PageInterval{Start: 1, End: 2}, PageInterval{Start: 5, End: 6})
	expected := []PagePointer{1, 2, 5, 6}
	for _, want := range expected {
		got, ok := l.Pop()
		if !ok || got != want {
			t.Errorf("expected (%d, true), got (%d, %v)", want, got, ok)
		}
	}
}

// --- Pages ---

func TestPageList_Pages_Empty(t *testing.T) {
	l := NewPageList()
	if pages := l.Pages(); len(pages) != 0 {
		t.Errorf("expected empty slice, got %v", pages)
	}
}

func TestPageList_Pages_ReturnsAllIntervals(t *testing.T) {
	want := []PageInterval{{Start: 1, End: 3}, {Start: 7, End: 9}}
	l := NewPageList(want...)
	got := l.Pages()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

// --- Clone ---

func TestPageList_Clone_ContainsSamePages(t *testing.T) {
	l := NewPageList(PageInterval{Start: 1, End: 5})
	clone := l.Clone()
	for _, interval := range l.Pages() {
		for p := interval.Start; p <= interval.End; p++ {
			if !clone.Has(p) {
				t.Errorf("clone missing page %d", p)
			}
		}
	}
}

func TestPageList_Clone_IsIndependentOfOriginal(t *testing.T) {
	original := NewPageList(PageInterval{Start: 1, End: 5})
	clone := original.Clone()

	clone.Add(20)
	if original.Has(20) {
		t.Error("modifying clone affected original")
	}

	original.Add(30)
	if clone.Has(30) {
		t.Error("modifying original affected clone")
	}
}

// --- AddMany ---

func TestPageList_AddMany_DisjoitIntervals(t *testing.T) {
	l := NewPageList()
	l.AddMany([]PageInterval{{Start: 1, End: 3}, {Start: 7, End: 9}})

	for _, p := range []PagePointer{1, 2, 3, 7, 8, 9} {
		if !l.Has(p) {
			t.Errorf("expected page %d after AddMany", p)
		}
	}
	if l.Has(5) {
		t.Error("unexpected page 5 present after AddMany")
	}
}

func TestPageList_AddMany_AddsToExistingList(t *testing.T) {
	l := NewPageList(PageInterval{Start: 100, End: 110})
	l.AddMany([]PageInterval{{Start: 1, End: 3}, {Start: 7, End: 9}})

	if !l.Has(100) || !l.Has(1) || !l.Has(7) {
		t.Error("expected pages from both original and AddMany to be present")
	}
}

func TestPageList_AddMany_EmptySlice(t *testing.T) {
	l := NewPageList(PageInterval{Start: 1, End: 3})
	l.AddMany([]PageInterval{})
	pages := l.Pages()
	if len(pages) != 1 || pages[0] != (PageInterval{Start: 1, End: 3}) {
		t.Errorf("AddMany with empty slice changed list: %v", pages)
	}
}

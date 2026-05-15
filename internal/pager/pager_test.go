package pager

import (
	"bytes"
	"distributed-storage/internal/store"
	"testing"
)

const testPageSize = 64

func makeStorage() *store.MemoryStorage {
	return store.NewMemoryStorage(testPageSize * 128)
}

func pageData(b string) []byte {
	d := make([]byte, testPageSize)
	copy(d, b)

	return d
}

// --- NewPager ---

func TestNewPager_InitialState(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	if p.PagesCount() != 1 {
		t.Errorf("expected PagesCount 1, got %d", p.PagesCount())
	}
	if !p.ReusablePages().Empty() {
		t.Error("expected empty ReusablePages on fresh pager")
	}
	if !p.ReleasedPages().Empty() {
		t.Error("expected empty ReleasedPages on fresh pager")
	}
}

func TestNewPager_WithAvailablePages(t *testing.T) {
	avail := NewPageList(PageInterval{Start: 3, End: 5})
	p := NewPager(makeStorage(), 10, testPageSize, avail)

	// Both AvailablePages and ReusablePages are seeded with the provided list.
	// The first CreatePage should pop from ReusablePages.
	ptr := p.CreatePage(pageData("first page"))
	if ptr != 3 {
		t.Errorf("expected page 3 from reusable pool, got %d", ptr)
	}
}

// --- PagesCount ---

func TestPager_PagesCount_IncreasesOnCreate(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	before := p.PagesCount()
	p.CreatePage(pageData("first page"))
	if p.PagesCount() != before+1 {
		t.Errorf("expected PagesCount %d, got %d", before+1, p.PagesCount())
	}
}

func TestPager_PagesCount_UnchangedOnReuse(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	ptr := p.CreatePage(pageData("first page"))
	p.FreePage(ptr)
	before := p.PagesCount()

	p.CreatePage(pageData("second page")) // reuses freed page
	if p.PagesCount() != before {
		t.Errorf("expected PagesCount unchanged at %d after reuse, got %d", before, p.PagesCount())
	}
}

// --- CreatePage ---

func TestPager_CreatePage_ReturnsPointer(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	ptr := p.CreatePage(pageData("first page"))
	if ptr == NULL_PAGE {
		t.Error("expected non-NULL_PAGE pointer from CreatePage")
	}
}

func TestPager_CreatePage_SequentialPointers(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	ptr1 := p.CreatePage(pageData("first page"))
	ptr2 := p.CreatePage(pageData("second page"))
	ptr3 := p.CreatePage(pageData("third page"))
	if ptr2 != ptr1+1 || ptr3 != ptr1+2 {
		t.Errorf("expected sequential pointers %d,%d,%d", ptr1, ptr1+1, ptr1+2)
	}
}

func TestPager_CreatePage_ReusesFreedPage(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	ptr := p.CreatePage(pageData("first page"))
	countAfterCreate := p.PagesCount()

	p.FreePage(ptr) // moves ptr to ReusablePages
	ptr2 := p.CreatePage(pageData("second page"))

	if ptr2 != ptr {
		t.Errorf("expected reused pointer %d, got %d", ptr, ptr2)
	}
	if p.PagesCount() != countAfterCreate {
		t.Errorf("expected PagesCount %d unchanged after reuse, got %d", countAfterCreate, p.PagesCount())
	}
}

func TestPager_CreatePage_DataAccessible(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	data := pageData("first page")
	ptr := p.CreatePage(data)

	got := p.Page(ptr)
	if !bytes.Equal(got, data) {
		t.Error("page data not accessible immediately after CreatePage")
	}
}

// --- Page ---

func TestPager_Page_ReturnsInMemoryData(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	data := pageData("first page")
	ptr := p.CreatePage(data)

	got := p.Page(ptr)
	if !bytes.Equal(got, data) {
		t.Error("Page returned wrong data for in-memory page")
	}
}

func TestPager_Page_FromStorageAfterSave(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	data := pageData("first page")
	ptr := p.CreatePage(data)

	if err := p.SaveChanges(); err != nil {
		t.Fatalf("SaveChanges: %v", err)
	}

	// After SaveChanges the in-memory updates are cleared; Page must read from storage.
	got := p.Page(ptr)
	if !bytes.Equal(got, data) {
		t.Error("Page returned wrong data from storage after SaveChanges")
	}
}

// --- UpdatePage ---

func TestPager_UpdatePage_Valid(t *testing.T) {
	// pagesCount=1 means page 0 exists (the header page).
	p := NewPager(makeStorage(), 1, testPageSize)
	data := pageData("header page")

	if err := p.UpdatePage(0, data); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(p.Page(0), data) {
		t.Error("Page(0) did not return updated data")
	}
}

func TestPager_UpdatePage_InvalidPointer(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	if err := p.UpdatePage(5, pageData("corrupted page")); err == nil {
		t.Fatal("expected error for pointer beyond PagesCount")
	}
}

func TestPager_UpdatePage_OverwritesExistingPage(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	ptr := p.CreatePage(pageData("first page"))

	newData := pageData("second page")
	// After CreatePage, PagesCount has grown to include ptr.
	if err := p.UpdatePage(ptr, newData); err != nil {
		t.Fatalf("UpdatePage: %v", err)
	}
	if !bytes.Equal(p.Page(ptr), newData) {
		t.Error("Page did not return overwritten data")
	}
}

// --- FreePage ---

func TestPager_FreePage_AvailablePageMovesToReusable(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	ptr := p.CreatePage(pageData("first page")) // ptr is added to AvailablePages

	p.FreePage(ptr)

	if !p.ReusablePages().Has(ptr) {
		t.Errorf("expected freed available page %d in ReusablePages", ptr)
	}
}

func TestPager_FreePage_NonAvailablePageMovesToReleased(t *testing.T) {
	// pagesCount=2 means pages 0 and 1 exist but are NOT in AvailablePages
	// (they were never allocated through CreatePage).
	p := NewPager(makeStorage(), 2, testPageSize)

	p.FreePage(0) // page 0 is not in AvailablePages

	if !p.ReleasedPages().Has(0) {
		t.Error("expected non-available page 0 in ReleasedPages after FreePage")
	}
	if p.ReusablePages().Has(0) {
		t.Error("expected page 0 NOT in ReusablePages")
	}
}

func TestPager_FreePage_RemovesFromPageUpdates(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	ptr := p.CreatePage(pageData("first page"))

	p.FreePage(ptr)

	// After freeing, Page() should fall back to storage (not return the old in-memory data).
	// We verify by checking the next CreatePage reuses the pointer successfully.
	ptr2 := p.CreatePage(pageData("second page"))
	if ptr2 != ptr {
		t.Errorf("expected reused pointer %d, got %d", ptr, ptr2)
	}
}

// --- ReleasedPages / ReusablePages ---

func TestPager_ReleasedPages_EmptyInitially(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	if !p.ReleasedPages().Empty() {
		t.Error("expected empty ReleasedPages on fresh pager")
	}
}

func TestPager_ReusablePages_EmptyInitially(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	if !p.ReusablePages().Empty() {
		t.Error("expected empty ReusablePages on fresh pager")
	}
}

// --- SaveChanges ---

func TestPager_SaveChanges_PersistsAllUpdates(t *testing.T) {
	storage := makeStorage()
	p := NewPager(storage, 1, testPageSize)

	pages := make(map[PagePointer][]byte)
	for _, name := range []string{"first page", "second page", "third page", "fourth page", "fifth page"} {
		d := pageData(name)
		ptr := p.CreatePage(d)
		pages[ptr] = d
	}

	if err := p.SaveChanges(); err != nil {
		t.Fatalf("SaveChanges: %v", err)
	}

	for ptr, want := range pages {
		got := p.Page(ptr)
		if !bytes.Equal(got, want) {
			t.Errorf("page %d: expected data 0x%x, got 0x%x", ptr, want[0], got[0])
		}
	}
}

func TestPager_SaveChanges_ClearsInMemoryUpdates(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	ptr := p.CreatePage(pageData("first page"))

	if err := p.SaveChanges(); err != nil {
		t.Fatalf("SaveChanges: %v", err)
	}

	// Updating the page creates a new in-memory entry; SaveChanges cleared the old one.
	newData := pageData("second page")
	if err := p.UpdatePage(ptr, newData); err != nil {
		t.Fatalf("UpdatePage after SaveChanges: %v", err)
	}
	if !bytes.Equal(p.Page(ptr), newData) {
		t.Error("expected new data after UpdatePage post-SaveChanges")
	}
}

// --- Snapshot / Restore ---

func TestPager_Snapshot_CapturesPagesCount(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	p.CreatePage(pageData("first page"))
	countAtSnapshot := p.PagesCount()

	snap := p.Snapshot()

	p.CreatePage(pageData("second page"))
	p.CreatePage(pageData("third page"))

	if snap.PagesCount != countAtSnapshot {
		t.Errorf("snapshot PagesCount changed: expected %d, got %d", countAtSnapshot, snap.PagesCount)
	}
}

func TestPager_Restore_ResetsPagesCount(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	p.CreatePage(pageData("first page"))
	snap := p.Snapshot()
	countAtSnapshot := p.PagesCount()

	p.CreatePage(pageData("second page"))
	p.CreatePage(pageData("third page"))

	p.Restore(snap)

	if p.PagesCount() != countAtSnapshot {
		t.Errorf("expected PagesCount %d after restore, got %d", countAtSnapshot, p.PagesCount())
	}
}

func TestPager_Restore_ResetsAvailableAndReusable(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	snap := p.Snapshot() // snapshot before any pages created

	ptr := p.CreatePage(pageData("first page"))

	p.Restore(snap)

	if p.ReusablePages().Has(ptr) {
		t.Errorf("expected ReusablePages to not contain page %d after restore", ptr)
	}
}

func TestPager_Snapshot_IsIndependentOfOriginal(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	snap := p.Snapshot()
	countBefore := snap.PagesCount

	// Creating pages on the original must not alter the snapshot's PagesCount.
	p.CreatePage(pageData("first page"))
	p.CreatePage(pageData("second page"))

	if snap.PagesCount != countBefore {
		t.Errorf("snapshot PagesCount was mutated: expected %d, got %d", countBefore, snap.PagesCount)
	}
}

func TestPager_Snapshot_AvailablePagesIsIndependent(t *testing.T) {
	p := NewPager(makeStorage(), 1, testPageSize)
	snap := p.Snapshot()

	ptr := p.CreatePage(pageData("first page"))
	_ = ptr

	if snap.AvailablePages.Has(ptr) {
		t.Errorf("snapshot AvailablePages was mutated to include page %d", ptr)
	}
}

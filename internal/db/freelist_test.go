package db

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	"slices"
	"testing"
)

// nodePageSize gives nodeCapacity = (48-16)/16 = 2, so multi-node overflow
// is easy to trigger with just 3 pages.
const nodePageSize = 48

type flFixture struct {
	fl      *freeList
	pg      *pager.Pager
	storage store.Storage
}

func newFixture(t *testing.T) flFixture {
	t.Helper()
	storage := store.NewMemoryStorage(nodePageSize * 256)
	pg := pager.NewPager(storage, 1, nodePageSize) // page 0 reserved as header
	fl := newFreeList(pager.NULL_PAGE, pager.NULL_PAGE, pg)
	return flFixture{fl: fl, pg: pg, storage: storage}
}

// save flushes pager state to storage so a resumed freeList reads the latest data.
func (f flFixture) save(t *testing.T) {
	t.Helper()
	if err := f.pg.SaveChanges(); err != nil {
		t.Fatalf("SaveChanges: %v", err)
	}
}

// resume creates a new freeList from the same storage, using the current
// head/tail of f.fl — simulates starting the next commit.
func (f flFixture) resume() *freeList {
	pg := pager.NewPager(f.storage, f.pg.PagesCount(), nodePageSize)
	return newFreeList(f.fl.head, f.fl.tail, pg)
}

func containsAll(result, want []pager.PagePointer) bool {
	for _, w := range want {
		if !slices.Contains(result, w) {
			return false
		}
	}
	return true
}

// --- push ---

func TestFreeList_Push_EmptyPagesIsNoop(t *testing.T) {
	f := newFixture(t)

	if err := f.fl.push(1, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.fl.head != pager.NULL_PAGE || f.fl.tail != pager.NULL_PAGE {
		t.Error("push of empty slice must not allocate any node")
	}
}

func TestFreeList_Push_AllocatesNode(t *testing.T) {
	f := newFixture(t)

	if err := f.fl.push(1, []pager.PagePointer{42}); err != nil {
		t.Fatalf("push: %v", err)
	}
	if f.fl.head == pager.NULL_PAGE {
		t.Error("expected head to be set after first push")
	}
	if f.fl.head != f.fl.tail {
		t.Error("single node: head and tail must point to the same page")
	}
}

func TestFreeList_Push_MultipleCallsReuseTailNode(t *testing.T) {
	f := newFixture(t)
	// capacity=2; push 1+1 = 2 entries — must stay in one node
	if err := f.fl.push(1, []pager.PagePointer{10}); err != nil {
		t.Fatalf("push 1: %v", err)
	}
	headAfterFirst := f.fl.head

	if err := f.fl.push(1, []pager.PagePointer{20}); err != nil {
		t.Fatalf("push 2: %v", err)
	}
	if f.fl.head != headAfterFirst {
		t.Error("second push should reuse existing tail node, not allocate a new one")
	}
	if f.fl.head != f.fl.tail {
		t.Error("still one node: head and tail must be equal")
	}
}

func TestFreeList_Push_OverflowAllocatesNewNode(t *testing.T) {
	f := newFixture(t)
	cap := f.fl.nodeCapacity() // 2

	pages := make([]pager.PagePointer, cap+1)
	for i := range pages {
		pages[i] = pager.PagePointer(10 + i)
	}

	if err := f.fl.push(1, pages); err != nil {
		t.Fatalf("push: %v", err)
	}
	if f.fl.head == f.fl.tail {
		t.Errorf("cap+1 entries must span two nodes (head=%d tail=%d)", f.fl.head, f.fl.tail)
	}
}

// --- collect ---

func TestFreeList_Collect_EmptyListReturnsNothing(t *testing.T) {
	f := newFixture(t)

	result, err := f.fl.collect(100)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected no pages from empty list, got %v", result)
	}
}

func TestFreeList_Collect_ReturnsAllPagesAndConsumesNode(t *testing.T) {
	f := newFixture(t)
	freed := []pager.PagePointer{10, 11}

	if err := f.fl.push(1, freed); err != nil {
		t.Fatalf("push: %v", err)
	}
	nodePtr := f.fl.head
	f.save(t)

	fl2 := f.resume()
	result, err := fl2.collect(1)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}

	if !containsAll(result, freed) {
		t.Errorf("expected freed pages %v in result %v", freed, result)
	}
	if !slices.Contains(result, nodePtr) {
		t.Errorf("expected consumed node page %d in result %v", nodePtr, result)
	}
	if fl2.head != pager.NULL_PAGE {
		t.Errorf("list should be empty after full consume, got head=%d", fl2.head)
	}
}

func TestFreeList_Collect_VersionFilter_BlocksHigherVersionEntries(t *testing.T) {
	f := newFixture(t)

	if err := f.fl.push(1, []pager.PagePointer{10}); err != nil {
		t.Fatalf("push v1: %v", err)
	}
	if err := f.fl.push(3, []pager.PagePointer{20}); err != nil {
		t.Fatalf("push v3: %v", err)
	}
	f.save(t)

	fl2 := f.resume()
	result, err := fl2.collect(2) // only version ≤ 2 → only page 10
	if err != nil {
		t.Fatalf("collect: %v", err)
	}

	if !slices.Contains(result, pager.PagePointer(10)) {
		t.Errorf("expected page 10 in result, got %v", result)
	}
	if slices.Contains(result, pager.PagePointer(20)) {
		t.Errorf("page 20 (version 3) must not appear for maxVersion=2, got %v", result)
	}
	if slices.Contains(result, f.fl.head) {
		t.Errorf("node page must not appear when node is not fully consumed")
	}
}

func TestFreeList_Collect_AdvancesReadPosThroughPartialNode(t *testing.T) {
	f := newFixture(t)

	if err := f.fl.push(1, []pager.PagePointer{10}); err != nil {
		t.Fatalf("push v1: %v", err)
	}
	if err := f.fl.push(5, []pager.PagePointer{20}); err != nil {
		t.Fatalf("push v5: %v", err)
	}
	f.save(t)

	// First collect: picks up only version 1
	fl2 := f.resume()
	r1, err := fl2.collect(1)
	if err != nil {
		t.Fatalf("collect 1: %v", err)
	}
	if !slices.Contains(r1, pager.PagePointer(10)) {
		t.Errorf("first collect: expected page 10, got %v", r1)
	}
	if slices.Contains(r1, pager.PagePointer(20)) {
		t.Errorf("first collect: page 20 must not appear yet, got %v", r1)
	}

	// Second collect from the updated position: reuse fl2's pager so the
	// updated readPos (written by collect but not yet flushed) is visible.
	fl3 := newFreeList(fl2.head, fl2.tail, fl2.pager)
	r2, err := fl3.collect(5)
	if err != nil {
		t.Fatalf("collect 2: %v", err)
	}
	if !slices.Contains(r2, pager.PagePointer(20)) {
		t.Errorf("second collect: expected page 20, got %v", r2)
	}
}

func TestFreeList_Collect_MultiNodeTraversal(t *testing.T) {
	f := newFixture(t)
	cap := f.fl.nodeCapacity() // 2

	allFreed := make([]pager.PagePointer, cap+1)
	for i := range allFreed {
		allFreed[i] = pager.PagePointer(100 + i)
	}
	if err := f.fl.push(1, allFreed); err != nil {
		t.Fatalf("push: %v", err)
	}
	f.save(t)

	fl2 := f.resume()
	result, err := fl2.collect(1)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}

	if !containsAll(result, allFreed) {
		t.Errorf("all freed pages must be in result: want %v, got %v", allFreed, result)
	}
	if fl2.head != pager.NULL_PAGE {
		t.Errorf("expected empty list after consuming all nodes, head=%d", fl2.head)
	}
}

func TestFreeList_Collect_ConsumedNodePageIsReturnedForReuse(t *testing.T) {
	f := newFixture(t)

	if err := f.fl.push(1, []pager.PagePointer{10}); err != nil {
		t.Fatalf("push: %v", err)
	}
	nodePtr := f.fl.head
	f.save(t)

	fl2 := f.resume()
	result, err := fl2.collect(1)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}

	if !slices.Contains(result, nodePtr) {
		t.Errorf("consumed node page %d should be returned for reuse, got %v", nodePtr, result)
	}
}

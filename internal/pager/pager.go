package pager

import (
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/store"
)

// PagerState is the transactional snapshot: the arena's mutable state (page
// count + write buffer), the reuse bookkeeping, and the freelist cursor.
// Restore rolls all of it back on transaction abort.
type PagerState struct {
	PagesCount    PagesCount
	pageUpdates   map[PagePointer][]byte
	ownedPages    helpers.Set[PagePointer]
	reusablePages []PagePointer
	retiredPages  helpers.Set[PagePointer]
	freeListHead  PagePointer
	freeListTail  PagePointer
}

// Pager layers page-reuse policy over a PageArena. It hands out pages in
// three tiers — pages freed within this transaction (reusablePages), then
// pages freed by earlier transactions via the FreeList, then a fresh page
// grown by the arena. Because both the Pager and the FreeList share the same
// arena, and the FreeList only ever grows for its own nodes, freeing pages
// into the list never recurses back into reuse.
type Pager struct {
	arena         *PageArena
	freeList      *FreeList
	reuseVersion  uint64
	ownedPages    helpers.Set[PagePointer]
	reusablePages []PagePointer
	retiredPages  helpers.Set[PagePointer]
}

func NewPager(storage store.Storage, pagesCount PagesCount, pageSize PageSize, reusable ...[]PagePointer) *Pager {
	return newPager(NewPageArena(storage, pagesCount, pageSize), reusable...)
}

func newPager(arena *PageArena, reusable ...[]PagePointer) *Pager {
	owned := helpers.NewSet[PagePointer]()
	var reusablePages []PagePointer
	if len(reusable) > 0 {
		for _, pointer := range reusable[0] {
			owned.Add(pointer)
			reusablePages = append(reusablePages, pointer)
		}
	}

	return &Pager{
		arena:         arena,
		ownedPages:    owned,
		reusablePages: reusablePages,
		retiredPages:  helpers.NewSet[PagePointer](),
	}
}

// Store exposes the underlying arena so a FreeList can be built on the very
// same instance this Pager allocates through.
func (pager *Pager) Store() *PageArena {
	return pager.arena
}

// UseFreeList wires a FreeList as the cross-transaction reuse source, bound
// to the version below which pages are safe to reuse.
func (pager *Pager) UseFreeList(freeList *FreeList, reuseVersion uint64) {
	pager.freeList = freeList
	pager.reuseVersion = reuseVersion
}

func (pager *Pager) FreeList() *FreeList {
	return pager.freeList
}

func (pager *Pager) Page(pointer PagePointer) []byte {
	return pager.arena.Page(pointer)
}

func (pager *Pager) UpdatePage(pointer PagePointer, data []byte) error {
	return pager.arena.UpdatePage(pointer, data)
}

func (pager *Pager) CreatePage(data []byte) PagePointer {
	// Freed within this transaction: cheapest and always safe to reuse.
	if pointer, ok := pager.popReusable(); ok {
		pager.arena.pageUpdates[pointer] = data
		return pointer
	}

	// Freed by an earlier transaction and now past the reuse version.
	if pointer, ok := pager.popFreeList(); ok {
		pager.ownedPages.Add(pointer)
		pager.arena.pageUpdates[pointer] = data
		return pointer
	}

	// Freelist is dry: grow the file.
	pointer := pager.arena.CreatePage(data)
	pager.ownedPages.Add(pointer)
	return pointer
}

func (pager *Pager) popReusable() (PagePointer, bool) {
	last := len(pager.reusablePages) - 1
	if last < 0 {
		return NULL_PAGE, false
	}
	pointer := pager.reusablePages[last]
	pager.reusablePages = pager.reusablePages[:last]
	return pointer, true
}

func (pager *Pager) popFreeList() (PagePointer, bool) {
	if pager.freeList == nil {
		return NULL_PAGE, false
	}
	return pager.freeList.pop(pager.reuseVersion)
}

func (pager *Pager) FreePage(pointer PagePointer) {
	if pager.ownedPages.Has(pointer) {
		pager.reusablePages = append(pager.reusablePages, pointer)
	} else {
		pager.retiredPages.Add(pointer)
	}

	delete(pager.arena.pageUpdates, pointer)
}

func (pager *Pager) RetiredPages() []PagePointer {
	return pager.retiredPages.Values()
}

func (pager *Pager) ReusablePages() []PagePointer {
	return append([]PagePointer(nil), pager.reusablePages...)
}

func (pager *Pager) PageSize() PageSize {
	return pager.arena.pageSize
}

func (pager *Pager) PagesCount() PagesCount {
	return pager.arena.pagesCount
}

func (pager *Pager) SaveChanges() error {
	return pager.arena.SaveChanges()
}

func (pager *Pager) Snapshot() PagerState {
	pageUpdates := make(map[PagePointer][]byte, len(pager.arena.pageUpdates))
	for pointer, page := range pager.arena.pageUpdates {
		pageUpdates[pointer] = page
	}

	state := PagerState{
		PagesCount:    pager.arena.pagesCount,
		pageUpdates:   pageUpdates,
		ownedPages:    helpers.NewSet(pager.ownedPages.Values()...),
		reusablePages: append([]PagePointer(nil), pager.reusablePages...),
		retiredPages:  helpers.NewSet(pager.retiredPages.Values()...),
	}
	if pager.freeList != nil {
		state.freeListHead = pager.freeList.Head
		state.freeListTail = pager.freeList.Tail
	}
	return state
}

func (pager *Pager) Restore(state PagerState) {
	pager.arena.pagesCount = state.PagesCount
	pager.arena.pageUpdates = state.pageUpdates
	pager.ownedPages = state.ownedPages
	pager.reusablePages = state.reusablePages
	pager.retiredPages = state.retiredPages
	if pager.freeList != nil {
		pager.freeList.Head = state.freeListHead
		pager.freeList.Tail = state.freeListTail
	}
}

func (pager *Pager) Fork(nextPageID PagePointer, reusable ...[]PagePointer) *Pager {
	return NewPager(pager.arena.storage, nextPageID, pager.arena.pageSize, reusable...)
}

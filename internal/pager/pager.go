package pager

import (
	"distributed-storage/internal/store"
	"fmt"
)

const NULL_PAGE = PagePointer(0)

type PagePointer = uint64
type PagesCount = uint64
type PageSize = int

type PagerConfig struct {
	pageSize PageSize
}

type PagerState struct {
	PagesCount    PagesCount             // Number of all pages in the pager
	OwnedPages    PageList               // Pages owned by this pager and safe for in-place mutation.
	ReusablePages PageList               // Pages not currently in use but owned by this pager.
	RetiredPages  PageList               // Pages no longer owned by this pager instance.
	pageUpdates   map[PagePointer][]byte // Map of page updates that will be synced with storage
}

type Pager struct {
	storage store.Storage
	config  PagerConfig
	state   PagerState
}

func NewPager(storage store.Storage, pagesCount PagesCount, pageSize PageSize, pages ...PageList) *Pager {
	var owned PageList

	if len(pages) > 0 {
		owned = pages[0]
	} else {
		owned = NewPageList()
	}

	return &Pager{
		storage: storage,
		config: PagerConfig{
			pageSize: pageSize,
		},
		state: PagerState{
			PagesCount:    pagesCount,
			OwnedPages:    owned.Clone(),
			ReusablePages: owned.Clone(),
			RetiredPages:  NewPageList(),
			pageUpdates:   map[PagePointer][]byte{},
		},
	}
}

func (pager *Pager) Page(pointer PagePointer) []byte {
	if page, exist := pager.state.pageUpdates[pointer]; exist {
		return page
	}

	return pager.storage.Segment(int(pointer)*int(pager.config.pageSize), int(pager.config.pageSize))
}

func (pager *Pager) UpdatePage(pointer PagePointer, data []byte) error {
	if pointer >= pager.state.PagesCount {
		return fmt.Errorf("Pager: invalid page pointer %d (next page ID: %d)", pointer, pager.state.PagesCount)
	}

	pager.state.pageUpdates[pointer] = data
	return nil
}

func (pager *Pager) CreatePage(data []byte) PagePointer {
	var pagePointer PagePointer

	if availablePage, ok := pager.state.ReusablePages.Pop(); ok {
		pagePointer = availablePage
	} else {
		pagePointer = pager.state.PagesCount
		pager.state.OwnedPages.Add(pagePointer)

		pager.state.PagesCount++
	}

	pager.state.pageUpdates[pagePointer] = data
	return pagePointer
}

func (pager *Pager) FreePage(pointer PagePointer) {
	// If page was in mutable pool we can return it to freed pages because we can mutate it later
	if pager.state.OwnedPages.Has(pointer) {
		pager.state.ReusablePages.Add(pointer)
	} else {
		pager.state.RetiredPages.Add(pointer)
	}

	delete(pager.state.pageUpdates, pointer)
}

func (pager *Pager) RetiredPages() PageList {
	return pager.state.RetiredPages
}

func (pager *Pager) ReusablePages() PageList {
	return pager.state.ReusablePages
}

func (pager *Pager) PagesCount() uint64 {
	return pager.state.PagesCount
}

func (pager *Pager) SaveChanges() error {
	updates := make([]store.SegmentUpdate, 0, len(pager.state.pageUpdates))

	for pointer, page := range pager.state.pageUpdates {

		updates = append(updates,
			store.SegmentUpdate{
				Offset: int(pointer) * int(pager.config.pageSize),
				Data:   page[:min(len(page), int(pager.config.pageSize))],
			},
		)
	}

	if err := pager.storage.UpdateSegments(updates); err != nil {
		return fmt.Errorf("Pager: failed to save changes: %w", err)
	}

	pager.state.pageUpdates = map[PagePointer][]byte{}

	return nil
}

func (pager *Pager) Snapshot() PagerState {
	pageUpdates := make(map[PagePointer][]byte, len(pager.state.pageUpdates))

	for pointer, page := range pager.state.pageUpdates {
		pageUpdates[pointer] = page
	}

	return PagerState{
		PagesCount:    pager.state.PagesCount,
		OwnedPages:    pager.state.OwnedPages.Clone(),
		ReusablePages: pager.state.ReusablePages.Clone(),
		RetiredPages:  pager.state.RetiredPages.Clone(),
		pageUpdates:   pageUpdates,
	}
}

func (pager *Pager) Restore(state PagerState) {
	pager.state = state
}

func (pager *Pager) Fork(nextPageID PagePointer, mutable ...PageList) *Pager {
	return NewPager(pager.storage, nextPageID, pager.config.pageSize, mutable...)
}

package pager

import (
	"distributed-storage/internal/store"
	"fmt"
)

const NULL_PAGE = PagePointer(0)

type PagePointer = uint64

type PagerConfig struct {
	pageSize int
}

type PagerState struct {
	TotalPages     uint64                 // Total number of pages in storage
	AvailablePages PageList               // List  of pages that are not reachable by others and could be mutated
	ReusablePages  PageList               // List of pages that can be reused by others
	ReleasedPages  PageList               // List of pages that were released and cannot be overwritten due to immutability
	pageUpdates    map[PagePointer][]byte // Map of page updates that will be synced with storage
}

type Pager struct {
	storage store.Storage
	config  PagerConfig
	state   PagerState
}

func NewPager(storage store.Storage, pagesNumber uint64, pageSize int, availablePages ...PageList) *Pager {
	pages := NewPageList()
	if len(availablePages) > 0 {
		pages = availablePages[0]
	}

	return &Pager{
		storage: storage,
		config: PagerConfig{
			pageSize: pageSize,
		},
		state: PagerState{
			TotalPages:     pagesNumber,
			AvailablePages: pages.Clone(),
			ReusablePages:  pages.Clone(),
			ReleasedPages:  NewPageList(),
			pageUpdates:    map[PagePointer][]byte{},
		},
	}
}

func (pager *Pager) Page(pointer PagePointer) []byte {
	if page, exist := pager.state.pageUpdates[pointer]; exist {
		return page
	}

	return pager.storage.Segment(int(pointer)*pager.config.pageSize, pager.config.pageSize)
}

func (pager *Pager) UpdatePage(pointer PagePointer, data []byte) error {
	if pointer > pager.state.TotalPages {
		return fmt.Errorf("Pager: invalid page pointer %d (total pages: %d)", pointer, pager.state.TotalPages)
	}

	pager.state.pageUpdates[pointer] = data
	return nil
}

func (pager *Pager) CreatePage(data []byte) PagePointer {
	var pagePointer PagePointer

	if availablePage, ok := pager.state.ReusablePages.Pop(); ok {
		pagePointer = availablePage
	} else {
		pager.state.TotalPages++

		pagePointer = pager.state.TotalPages
		pager.state.AvailablePages.Add(pagePointer)
	}

	pager.state.pageUpdates[pagePointer] = data
	return pagePointer
}

func (pager *Pager) FreePage(pointer PagePointer) {
	// If released page was in page pool we can return it to reusable pages because nobody can reference this page
	if pager.state.AvailablePages.Has(pointer) {
		pager.state.ReusablePages.Add(pointer)
	} else {
		pager.state.ReleasedPages.Add(pointer)
	}

	delete(pager.state.pageUpdates, pointer)
}

func (pager *Pager) ReleasedPages() PageList {
	return pager.state.ReleasedPages
}

func (pager *Pager) ReusablePages() PageList {
	return pager.state.ReusablePages
}

func (pager *Pager) TotalPages() uint64 {
	return pager.state.TotalPages
}

func (pager *Pager) SaveChanges() error {
	updates := make([]store.SegmentUpdate, 0, len(pager.state.pageUpdates))

	for pointer, page := range pager.state.pageUpdates {

		updates = append(updates,
			store.SegmentUpdate{
				Offset: int(pointer) * pager.config.pageSize,
				Data:   page[:min(len(page), pager.config.pageSize)],
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
		TotalPages:     pager.state.TotalPages,
		AvailablePages: pager.state.AvailablePages.Clone(),
		ReusablePages:  pager.state.ReusablePages.Clone(),
		ReleasedPages:  pager.state.ReleasedPages.Clone(),
		pageUpdates:    pageUpdates,
	}
}

func (pager *Pager) Restore(state PagerState) {
	pager.state = state
}

func (pager *Pager) Fork(pagesCount uint64, availablePages ...PageList) *Pager {
	return NewPager(pager.storage, pagesCount, pager.config.pageSize, availablePages...)
}

package pager

import (
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/store"
	"fmt"
)

const NULL_PAGE = PagePointer(0)

type PagePointer = uint64

type PagerConfig struct {
	pageSize int
}

type PagerState struct {
	TotalPages    uint64                   // Total number of pages in storage
	PagePool      helpers.Set[PagePointer] // Total set of pages that are not reachable by others and could be reused
	ReusablePages helpers.Set[PagePointer] // Set of pages that are prepared for reuse
	ReleasedPages helpers.Set[PagePointer] // Set of pages that were released and cannot be overwritten due to immutability
	pageUpdates   map[PagePointer][]byte   // Map of page updates that will be synced with storage
}

type Pager struct {
	storage store.Storage
	config  PagerConfig
	state   PagerState
}

func NewPager(storage store.Storage, pagesNumber uint64, pageSize int, availablePages ...PagePointer) *Pager {
	return &Pager{
		storage: storage,
		config: PagerConfig{
			pageSize: pageSize,
		},
		state: PagerState{
			TotalPages:    pagesNumber,
			PagePool:      helpers.NewSet(availablePages...),
			ReusablePages: helpers.NewSet(availablePages...),
			ReleasedPages: helpers.NewSet[PagePointer](),
			pageUpdates:   map[PagePointer][]byte{},
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
		pager.state.PagePool.Add(pagePointer)
	}

	pager.state.pageUpdates[pagePointer] = data
	return pagePointer
}

func (pager *Pager) FreePage(pointer PagePointer) {
	// If released page was in page pool we can return it to reusable pages because nobody can reference this page
	if pager.state.PagePool.Has(pointer) {
		pager.state.ReusablePages.Add(pointer)
	} else {
		pager.state.ReleasedPages.Add(pointer)
	}

	delete(pager.state.pageUpdates, pointer)
}

func (pager *Pager) ReleasedPages() []PagePointer {
	return pager.state.ReleasedPages.Values()
}

func (pager *Pager) ReusablePages() []PagePointer {
	return pager.state.ReusablePages.Values()
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
				Data:   page[:pager.config.pageSize],
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
	for k, v := range pager.state.pageUpdates {
		pageUpdates[k] = v
	}

	return PagerState{
		TotalPages:    pager.state.TotalPages,
		PagePool:      helpers.NewSet(pager.state.PagePool.Values()...),
		ReusablePages: helpers.NewSet(pager.state.ReusablePages.Values()...),
		ReleasedPages: helpers.NewSet(pager.state.ReleasedPages.Values()...),
		pageUpdates:   pageUpdates,
	}
}

func (pager *Pager) Restore(state PagerState) {
	pager.state = state
}

func (pager *Pager) Fork(pagesCount uint64, availablePages ...PagePointer) *Pager {
	return NewPager(pager.storage, pagesCount, pager.config.pageSize, availablePages...)
}

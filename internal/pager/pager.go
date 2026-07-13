package pager

import (
	"distributed-storage/internal/helpers"
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
	PagesCount    PagesCount
	ownedPages    helpers.Set[PagePointer]
	reusablePages helpers.Set[PagePointer]
	retiredPages  helpers.Set[PagePointer]
	pageUpdates   map[PagePointer][]byte
}

type Pager struct {
	storage store.Storage
	config  PagerConfig
	state   PagerState
}

func NewPager(storage store.Storage, pagesCount PagesCount, pageSize PageSize, reusable ...[]PagePointer) *Pager {
	owned := helpers.NewSet[PagePointer]()
	reusableSet := helpers.NewSet[PagePointer]()
	if len(reusable) > 0 {
		for _, p := range reusable[0] {
			owned.Add(p)
			reusableSet.Add(p)
		}
	}

	return &Pager{
		storage: storage,
		config:  PagerConfig{pageSize: pageSize},
		state: PagerState{
			PagesCount:    pagesCount,
			ownedPages:    owned,
			reusablePages: reusableSet,
			retiredPages:  helpers.NewSet[PagePointer](),
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

	if p, ok := pager.state.reusablePages.Pop(); ok {
		pagePointer = p
	} else {
		pagePointer = pager.state.PagesCount
		pager.state.ownedPages.Add(pagePointer)
		pager.state.PagesCount++
	}

	pager.state.pageUpdates[pagePointer] = data
	return pagePointer
}

func (pager *Pager) FreePage(pointer PagePointer) {
	if pager.state.ownedPages.Has(pointer) {
		pager.state.reusablePages.Add(pointer)
	} else {
		pager.state.retiredPages.Add(pointer)
	}

	delete(pager.state.pageUpdates, pointer)
}

func (pager *Pager) RetiredPages() []PagePointer {
	return pager.state.retiredPages.Values()
}

func (pager *Pager) ReusablePages() []PagePointer {
	return pager.state.reusablePages.Values()
}

func (pager *Pager) PageSize() PageSize {
	return pager.config.pageSize
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
		ownedPages:    helpers.NewSet(pager.state.ownedPages.Values()...),
		reusablePages: helpers.NewSet(pager.state.reusablePages.Values()...),
		retiredPages:  helpers.NewSet(pager.state.retiredPages.Values()...),
		pageUpdates:   pageUpdates,
	}
}

func (pager *Pager) Restore(state PagerState) {
	pager.state = state
}

func (pager *Pager) Fork(nextPageID PagePointer, reusable ...[]PagePointer) *Pager {
	return NewPager(pager.storage, nextPageID, pager.config.pageSize, reusable...)
}

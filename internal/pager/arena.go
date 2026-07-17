package pager

import (
	"distributed-storage/internal/store"
	"fmt"
)

const NULL_PAGE = PagePointer(0)

type PagePointer = uint64
type PagesCount = uint64
type PageSize = int

type PageArena struct {
	storage     store.Storage
	pageSize    PageSize
	pagesCount  PagesCount
	pageUpdates map[PagePointer][]byte
}

func NewPageArena(storage store.Storage, pagesCount PagesCount, pageSize PageSize) *PageArena {
	return &PageArena{
		storage:     storage,
		pageSize:    pageSize,
		pagesCount:  pagesCount,
		pageUpdates: map[PagePointer][]byte{},
	}
}

func (s *PageArena) Page(pointer PagePointer) []byte {
	if page, exist := s.pageUpdates[pointer]; exist {
		return page
	}

	return s.storage.Segment(int(pointer)*int(s.pageSize), int(s.pageSize))
}

func (s *PageArena) CreatePage(data []byte) PagePointer {
	pointer := s.pagesCount
	s.pagesCount++
	s.pageUpdates[pointer] = data
	return pointer
}

func (s *PageArena) UpdatePage(pointer PagePointer, data []byte) error {
	if pointer >= s.pagesCount {
		return fmt.Errorf("PageArena: invalid page pointer %d (next page ID: %d)", pointer, s.pagesCount)
	}

	s.pageUpdates[pointer] = data
	return nil
}

// SaveChanges flushes the buffered page writes to storage and clears the
// buffer. This is the only place that touches the underlying storage.
func (s *PageArena) SaveChanges() error {
	updates := make([]store.SegmentUpdate, 0, len(s.pageUpdates))

	for pointer, page := range s.pageUpdates {
		updates = append(updates,
			store.SegmentUpdate{
				Offset: int(pointer) * int(s.pageSize),
				Data:   page[:min(len(page), int(s.pageSize))],
			},
		)
	}

	if err := s.storage.UpdateSegments(updates); err != nil {
		return fmt.Errorf("PageArena: failed to save changes: %w", err)
	}

	s.pageUpdates = map[PagePointer][]byte{}

	return nil
}

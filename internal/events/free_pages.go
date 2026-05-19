package events

import "distributed-storage/internal/pager"

type FreePages struct {
	Version uint64
	List    pager.PageList
}

func NewFreePages(dbVersion uint64, list pager.PageList) *FreePages {
	return &FreePages{Version: dbVersion, List: list}
}

func (event *FreePages) Type() EventType {
	return FREE_PAGES_EVENT
}

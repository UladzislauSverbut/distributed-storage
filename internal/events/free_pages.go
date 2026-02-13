package events

import (
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/pager"
	"strconv"
)

const FREE_PAGES_EVENT = "FREE_PAGES"

type FreePages struct {
	Version uint64
	Pages   []pager.PagePointer
}

func NewFreePages(dbVersion uint64, pages []pager.PagePointer) *FreePages {
	return &FreePages{Version: dbVersion, Pages: pages}
}

func (event *FreePages) Name() string {
	return FREE_PAGES_EVENT
}

func (event *FreePages) Serialize() []byte {
	return []byte(event.Name() + "(DB_V=" + strconv.FormatUint(event.Version, 10) + ",PAGES=" + helpers.JoinFunc(event.Pages, func(page uint64) string { return strconv.FormatUint(page, 10) }, ",") + ")\n")
}

func (event *FreePages) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

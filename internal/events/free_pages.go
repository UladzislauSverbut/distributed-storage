package events

import (
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/pager"
	"strconv"
)

const FREE_PAGES_EVENT = "FREE_PAGES"

type FreePages struct {
	TxID  TxID
	Pages []pager.PagePointer
}

func (e *FreePages) Name() string {
	return FREE_PAGES_EVENT
}

func (e *FreePages) Serialize() []byte {
	return []byte(e.Name() + "(PAGES=" + helpers.StringifySlice(e.Pages, func(page uint64) string { return strconv.FormatUint(page, 10) }, ",") + ")\n")
}

func (e *FreePages) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

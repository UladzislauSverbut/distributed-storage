package events

import (
	"bytes"
	"distributed-storage/internal/pager"
	"encoding/binary"
	"errors"
)

const FREE_PAGES_EVENT = "FREE_PAGES"

var freePagesParsingError = errors.New("FreePages: couldn't parse event")

type FreePages struct {
	Version uint64
	List    pager.PageList
}

func NewFreePages(dbVersion uint64, list pager.PageList) *FreePages {
	return &FreePages{Version: dbVersion, List: list}
}

func (event *FreePages) Name() string {
	return FREE_PAGES_EVENT
}

func (event *FreePages) Serialize() []byte {
	serializedEvent := []byte(event.Name())
	version := make([]byte, 8)
	pages := make([]byte, len(event.List.Pages())*16) // Each page range consists of two uint64 values (start and end)

	for idx, interval := range event.List.Pages() {
		binary.LittleEndian.PutUint64(pages[idx*16:], interval.Start)
		binary.LittleEndian.PutUint64(pages[idx*16+8:], interval.End)
	}

	binary.LittleEndian.PutUint64(version, event.Version)

	serializedEvent = append(serializedEvent, version...)
	serializedEvent = append(serializedEvent, pages...)

	return serializedEvent
}

func ParseFreePages(data []byte) (*FreePages, error) {
	offset := len(FREE_PAGES_EVENT)

	if !bytes.Equal(data[:offset], []byte(FREE_PAGES_EVENT)) {
		return nil, freePagesParsingError
	}

	serializedVersion := data[offset : offset+8]
	serializedPages := data[offset+8:]

	version := binary.LittleEndian.Uint64(serializedVersion)
	pages := make([]pager.PageInterval, len(serializedPages)/16)

	for idx := 0; idx < len(serializedPages)/16; idx++ {
		start := binary.LittleEndian.Uint64(serializedPages[idx*16:])
		end := binary.LittleEndian.Uint64(serializedPages[idx*16+8:])
		pages[idx] = pager.PageInterval{Start: start, End: end}
	}

	return &FreePages{
		Version: version,
		List:    pager.NewPageList(pages...),
	}, nil

}

package events

import (
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/pager"
	"encoding/binary"
	"errors"
)

const FREE_PAGES_EVENT = "FREE_PAGES"

var freePagesParsingError = errors.New("FreePages: couldn't parse event")

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
	serializedEvent := []byte(event.Name())
	version := make([]byte, 8)
	pages := make([]byte, len(event.Pages)*8)

	for idx, page := range event.Pages {
		binary.LittleEndian.PutUint64(pages[idx*8:], page)
	}

	binary.LittleEndian.PutUint64(version, event.Version)

	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, version...)
	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, pages...)

	return serializedEvent
}

func ParseFreePages(data []byte) (*FreePages, error) {
	parts := helpers.SplitBy(data, ' ')

	if len(parts) != 3 || string(parts[0]) != FREE_PAGES_EVENT {
		return nil, freePagesParsingError
	}

	serializedVersion := parts[1]
	serializedPages := parts[2]

	version := binary.LittleEndian.Uint64(serializedVersion)
	pages := make([]pager.PagePointer, (len(serializedPages) / 8))

	for idx := 0; idx < len(serializedPages)/8; idx++ {
		pages[idx] = binary.LittleEndian.Uint64(serializedPages[idx*8:])
	}

	return &FreePages{
		Version: version,
		Pages:   pages,
	}, nil

}

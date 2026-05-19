package wal

import (
	"fmt"
	"io"
	"os"
)

const META_ARCHIVE_NAME = "meta.wal"
const MAX_DESCRIPTORS = 20
const INITIAL_SEGMENT_ID SegmentID = 1
const INITIAL_LAST_ENTRY_INDEX EntryIndex = 0

type SegmentDescriptor struct {
	ID        SegmentID
	LastIndex EntryIndex
}

type SegmentStore struct {
	Directory       string
	ActiveSegment   *Segment
	ArchiveSegments map[SegmentID]*Segment
	Descriptors     []SegmentDescriptor
	MetaFile        *os.File
	LastEntryIndex  EntryIndex

	codec Codec
}

func NewSegmentStore(directory string) (*SegmentStore, error) {
	store := &SegmentStore{
		Directory:       directory,
		ActiveSegment:   nil,
		ArchiveSegments: make(map[SegmentID]*Segment),
		Descriptors:     []SegmentDescriptor{},
	}

	if err := store.openMetaFile(); err != nil {
		return nil, fmt.Errorf("SegmentStore: failed to initialize segment store: %w", err)
	}

	if err := store.readMetaFile(); err != nil {
		return nil, fmt.Errorf("SegmentStore: failed to read meta file during initialization: %w", err)
	}

	if err := store.openActiveSegment(); err != nil {
		return nil, fmt.Errorf("SegmentStore: failed to open active segment during initialization: %w", err)
	}

	return store, nil
}

func (store *SegmentStore) openMetaFile() error {
	fileName := fmt.Sprintf("%s/%s", store.Directory, META_ARCHIVE_NAME)
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)

	if err != nil {
		return fmt.Errorf("SegmentStore: failed to open meta archive file: %w", err)
	}

	store.MetaFile = file
	return nil
}

func (store *SegmentStore) readMetaFile() error {
	archive, err := io.ReadAll(store.MetaFile)
	if err != nil {
		return fmt.Errorf("SegmentStore: failed to read meta archive file: %w", err)
	}

	if len(archive) == 0 {
		store.Descriptors = append(store.Descriptors, SegmentDescriptor{
			ID:        INITIAL_SEGMENT_ID,
			LastIndex: INITIAL_LAST_ENTRY_INDEX,
		})

		return nil
	}

	store.Descriptors, err = store.codec.decodeSegmentDescriptors(archive)

	if err != nil {
		return fmt.Errorf("SegmentStore: failed to decode segment descriptors from meta archive: %w", err)
	}

	store.LastEntryIndex = store.Descriptors[len(store.Descriptors)-1].LastIndex

	return nil
}

func (store *SegmentStore) openActiveSegment() error {
	lastSegmentDescriptorIndex := len(store.Descriptors) - 1
	segmentID := store.Descriptors[lastSegmentDescriptorIndex].ID

	segment, lastIndex, err := NewSegment(store.Directory, segmentID)
	if err != nil {
		return fmt.Errorf("SegmentStore: failed to open active segment: %w", err)
	}

	store.ActiveSegment = segment
	store.LastEntryIndex = lastIndex
	store.Descriptors[lastSegmentDescriptorIndex].LastIndex = lastIndex

	return nil
}

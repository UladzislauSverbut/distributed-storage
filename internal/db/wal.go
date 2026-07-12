package db

import (
	"distributed-storage/internal/catalog"
	"distributed-storage/internal/events"
	"distributed-storage/internal/pager"
	transaction "distributed-storage/internal/tx"
	walPkg "distributed-storage/internal/wal"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
)

type WAL struct {
	inner            *walPkg.WAL
	directory        string
	archiveDirectory string
}

func newWAL(config walPkg.WALConfig) (*WAL, error) {
	inner, err := walPkg.NewWAL(config)
	if err != nil {
		return nil, err
	}
	return &WAL{
		inner:            inner,
		directory:        config.Directory,
		archiveDirectory: config.ArchiveDirectory,
	}, nil
}

func (w *WAL) appendTransactions(commits []transaction.TransactionCommit) {
	for _, commit := range commits {
		w.inner.Append(EncodeEvent(events.NewStartTransaction()))
		for _, event := range commit.ChangeEvents {
			w.inner.Append(EncodeEvent(event))
		}
		w.inner.Append(EncodeEvent(events.NewCommitTransaction()))
	}
}

func (w *WAL) appendVersionUpdate(version catalog.DatabaseVersion) {
	w.inner.Append(EncodeEvent(events.NewUpdateDBVersion(uint64(version))))
}

func (w *WAL) appendFreePages(version catalog.DatabaseVersion, pages pager.PageList) {
	w.inner.Append(EncodeEvent(events.NewFreePages(uint64(version), pages)))
}

func (w *WAL) sync() (walPkg.EntryIndex, error) {
	return w.inner.Sync()
}

func (w *WAL) empty() bool {
	return w.inner.Empty()
}

func (w *WAL) eventsSince(version catalog.DatabaseVersion) ([]events.Event, walPkg.EntryIndex, error) {
	records, lastIndex, err := w.readAllRecords()
	if err != nil {
		return nil, 0, err
	}

	var allEvents []events.Event
	for _, record := range records {
		event, err := DecodeEvent(record)
		if err != nil {
			return nil, 0, fmt.Errorf("WAL: failed to decode event: %w", err)
		}
		allEvents = append(allEvents, event)
	}

	// Find the last UpdateDBVersion(version) marker and return everything after it
	lastIdx := -1
	for i, event := range allEvents {
		if v, ok := event.(*events.UpdateDBVersion); ok && catalog.DatabaseVersion(v.Version) == version {
			lastIdx = i
		}
	}

	if lastIdx < 0 {
		return nil, 0, nil
	}

	return allEvents[lastIdx:], lastIndex, nil
}

func (w *WAL) readAllRecords() ([][]byte, walPkg.EntryIndex, error) {
	var all [][]byte
	var lastIndex walPkg.EntryIndex

	for _, dir := range []string{w.archiveDirectory, w.directory} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, 0, fmt.Errorf("WAL: failed to read directory %q: %w", dir, err)
		}

		var segmentFiles []string
		for _, entry := range entries {
			if _, err := parseSegmentName(entry.Name()); err == nil {
				segmentFiles = append(segmentFiles, dir+"/"+entry.Name())
			}
		}
		sort.Strings(segmentFiles)

		for _, path := range segmentFiles {
			records, idx, err := readSegmentFile(path)
			if err != nil {
				return nil, 0, err
			}
			all = append(all, records...)
			lastIndex = idx
		}
	}

	return all, lastIndex, nil
}

func readSegmentFile(path string) ([][]byte, walPkg.EntryIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, fmt.Errorf("WAL: failed to read segment %q: %w", path, err)
	}

	var records [][]byte
	var lastIndex walPkg.EntryIndex
	for len(data) > 0 {
		index, entry, offset, err := decodeSegmentEntry(data)
		if err != nil {
			return nil, 0, fmt.Errorf("WAL: failed to decode segment entry: %w", err)
		}
		records = append(records, entry)
		lastIndex = index
		data = data[offset:]
	}

	return records, lastIndex, nil
}

func parseSegmentName(name string) (walPkg.EntryIndex, error) {
	var index walPkg.EntryIndex
	if _, err := fmt.Sscanf(name, "%020d.wal", &index); err != nil {
		return 0, err
	}
	return index, nil
}

func decodeSegmentEntry(data []byte) (index walPkg.EntryIndex, entry []byte, nextOffset int, err error) {
	if len(data) < 16 {
		err = fmt.Errorf("WAL: insufficient data to decode entry")
		return
	}
	index = walPkg.EntryIndex(binary.LittleEndian.Uint64(data[0:8]))
	length := binary.LittleEndian.Uint32(data[8:12])
	checksum := binary.LittleEndian.Uint32(data[12:16])
	entry = data[16 : 16+int(length)]
	if checksum != crc32.ChecksumIEEE(entry) {
		err = fmt.Errorf("WAL: entry checksum mismatch")
		return
	}
	return index, entry, 16 + int(length), nil
}

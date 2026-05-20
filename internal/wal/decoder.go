package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

func encodeSegmentEntry(index EntryIndex, entry []byte) []byte {
	var out []byte

	serializedIndex := make([]byte, 8)
	serializedLength := make([]byte, 4)
	checksum := make([]byte, 4)

	binary.LittleEndian.PutUint64(serializedIndex, uint64(index))
	binary.LittleEndian.PutUint32(serializedLength, uint32(len(entry)))
	binary.LittleEndian.PutUint32(checksum, crc32.ChecksumIEEE(entry))

	out = append(out, serializedIndex...)
	out = append(out, serializedLength...)
	out = append(out, checksum...)
	return append(out, entry...)

}

func decodeSegmentEntry(data []byte) (index EntryIndex, entry []byte, nextOffset int, err error) {
	if len(data) < 16 {
		err = fmt.Errorf("DecodeWALEntry: insufficient data to decode WAL entry")
		return
	}

	index = EntryIndex(binary.LittleEndian.Uint64(data[0:8]))
	length := binary.LittleEndian.Uint32(data[8:12])
	checksum := binary.LittleEndian.Uint32(data[12:16])
	entry = data[16 : 16+length]

	if checksum != crc32.ChecksumIEEE(entry) {
		err = fmt.Errorf("DecodeWALEntry: entry checksum mismatch")
		return
	}

	return index, entry, 16 + int(length), err
}

func encodeSegmentName(startIndex EntryIndex) string {
	return fmt.Sprintf("%020d.wal", startIndex)
}

func parseSegmentName(name string) (EntryIndex, error) {
	var index EntryIndex

	if _, err := fmt.Sscanf(name, "%020d.wal", &index); err != nil {
		return 0, fmt.Errorf("parseSegmentName: failed to parse segment name '%s': %w", name, err)
	}

	return index, nil
}

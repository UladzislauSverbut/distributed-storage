package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type Codec struct{}

func (c *Codec) encodeWALEntry(index EntryIndex, entry []byte) []byte {
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

func (c *Codec) decodeWALEntry(data []byte) (index EntryIndex, entry []byte, nextOffset int, err error) {
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

func (c *Codec) encodeSegmentDescriptors(descriptors []SegmentDescriptor) []byte {
	var out []byte

	for _, descriptor := range descriptors {
		serializedID := make([]byte, 8)
		serializedLastIndex := make([]byte, 8)

		binary.LittleEndian.PutUint64(serializedID, uint64(descriptor.ID))
		binary.LittleEndian.PutUint64(serializedLastIndex, uint64(descriptor.LastIndex))

		out = append(out, serializedID...)
		out = append(out, serializedLastIndex...)
	}

	checksum := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksum, crc32.ChecksumIEEE(out))

	return append(checksum, out...)
}

func (c *Codec) decodeSegmentDescriptors(data []byte) (descriptors []SegmentDescriptor, err error) {
	if len(data) < 4 {
		err = fmt.Errorf("DecodeSegmentDescriptors: insufficient data to decode segment descriptors")
		return
	}

	checksum := binary.LittleEndian.Uint32(data[0:4])
	descriptorsData := data[4:]

	if checksum != crc32.ChecksumIEEE(descriptorsData) {
		err = fmt.Errorf("DecodeSegmentDescriptors: segment descriptors checksum mismatch")
		return
	}

	for offset := 0; offset < len(descriptorsData); {
		id := SegmentID(binary.LittleEndian.Uint64(descriptorsData[offset : offset+8]))

		offset += 8
		lastIndex := EntryIndex(binary.LittleEndian.Uint64(descriptorsData[offset : offset+8]))
		offset += 8

		descriptors = append(descriptors, SegmentDescriptor{
			ID:        id,
			LastIndex: lastIndex,
		})
	}

	return descriptors, nil
}

package codec

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

func EncodeWALEntry(index uint64, entry []byte) []byte {
	var out []byte

	serializedIndex := make([]byte, 8)
	serializedLength := make([]byte, 4)
	serializedChecksum := make([]byte, 4)

	binary.LittleEndian.PutUint64(serializedIndex, index)
	binary.LittleEndian.PutUint32(serializedLength, uint32(len(entry)))
	binary.LittleEndian.PutUint32(serializedChecksum, crc32.ChecksumIEEE(entry))

	out = append(out, serializedIndex...)
	out = append(out, serializedLength...)
	out = append(out, serializedChecksum...)

	return append(out, entry...)
}

func DecodeWALEntry(data []byte) (index uint64, entry []byte, nextOffset int, err error) {
	if len(data) < 16 {
		err = fmt.Errorf("DecodeWALEntry: insufficient data to decode WAL entry")
		return
	}

	index = binary.LittleEndian.Uint64(data[0:8])
	length := binary.LittleEndian.Uint32(data[8:12])
	checksum := binary.LittleEndian.Uint32(data[12:16])
	entry = data[16 : 16+length]

	if checksum != crc32.ChecksumIEEE(entry) {
		err = fmt.Errorf("DecodeWALEntry: entry checksum mismatch")
		return
	}

	return index, entry, 16 + int(length), err
}

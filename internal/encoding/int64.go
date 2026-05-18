package encoding

import "encoding/binary"

type Int64Encoder struct{}

func (Int64Encoder) Encode(v int64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, uint64(v)+(1<<63))
	return out
}

func (Int64Encoder) Decode(data []byte) (int64, int) {
	return int64(binary.LittleEndian.Uint64(data) + (1 << 63)), 8
}

var Int64 = Int64Encoder{}

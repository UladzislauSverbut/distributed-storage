package encoding

import "encoding/binary"

type Uint64Encoder struct{}

func (Uint64Encoder) Encode(v uint64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, v)
	return out
}

func (Uint64Encoder) Decode(data []byte) (uint64, int) {
	return binary.LittleEndian.Uint64(data), 8
}

var Uint64 = Uint64Encoder{}

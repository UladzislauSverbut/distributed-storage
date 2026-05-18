package encoding

import "encoding/binary"

type Int32Encoder struct{}

func (Int32Encoder) Encode(v int32) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, uint32(v)+(1<<31))
	return out
}

func (Int32Encoder) Decode(data []byte) (int32, int) {
	return int32(binary.LittleEndian.Uint32(data) + (1 << 31)), 4
}

var Int32 = Int32Encoder{}

package encoding

import "encoding/binary"

type Uint32Encoder struct{}

func (Uint32Encoder) Encode(v uint32) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, v)
	return out
}

func (Uint32Encoder) Decode(data []byte) (uint32, int) {
	return binary.LittleEndian.Uint32(data), 4
}

var Uint32 = Uint32Encoder{}

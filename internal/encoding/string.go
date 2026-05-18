package encoding

import "bytes"

type StringEncoder struct{}

func (StringEncoder) Encode(s string) []byte {
	b := []byte(s)
	escapeSymbolsCount := bytes.Count(b, []byte{0}) + bytes.Count(b, []byte{1})
	out := make([]byte, len(b)+escapeSymbolsCount+1) // +1 for null terminator

	pos := 0
	for _, sym := range b {
		if sym <= 1 {
			out[pos] = 0x01
			out[pos+1] = sym + 1
			pos += 2
		} else {
			out[pos] = sym
			pos++
		}
	}
	out[pos] = 0x00
	return out
}

func (StringEncoder) Decode(data []byte) (string, int) {
	nullIdx := bytes.Index(data, []byte{0})

	result := make([]byte, nullIdx) // worst case: no escapes
	srcPos := 0
	dstPos := 0
	for srcPos < nullIdx {
		if data[srcPos] == 0x01 {
			result[dstPos] = data[srcPos+1] - 1
			srcPos += 2
		} else {
			result[dstPos] = data[srcPos]
			srcPos++
		}
		dstPos++
	}
	return string(result[:dstPos]), nullIdx + 1
}

var String = StringEncoder{}

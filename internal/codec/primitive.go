package codec

import (
	"bytes"
	"distributed-storage/internal/primitive"
	"encoding/binary"
	"fmt"
)

func EncodeValue(value primitive.Primitive) []byte {
	valueType := value.Type()
	encodedValue := []byte{valueType}

	switch value := value.(type) {
	case *primitive.Null:
		encodedValue = append(encodedValue, encodeNull()...)
	case *primitive.Int32:
		encodedValue = append(encodedValue, encodeInt32(value)...)
	case *primitive.Int64:
		encodedValue = append(encodedValue, encodeInt64(value)...)
	case *primitive.Uint32:
		encodedValue = append(encodedValue, encodeUint32(value)...)
	case *primitive.Uint64:
		encodedValue = append(encodedValue, encodeUint64(value)...)
	case *primitive.String:
		encodedValue = append(encodedValue, encodeString(value)...)
	default:
		panic(fmt.Sprintf("EncodeValue: couldn't encode value because of wrong type %d", valueType))
	}

	return encodedValue
}

func DecodeValue(data []byte) (value primitive.Primitive, offset int, err error) {
	if len(data) == 0 {
		panic("DecodeValue: empty input")
	}

	valueType := data[0]
	data = data[1:]

	switch valueType {
	case primitive.TYPE_NULL:
		value, offset = decodeNull(data)
	case primitive.TYPE_INT32:
		value, offset = decodeInt32(data)
	case primitive.TYPE_INT64:
		value, offset = decodeInt64(data)
	case primitive.TYPE_UINT32:
		value, offset = decodeUint32(data)
	case primitive.TYPE_UINT64:
		value, offset = decodeUint64(data)
	case primitive.TYPE_STRING:
		value, offset = decodeString(data)
	default:
		panic(fmt.Sprintf("DecodeValue: couldn't parse value because of wrong type %d", valueType))
	}

	return value, offset + 1, nil // one byte is added for value type
}

func encodeNull() []byte {
	return []byte{}
}

func decodeNull(_ []byte) (*primitive.Null, int) {
	return primitive.NewNull(), 0
}

func encodeInt32(val *primitive.Int32) []byte {
	out := make([]byte, 4)

	binary.BigEndian.PutUint32(out, uint32(val.Value())+(1<<31))
	return out
}

func decodeInt32(data []byte) (*primitive.Int32, int) {
	return primitive.NewInt32(int32(binary.BigEndian.Uint32(data) + (1 << 31))), 4
}

func encodeInt64(val *primitive.Int64) []byte {
	out := make([]byte, 8)

	binary.BigEndian.PutUint64(out, uint64(val.Value())+(1<<63))
	return out
}

func decodeInt64(data []byte) (*primitive.Int64, int) {
	return primitive.NewInt64(int64(binary.BigEndian.Uint64(data) + (1 << 63))), 8
}

func encodeUint32(val *primitive.Uint32) []byte {
	out := make([]byte, 4)

	binary.BigEndian.PutUint32(out, val.Value())
	return out
}

func decodeUint32(data []byte) (*primitive.Uint32, int) {
	return primitive.NewUint32(binary.BigEndian.Uint32(data)), 4
}

func encodeUint64(val *primitive.Uint64) []byte {
	out := make([]byte, 8)

	binary.BigEndian.PutUint64(out, val.Value())
	return out
}

func decodeUint64(data []byte) (*primitive.Uint64, int) {
	return primitive.NewUint64(binary.BigEndian.Uint64(data)), 8
}

func encodeString(val *primitive.String) []byte {
	value := []byte(val.Value())
	escapeSymbolsCount := bytes.Count(value, []byte{0}) + bytes.Count(value, []byte{1})
	out := make([]byte, len(value)+escapeSymbolsCount+1) // +1 for null terminator

	pos := 0
	for _, symbol := range value {
		if symbol <= 1 { // we mask symbols 0 and 1 because we use 0 as a null terminator and 0x01 as an escape symbol
			out[pos] = 0x01
			out[pos+1] = symbol + 1
			pos += 2
		} else {
			out[pos] = symbol
			pos++
		}
	}
	out[pos] = 0x00

	return out
}

func decodeString(data []byte) (*primitive.String, int) {
	nullIdx := bytes.Index(data, []byte{0})

	result := make([]byte, nullIdx) // worst case: no masked symbols 0 and 1
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

	return primitive.NewString(string(result[:dstPos])), nullIdx + 1
}

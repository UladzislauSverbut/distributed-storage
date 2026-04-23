package vals

import "encoding/binary"

type Int64Value struct {
	num int64
}

func (value *Int64Value) Value() int64    { return value.num }
func (value *Int64Value) Type() ValueType { return TYPE_INT64 }
func (value *Int64Value) Empty() bool     { return false }

func (value *Int64Value) Serialize() []byte {
	serializedInt := make([]byte, 8)
	binary.LittleEndian.PutUint64(serializedInt, uint64(value.num)+(1<<63)) // reverse order of bits to make negative numbers smaller than positive ones

	return serializedInt
}

func (value *Int64Value) Parse(data []byte) int {
	value.num = int64(binary.LittleEndian.Uint64(data) + (1 << 63))

	return 8
}

func NewInt64(value int64) *Int64Value {
	return &Int64Value{value}
}

func ParseInt64(data []byte) (*Int64Value, int) {
	value := &Int64Value{}
	size := value.Parse(data)

	return value, size
}

func SerializeInt64(value int64) []byte {
	return NewInt64(value).Serialize()
}

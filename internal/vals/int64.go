package vals

import "distributed-storage/internal/encoding"

type Int64Value struct {
	num int64
}

func (value *Int64Value) Value() int64    { return value.num }
func (value *Int64Value) Type() ValueType { return TYPE_INT64 }
func (value *Int64Value) Empty() bool     { return false }

func (value *Int64Value) Equal(other Value) bool {
	if other.Type() != TYPE_INT64 {
		return false
	}

	return value.num == other.(*Int64Value).num
}

func (value *Int64Value) Serialize() []byte {
	return encoding.Int64.Encode(value.num)
}

func (value *Int64Value) Parse(data []byte) int {
	v, size := encoding.Int64.Decode(data)
	value.num = v
	return size
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
	return encoding.Int64.Encode(value)
}

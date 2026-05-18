package vals

import "distributed-storage/internal/encoding"

type Int32Value struct {
	num int32
}

func (value *Int32Value) Value() int32    { return value.num }
func (value *Int32Value) Type() ValueType { return TYPE_INT32 }
func (value *Int32Value) Empty() bool     { return false }

func (value *Int32Value) Equal(other Value) bool {
	if other.Type() != TYPE_INT32 {
		return false
	}

	return value.num == other.(*Int32Value).num
}

func (value *Int32Value) Serialize() []byte {
	return encoding.Int32.Encode(value.num)
}

func (value *Int32Value) Parse(data []byte) int {
	v, size := encoding.Int32.Decode(data)
	value.num = v
	return size
}

func NewInt32(value int32) *Int32Value {
	return &Int32Value{value}
}

func ParseInt32(data []byte) (*Int32Value, int) {
	value := &Int32Value{}
	size := value.Parse(data)
	return value, size
}

func SerializeInt32(value int32) []byte {
	return encoding.Int32.Encode(value)
}

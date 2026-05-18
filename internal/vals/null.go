package vals

import "distributed-storage/internal/encoding"

type NullValue struct{}

func (value *NullValue) Value()          {}
func (value *NullValue) Type() ValueType { return TYPE_NULL }
func (value *NullValue) Empty() bool     { return true }

func (value *NullValue) Serialize() []byte {
	return encoding.Null.Encode()
}

func (value *NullValue) Parse(payload []byte) int {
	return encoding.Null.Decode(payload)
}

func (value *NullValue) Equal(other Value) bool {
	return other.Type() == TYPE_NULL
}

func NewNull() *NullValue {
	return &NullValue{}
}

func ParseNull(_ []byte) (*NullValue, int) {
	return &NullValue{}, 0
}

func SerializeNull() []byte {
	return []byte{}
}

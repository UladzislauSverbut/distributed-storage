package vals

import "distributed-storage/internal/encoding"

type StringValue struct {
	str []byte
}

func (value *StringValue) Value() string   { return string(value.str) }
func (value *StringValue) Type() ValueType { return TYPE_STRING }
func (value *StringValue) Empty() bool     { return false }

func (value *StringValue) Equal(other Value) bool {
	if other.Type() != TYPE_STRING {
		return false
	}

	return string(value.str) == other.(*StringValue).Value()
}

func (value *StringValue) Serialize() []byte {
	return encoding.String.Encode(string(value.str))
}

func (value *StringValue) Parse(data []byte) int {
	s, size := encoding.String.Decode(data)
	value.str = []byte(s)
	return size
}

func NewString(value string) *StringValue {
	return &StringValue{[]byte(value)}
}

func ParseString(data []byte) (*StringValue, int) {
	value := &StringValue{}
	size := value.Parse(data)
	return value, size
}

func SerializeString(value string) []byte {
	return encoding.String.Encode(value)
}

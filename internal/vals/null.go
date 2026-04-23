package vals

type NullValue struct {
}

func (value *NullValue) Value()          { return }
func (value *NullValue) Type() ValueType { return TYPE_NULL }
func (value *NullValue) Empty() bool     { return true }

func (value *NullValue) Serialize() []byte {
	return []byte{}
}

func (value *NullValue) Parse(payload []byte) int {
	return 0
}

func NewNull() *NullValue {
	return &NullValue{}
}

func ParseNull(payload []byte) (*NullValue, int) {
	return &NullValue{}, 0
}

func SerializeNull() []byte {
	return []byte{}
}

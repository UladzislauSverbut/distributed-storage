package vals

type NullValue struct {
}

func (value *NullValue) Value() {
	return
}

func (value *NullValue) Type() ValueType {
	return TYPE_NULL
}

func (value *NullValue) Size() int {
	return 0
}

func (value *NullValue) Empty() bool {
	return true
}

func (value *NullValue) Serialize() []byte {
	return []byte{0}
}

func (value *NullValue) Parse(payload []byte) {
	return
}

func NewNull() *NullValue {
	return &NullValue{}
}

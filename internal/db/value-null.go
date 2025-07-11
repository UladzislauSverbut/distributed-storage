package db

type NullValue struct {
}

func (value *NullValue) Value() {
	return
}

func (value *NullValue) Type() ValueType {
	return VALUE_TYPE_NULL
}

func (value *NullValue) Size() int {
	return 0
}

func (value *NullValue) Empty() bool {
	return true
}

func (value *NullValue) serialize() []byte {
	return []byte{0}
}

func (value *NullValue) parse(payload []byte) {
	return
}

func NewNullValue() *NullValue {
	return &NullValue{}
}

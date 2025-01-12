package db

type NullValue struct {
}

func (value *NullValue) Get() {
	return
}

func (value *NullValue) GetType() ValueType {
	return VALUE_TYPE_NULL
}

func (value *NullValue) Size() int {
	return 0
}

func (value *NullValue) serialize() []byte {
	return []byte{}
}

func (value *NullValue) parse(payload []byte) {
	return
}

func NewNullValue() *NullValue {
	return &NullValue{}
}

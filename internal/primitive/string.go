package primitive

import "bytes"

type String struct {
	str []byte
}

func (value *String) Value() string       { return string(value.str) }
func (value *String) Type() PrimitiveType { return TYPE_STRING }
func (value *String) Empty() bool         { return false }

func (value *String) Equal(other Primitive) bool {
	if other.Type() != TYPE_STRING {
		return false
	}

	return bytes.Equal(value.str, other.(*String).str)
}

func NewString(value string) *String {
	return &String{[]byte(value)}
}

package primitive

type Int32 struct {
	num int32
}

func (value *Int32) Value() int32        { return value.num }
func (value *Int32) Type() PrimitiveType { return TYPE_INT32 }
func (value *Int32) Empty() bool         { return false }

func (value *Int32) Equal(other Primitive) bool {
	if other.Type() != TYPE_INT32 {
		return false
	}

	return value.num == other.(*Int32).num
}

func NewInt32(value int32) *Int32 {
	return &Int32{value}
}

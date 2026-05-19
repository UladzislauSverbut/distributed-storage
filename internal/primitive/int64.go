package primitive

type Int64 struct {
	num int64
}

func (value *Int64) Value() int64        { return value.num }
func (value *Int64) Type() PrimitiveType { return TYPE_INT64 }
func (value *Int64) Empty() bool         { return false }

func (value *Int64) Equal(other Primitive) bool {
	if other.Type() != TYPE_INT64 {
		return false
	}

	return value.num == other.(*Int64).num
}

func NewInt64(value int64) *Int64 {
	return &Int64{value}
}

package primitive

type Uint32 struct {
	num uint32
}

func (value *Uint32) Value() uint32       { return value.num }
func (value *Uint32) Type() PrimitiveType { return TYPE_UINT32 }
func (value *Uint32) Empty() bool         { return false }

func (value *Uint32) Equal(other Primitive) bool {
	if other.Type() != TYPE_UINT32 {
		return false
	}

	return value.num == other.(*Uint32).num
}

func NewUint32(value uint32) *Uint32 {
	return &Uint32{value}
}

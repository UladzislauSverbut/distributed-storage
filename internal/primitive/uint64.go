package primitive

type Uint64 struct {
	num uint64
}

func (value *Uint64) Value() uint64       { return value.num }
func (value *Uint64) Type() PrimitiveType { return TYPE_UINT64 }
func (value *Uint64) Empty() bool         { return false }

func (value *Uint64) Equal(other Primitive) bool {
	if other.Type() != TYPE_UINT64 {
		return false
	}

	return value.num == other.(*Uint64).num
}

func NewUint64(value uint64) *Uint64 {
	return &Uint64{value}
}

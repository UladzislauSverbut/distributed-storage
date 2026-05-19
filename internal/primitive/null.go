package primitive

type Null struct{}

func (value *Null) Value()              {}
func (value *Null) Type() PrimitiveType { return TYPE_NULL }
func (value *Null) Empty() bool         { return true }

func (value *Null) Equal(other Primitive) bool {
	return other.Type() == TYPE_NULL
}

func NewNull() *Null {
	return &Null{}
}

package vals

import "distributed-storage/internal/encoding"

type Uint32Value struct {
	num uint32
}

func (value *Uint32Value) Value() uint32   { return value.num }
func (value *Uint32Value) Type() ValueType { return TYPE_UINT32 }
func (value *Uint32Value) Empty() bool     { return false }

func (value *Uint32Value) Equal(other Value) bool {
	if other.Type() != TYPE_UINT32 {
		return false
	}

	return value.num == other.(*Uint32Value).num
}

func (value *Uint32Value) Serialize() []byte {
	return encoding.Uint32.Encode(value.num)
}

func (value *Uint32Value) Parse(data []byte) int {
	v, size := encoding.Uint32.Decode(data)
	value.num = v
	return size
}

func NewUint32(value uint32) *Uint32Value {
	return &Uint32Value{value}
}

func ParseUint32(data []byte) (*Uint32Value, int) {
	value := &Uint32Value{}
	size := value.Parse(data)
	return value, size
}

func SerializeUint32(value uint32) []byte {
	return encoding.Uint32.Encode(value)
}

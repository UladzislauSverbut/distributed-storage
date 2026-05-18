package vals

import "distributed-storage/internal/encoding"

type Uint64Value struct {
	num uint64
}

func (value *Uint64Value) Value() uint64   { return value.num }
func (value *Uint64Value) Type() ValueType { return TYPE_UINT64 }
func (value *Uint64Value) Empty() bool     { return false }

func (value *Uint64Value) Equal(other Value) bool {
	if other.Type() != TYPE_UINT64 {
		return false
	}

	return value.num == other.(*Uint64Value).num
}

func (value *Uint64Value) Serialize() []byte {
	return encoding.Uint64.Encode(value.num)
}

func (value *Uint64Value) Parse(data []byte) int {
	v, size := encoding.Uint64.Decode(data)
	value.num = v
	return size
}

func NewUint64(value uint64) *Uint64Value {
	return &Uint64Value{value}
}

func ParseUint64(data []byte) (*Uint64Value, int) {
	value := &Uint64Value{}
	size := value.Parse(data)
	return value, size
}

func SerializeUint64(value uint64) []byte {
	return encoding.Uint64.Encode(value)
}

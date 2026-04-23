package vals

import "encoding/binary"

type Uint64Value struct {
	num uint64
}

func (value *Uint64Value) Value() uint64   { return value.num }
func (value *Uint64Value) Type() ValueType { return TYPE_UINT64 }
func (value *Uint64Value) Empty() bool     { return false }

func (value *Uint64Value) Serialize() []byte {
	serializedInt := make([]byte, 8)
	binary.LittleEndian.PutUint64(serializedInt, value.num)

	return serializedInt
}

func (value *Uint64Value) Parse(data []byte) int {
	value.num = binary.LittleEndian.Uint64(data)

	return 8
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
	return NewUint64(value).Serialize()
}

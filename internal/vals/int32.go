package vals

import "encoding/binary"

type Int32Value struct {
	num int32
}

func (value *Int32Value) Value() int32    { return value.num }
func (value *Int32Value) Type() ValueType { return TYPE_INT32 }
func (value *Int32Value) Empty() bool     { return false }

func (value *Int32Value) Equal(other Value) bool {
	if other.Type() != TYPE_INT32 {
		return false
	}

	return value.num == other.(*Int32Value).num
}

func (value *Int32Value) Serialize() []byte {
	serializedInt := make([]byte, 4)
	binary.LittleEndian.PutUint32(serializedInt, uint32(value.num)+(1<<31)) // reverse order of bits to make negative numbers smaller than positive ones

	return serializedInt
}

func (value *Int32Value) Parse(data []byte) int {
	value.num = int32(binary.LittleEndian.Uint32(data) + (1 << 31))

	return 4
}

func NewInt32(value int32) *Int32Value {
	return &Int32Value{value}
}

func ParseInt32(data []byte) (*Int32Value, int) {
	value := &Int32Value{}
	size := value.Parse(data)

	return value, size
}

func SerializeInt32(value int32) []byte {
	return NewInt32(value).Serialize()
}

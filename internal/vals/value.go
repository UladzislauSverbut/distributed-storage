package vals

import "fmt"

type ValueType = uint8

type Value interface {
	Type() ValueType
	Empty() bool
	Equal(Value) bool
	Serialize() []byte
	Parse([]byte) int
}

const (
	TYPE_NULL ValueType = iota
	TYPE_STRING
	TYPE_INT32
	TYPE_INT64
	TYPE_UINT32
	TYPE_UINT64
)

func New(valueType ValueType) Value {
	switch valueType {
	case TYPE_NULL:
		return &NullValue{}
	case TYPE_STRING:
		return &StringValue{}
	case TYPE_INT32:
		return &Int32Value{}
	case TYPE_INT64:
		return &Int64Value{}
	case TYPE_UINT32:
		return &Uint32Value{}
	case TYPE_UINT64:
		return &Uint64Value{}
	default:
		panic(fmt.Sprintf("Value can`t be created because type is not supported %d", valueType))
	}
}

func ParseValue(valueType ValueType, data []byte) (Value, int) {
	value := New(valueType)
	size := value.Parse(data)
	return value, size
}

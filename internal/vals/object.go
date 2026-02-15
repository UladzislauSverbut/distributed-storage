package vals

type Object struct {
	values map[string]Value
}

func (object *Object) GetMany(fields []string) []Value {
	values := make([]Value, len(fields))

	for fieldIndex, field := range fields {
		values[fieldIndex] = object.Get(field)
	}

	return values
}

func (object *Object) Has(field string) bool {
	_, ok := object.values[field]

	return ok
}

func (object *Object) Set(field string, value Value) *Object {
	object.values[field] = value

	return object
}

func (object *Object) Get(field string) Value {
	value, ok := object.values[field]

	if ok {
		return value
	} else {
		return NewNull()
	}
}

func (object *Object) GetString(field string) string {
	return object.Get(field).(*StringValue).Value()
}

func (object *Object) GetUint32(field string) uint32 {
	return object.Get(field).(*IntValue[uint32]).Value()
}

func (object *Object) GetUint64(field string) uint64 {
	return object.Get(field).(*IntValue[uint64]).Value()
}

func (object *Object) GetInt32(field string) int32 {
	return object.Get(field).(*IntValue[int32]).Value()
}

func (object *Object) GetInt64(field string) int64 {
	return object.Get(field).(*IntValue[int64]).Value()
}

func NewObject() *Object {
	return &Object{values: make(map[string]Value)}
}

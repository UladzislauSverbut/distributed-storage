package db

type Object struct {
	values map[string]Value
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
		return NewNullValue()
	}
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

func NewObject() *Object {
	return &Object{values: make(map[string]Value)}
}

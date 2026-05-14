package vals

type Object struct {
	fields map[string]Value
}

func (object *Object) GetMany(fields []string) []Value {
	values := make([]Value, len(fields))

	for fieldIndex, field := range fields {
		values[fieldIndex] = object.Get(field)
	}

	return values
}

func (object *Object) Has(field string) bool {
	_, ok := object.fields[field]

	return ok
}

func (object *Object) Set(field string, value Value) *Object {
	object.fields[field] = value

	return object
}

func (object *Object) Get(field string) Value {
	value, ok := object.fields[field]

	if ok {
		return value
	} else {
		return NewNull()
	}
}

func (object *Object) Merge(other *Object) *Object {
	mergedObject := NewObject()

	for field, value := range object.Values() {
		mergedObject.Set(field, value)
	}

	if other == nil {
		return mergedObject
	}

	for field, value := range other.Values() {
		mergedObject.Set(field, value)
	}
	return mergedObject
}

func (object *Object) Matches(other *Object) bool {
	if other == nil {
		return false
	}

	for name, value := range object.fields {
		if !other.Get(name).Equal(value) {
			return false
		}
	}

	return true
}

func (object *Object) GetString(field string) string {
	return object.Get(field).(*StringValue).Value()
}

func (object *Object) GetUint32(field string) uint32 {
	return object.Get(field).(*Uint32Value).Value()
}

func (object *Object) GetUint64(field string) uint64 {
	return object.Get(field).(*Uint64Value).Value()
}

func (object *Object) GetInt32(field string) int32 {
	return object.Get(field).(*Int32Value).Value()
}

func (object *Object) GetInt64(field string) int64 {
	return object.Get(field).(*Int64Value).Value()
}

func (object *Object) Values() map[string]Value {
	return object.fields
}

func NewObject() *Object {
	return &Object{
		fields: make(map[string]Value),
	}
}

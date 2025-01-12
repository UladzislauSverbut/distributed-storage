package db

import "slices"

type Object struct {
	fields []string
	values []Value
}

func (object *Object) Set(field string, value Value) *Object {
	fieldPosition := slices.Index(object.fields, field)

	if fieldPosition >= 0 {
		object.values[fieldPosition] = value
	} else {
		object.fields = append(object.fields, field)
		object.values = append(object.values, value)
	}

	return object
}

func (object *Object) Get(field string) Value {
	fieldPosition := slices.Index(object.fields, field)

	if fieldPosition >= 0 {
		return object.values[fieldPosition]
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

func (record *Object) Has(field string) bool {
	return slices.Index(record.fields, field) >= 0
}

func NewObject() *Object {
	return &Object{}
}

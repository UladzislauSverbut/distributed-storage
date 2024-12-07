package db

import "slices"

type Record struct {
	fields []string
	values []Value
}

func (record *Record) Set(field string, value Value) *Record {
	fieldPosition := slices.Index(record.fields, field)

	if fieldPosition >= 0 {
		record.values[fieldPosition] = value
	} else {
		record.fields = append(record.fields, field)
		record.values = append(record.values, value)
	}

	return record
}

func (record *Record) Get(field string) Value {
	fieldPosition := slices.Index(record.fields, field)

	if fieldPosition >= 0 {
		return record.values[fieldPosition]
	} else {
		return nil
	}
}

func (record *Record) Has(field string) bool {
	return slices.Index(record.fields, field) >= 0
}

func NewRecord() *Record {
	return &Record{}
}

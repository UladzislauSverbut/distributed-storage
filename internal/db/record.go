package db

import "slices"

type Record struct {
	Fields []string
	Values []Value
}

func (record *Record) Set(field string, value Value) *Record {
	fieldPosition := slices.Index(record.Fields, field)

	if fieldPosition >= 0 {
		record.Values[fieldPosition] = value
	} else {
		record.Fields = append(record.Fields, field)
		record.Values = append(record.Values, value)
	}

	return record
}

func (record *Record) Get(field string) Value {
	fieldPosition := slices.Index(record.Fields, field)

	if fieldPosition >= 0 {
		return record.Values[fieldPosition]
	} else {
		return nil
	}
}

func (record *Record) Has(field string) bool {
	return slices.Index(record.Fields, field) >= 0
}

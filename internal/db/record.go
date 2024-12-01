package db

type Record struct {
	Fields []string
	Values []Value
}

func (record *Record) addValue(key string, value Value) {
	record.Fields = append(record.Fields, key)
	record.Values = append(record.Values, value)
}

func (record *Record) get(key string) Value {
	for index, field := range record.Fields {
		if field == key {
			return record.Values[index]
		}
	}

	return nil
}

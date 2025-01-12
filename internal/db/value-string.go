package db

import "bytes"

type StringValue struct {
	str []byte
}

func (value *StringValue) Get() string {
	return string(value.str)
}

func (value *StringValue) GetType() ValueType {
	return VALUE_TYPE_STRING
}

func (value *StringValue) Size() int {
	return len(value.str) + 1
}

func (value *StringValue) Empty() bool {
	return false
}

func (value *StringValue) serialize() []byte {
	escapeSymbolsCount := bytes.Count(value.str, []byte{0}) + bytes.Count(value.str, []byte{1})
	serializedString := make([]byte, len(value.str)+escapeSymbolsCount+1)

	serializedSymbolPosition := 0

	for _, symbol := range value.str {
		if symbol <= 1 {
			serializedString[serializedSymbolPosition] = 0x01
			serializedString[serializedSymbolPosition+1] = symbol + 1
			serializedSymbolPosition += 2
		} else {
			serializedString[serializedSymbolPosition] = symbol
			serializedSymbolPosition += 1
		}
	}

	serializedString[serializedSymbolPosition] = 0

	return serializedString
}

func (value *StringValue) parse(serializedString []byte) {
	stringLength := bytes.Index(serializedString, []byte{0})
	escapeSymbolsCount := bytes.Count(serializedString[:stringLength], []byte{0x01, 0x01}) + bytes.Count(serializedString[:stringLength], []byte{0x01, 0x02})

	value.str = make([]byte, stringLength-escapeSymbolsCount)

	for serializedSymbolPosition := 0; serializedSymbolPosition < stringLength; {
		if serializedString[serializedSymbolPosition] == 0x01 && serializedString[serializedSymbolPosition+1] < 0x02 {
			value.str[serializedSymbolPosition] = serializedString[serializedSymbolPosition+1] - 1
			serializedSymbolPosition += 2
		} else {
			value.str[serializedSymbolPosition] = serializedString[serializedSymbolPosition]
			serializedSymbolPosition += 1
		}
	}
}

func NewStringValue(value string) *StringValue {
	return &StringValue{[]byte(value)}
}

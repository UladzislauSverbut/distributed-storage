package db

import "bytes"

type StringValue struct {
	val []byte
}

func (value *StringValue) getValue() []byte {
	return value.val
}

func (value *StringValue) getType() ValueType {
	return VALUE_TYPE_STRING
}

func (value *StringValue) size() int {
	return len(value.val) + 1
}

func (value *StringValue) serialize() []byte {
	escapeSymbolsCount := bytes.Count(value.val, []byte{0}) + bytes.Count(value.val, []byte{1})
	serializedString := make([]byte, len(value.val)+escapeSymbolsCount+1)

	serializedSymbolPosition := 0

	for _, symbol := range value.val {
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
	escapeSymbolsCount := bytes.Count(value.val[:stringLength], []byte{0x01, 0x01}) + bytes.Count(value.val[:stringLength], []byte{0x01, 0x02})

	value.val = make([]byte, stringLength-escapeSymbolsCount)

	for serializedSymbolPosition := 0; serializedSymbolPosition < stringLength; {
		if serializedString[serializedSymbolPosition] == 0x01 && serializedString[serializedSymbolPosition+1] < 0x02 {
			value.val[serializedSymbolPosition] = serializedString[serializedSymbolPosition+1] - 1
			serializedSymbolPosition += 2
		} else {
			value.val[serializedSymbolPosition] = serializedString[serializedSymbolPosition]
			serializedSymbolPosition += 1
		}
	}
}

func NewStringValue(value []byte) *StringValue {
	return &StringValue{value}
}

package db

import "bytes"

type StringValue struct {
	Val []byte
}

func (value *StringValue) GetType() ValueType {
	return VALUE_TYPE_STRING
}

func (value *StringValue) Size() int {
	return len(value.Val) + 1
}

func (value *StringValue) serialize() []byte {
	escapeSymbolsCount := bytes.Count(value.Val, []byte{0}) + bytes.Count(value.Val, []byte{1})
	serializedString := make([]byte, len(value.Val)+escapeSymbolsCount+1)

	serializedSymbolPosition := 0

	for _, symbol := range value.Val {
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
	escapeSymbolsCount := bytes.Count(value.Val[:stringLength], []byte{0x01, 0x01}) + bytes.Count(value.Val[:stringLength], []byte{0x01, 0x02})

	value.Val = make([]byte, stringLength-escapeSymbolsCount)

	for serializedSymbolPosition := 0; serializedSymbolPosition < stringLength; {
		if serializedString[serializedSymbolPosition] == 0x01 && serializedString[serializedSymbolPosition+1] < 0x02 {
			value.Val[serializedSymbolPosition] = serializedString[serializedSymbolPosition+1] - 1
			serializedSymbolPosition += 2
		} else {
			value.Val[serializedSymbolPosition] = serializedString[serializedSymbolPosition]
			serializedSymbolPosition += 1
		}
	}
}

func NewStringValue(value string) *StringValue {
	return &StringValue{[]byte(value)}
}

package events

import (
	"bytes"
	"errors"
)

const START_TRANSACTION_EVENT = "START_TRANSACTION"

var startTransactionParsingError = errors.New("StartTransaction: couldn't parse event")

type StartTransaction struct{}

func NewStartTransaction() *StartTransaction {
	return &StartTransaction{}
}

func (event *StartTransaction) Name() string {
	return START_TRANSACTION_EVENT
}

func (event *StartTransaction) Serialize() []byte {
	return []byte(event.Name())
}

func ParseStartTransaction(data []byte) (*StartTransaction, error) {
	offset := len(START_TRANSACTION_EVENT)

	if !bytes.Equal(data[0:offset], []byte(START_TRANSACTION_EVENT)) {
		return nil, startTransactionParsingError
	}

	return &StartTransaction{}, nil
}

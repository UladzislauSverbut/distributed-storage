package events

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const START_TRANSACTION_EVENT = "START_TRANSACTION"

var startTransactionParsingError = errors.New("StartTransaction: couldn't parse event")

type StartTransaction struct {
	ID uint64
}

func NewStartTransaction(txID uint64) *StartTransaction {
	return &StartTransaction{ID: txID}
}

func (event *StartTransaction) Name() string {
	return START_TRANSACTION_EVENT
}

func (event *StartTransaction) Serialize() []byte {
	serializedEvent := []byte(event.Name())
	serializedID := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedID, event.ID)

	serializedEvent = append(serializedEvent, serializedID...)

	return serializedEvent
}

func ParseStartTransaction(data []byte) (*StartTransaction, error) {
	offset := len(START_TRANSACTION_EVENT)

	if !bytes.Equal(data[0:offset], []byte(START_TRANSACTION_EVENT)) {
		return nil, startTransactionParsingError
	}

	serializedID := data[offset : offset+8]

	return &StartTransaction{
		ID: binary.LittleEndian.Uint64(serializedID),
	}, nil
}

package events

import (
	"distributed-storage/internal/helpers"
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
	transactionID := make([]byte, 8)

	binary.LittleEndian.PutUint64(transactionID, event.ID)

	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, transactionID...)

	return serializedEvent
}

func ParseStartTransaction(data []byte) (*StartTransaction, error) {
	parts := helpers.SplitBy(data, ' ')

	if len(parts) != 2 || string(parts[0]) != START_TRANSACTION_EVENT {
		return nil, startTransactionParsingError
	}

	return &StartTransaction{
		ID: binary.LittleEndian.Uint64(parts[1]),
	}, nil
}

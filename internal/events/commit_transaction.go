package events

import (
	"distributed-storage/internal/helpers"
	"encoding/binary"
	"errors"
)

const COMMIT_TRANSACTION_EVENT = "COMMIT_TRANSACTION"

var commitTransactionParsingError = errors.New("CommitTransaction: couldn't parse event")

type CommitTransaction struct {
	ID uint64
}

func NewCommitTransaction(txID uint64) *CommitTransaction {
	return &CommitTransaction{ID: txID}
}

func (event *CommitTransaction) Name() string {
	return COMMIT_TRANSACTION_EVENT
}

func (event *CommitTransaction) Serialize() []byte {
	serializedEvent := []byte(event.Name())
	transactionID := make([]byte, 8)

	binary.LittleEndian.PutUint64(transactionID, event.ID)

	serializedEvent = append(serializedEvent, ' ')
	serializedEvent = append(serializedEvent, transactionID...)

	return serializedEvent
}

func ParseCommitTransaction(data []byte) (*CommitTransaction, error) {
	parts := helpers.SplitBy(data, ' ')

	if len(parts) != 2 || string(parts[0]) != COMMIT_TRANSACTION_EVENT {
		return nil, commitTransactionParsingError
	}

	return &CommitTransaction{
		ID: binary.LittleEndian.Uint64(parts[1]),
	}, nil
}

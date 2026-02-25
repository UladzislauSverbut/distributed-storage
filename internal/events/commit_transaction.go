package events

import (
	"bytes"
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
	serializedID := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedID, event.ID)

	serializedEvent = append(serializedEvent, serializedID...)

	return serializedEvent
}

func ParseCommitTransaction(data []byte) (*CommitTransaction, error) {
	offset := len(COMMIT_TRANSACTION_EVENT)
	if !bytes.Equal(data[:offset], []byte(COMMIT_TRANSACTION_EVENT)) {
		return nil, commitTransactionParsingError
	}

	serializedID := data[offset : offset+8]

	return &CommitTransaction{
		ID: binary.LittleEndian.Uint64(serializedID),
	}, nil
}

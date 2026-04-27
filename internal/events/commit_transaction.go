package events

import (
	"bytes"
	"errors"
)

const COMMIT_TRANSACTION_EVENT = "COMMIT_TRANSACTION"

var commitTransactionParsingError = errors.New("CommitTransaction: couldn't parse event")

type CommitTransaction struct{}

func NewCommitTransaction() *CommitTransaction {
	return &CommitTransaction{}
}

func (event *CommitTransaction) Name() string {
	return COMMIT_TRANSACTION_EVENT
}

func (event *CommitTransaction) Serialize() []byte {
	return []byte(event.Name())
}

func ParseCommitTransaction(data []byte) (*CommitTransaction, error) {
	offset := len(COMMIT_TRANSACTION_EVENT)

	if !bytes.Equal(data[:offset], []byte(COMMIT_TRANSACTION_EVENT)) {
		return nil, commitTransactionParsingError
	}

	return &CommitTransaction{}, nil
}

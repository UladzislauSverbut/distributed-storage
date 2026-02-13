package events

import "strconv"

const COMMIT_TRANSACTION_EVENT = "COMMIT_TRANSACTION"

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
	return []byte(event.Name() + "(TX=" + strconv.FormatUint(event.ID, 10) + ")\n")
}

func (event *CommitTransaction) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

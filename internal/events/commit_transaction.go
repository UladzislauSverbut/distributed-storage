package events

import "strconv"

const COMMIT_TRANSACTION_EVENT = "COMMIT_TRANSACTION"

type CommitTransaction struct {
	TxID TxID
}

func (e *CommitTransaction) Name() string {
	return COMMIT_TRANSACTION_EVENT
}

func (e *CommitTransaction) Serialize() []byte {
	return []byte(e.Name() + "(TX=" + strconv.FormatUint(uint64(e.TxID), 10) + ")\n")
}

func (e *CommitTransaction) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

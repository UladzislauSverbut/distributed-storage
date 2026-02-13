package events

import "strconv"

const START_TRANSACTION_EVENT = "START_TRANSACTION"

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
	return []byte(event.Name() + "(TX=" + strconv.FormatUint(uint64(event.ID), 10) + ")\n")
}

func (event *StartTransaction) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

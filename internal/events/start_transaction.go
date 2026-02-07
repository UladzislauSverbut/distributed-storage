package events

import "strconv"

const START_TRANSACTION_EVENT = "START_TRANSACTION"

type StartTransaction struct {
	ID uint64
}

func (e *StartTransaction) Name() string {
	return START_TRANSACTION_EVENT
}

func (e *StartTransaction) Serialize() []byte {
	return []byte(e.Name() + "(TX=" + strconv.FormatUint(uint64(e.ID), 10) + ")\n")
}

func (e *StartTransaction) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}

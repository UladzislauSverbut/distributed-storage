package events

type StartTransaction struct{}

func NewStartTransaction() *StartTransaction {
	return &StartTransaction{}
}

func (event *StartTransaction) Type() EventType {
	return START_TRANSACTION_EVENT
}

package events

type CommitTransaction struct{}

func NewCommitTransaction() *CommitTransaction {
	return &CommitTransaction{}
}

func (event *CommitTransaction) Type() EventType {
	return COMMIT_TRANSACTION_EVENT
}

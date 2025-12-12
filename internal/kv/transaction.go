package kv

import (
	"fmt"
)

type Transaction struct {
	kv         *KeyValue
	snapshotID SnapshotID
}

func NewTransaction(kv *KeyValue) *Transaction {
	return &Transaction{
		kv: kv,
	}
}

func (transaction *Transaction) Begin() {
	transaction.snapshotID = transaction.kv.storage.Snapshot()
}

func (transaction *Transaction) Abort() {
	transaction.kv.storage.Restore(transaction.snapshotID)
}

func (transaction *Transaction) Commit() error {
	if err := transaction.kv.storage.Flush(); err != nil {
		transaction.Abort()
		return err
	}

	if err := transaction.kv.storage.SaveRoot(transaction.kv.tree.Root()); err != nil {
		panic(fmt.Errorf("Transaction: couldn't save root pointer on commit: %w", err))
	}

	return nil
}

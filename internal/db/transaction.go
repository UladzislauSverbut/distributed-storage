package db

import (
	"distributed-storage/internal/kv"
	"fmt"
)

type Transaction struct {
	tableName string
	inner     *kv.Transaction
}

func NewTransaction(database Database, tableName string) *Transaction {
	return &Transaction{
		tableName: tableName,
		inner:     kv.NewTransaction(database.kv),
	}
}

func (transaction *Transaction) Begin() {
	transaction.inner.Begin()
}

func (transaction *Transaction) Abort() {
	transaction.inner.Abort()
}

func (transaction *Transaction) Commit() error {
	if err := transaction.inner.Commit(); err != nil {
		return fmt.Errorf("Database: couldn't commit table %s transaction %w", transaction.tableName, err)
	}

	return nil
}

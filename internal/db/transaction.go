package db

import (
	"context"
	"fmt"
	"sync/atomic"
)

type TransactionState int32

type TransactionCommit struct {
	DatabaseVersion DatabaseVersion
	ReadEvents      []TableEvent
	ChangeEvents    []TableEvent
	Response        chan<- TransactionCommitResponse
}

type TransactionCommitResponse struct {
	Success bool
	Error   error
}

type Transaction struct {
	version DatabaseVersion
	state   atomic.Int32 // It's reference to TransactionState but with atomic operations support
	manager *TableManager

	commitQueue chan<- TransactionCommit
	ctx         context.Context
}

const (
	PROCESSING TransactionState = iota
	COMMITTING
	COMMITTED
	ABORTED
)

func NewTransaction(db *Database, ctx context.Context) (*Transaction, error) {
	return db.createTransaction(ctx)
}

func (tx *Transaction) Commit() (err error) {
	defer func() {
		if panic := recover(); panic != nil {
			err = fmt.Errorf("Transaction: panic during commit: %v", panic)

			tx.Rollback()
		}
	}()

	if !tx.setCommitting() {
		return fmt.Errorf("Transaction: couldn't commit transaction because it is not active")
	}

	// If there is nothing to write, then just return
	if len(tx.manager.changeEvents()) == 0 {
		tx.setCommitted()
		return nil
	}

	// Create channel to get response from db writer
	responseChannel := make(chan TransactionCommitResponse, 1)

	tx.commitQueue <- TransactionCommit{
		DatabaseVersion: tx.version,
		ChangeEvents:    tx.manager.changeEvents(),
		Response:        responseChannel,
	}

	select {
	case response := <-responseChannel:
		if response.Success {
			tx.setCommitted()
			return nil
		} else {
			tx.setAborted()
			return response.Error
		}
	case <-tx.ctx.Done():
		tx.setAborted()
		return fmt.Errorf("Transaction: commit transaction cancelled by context")
	}
}

func (tx *Transaction) Rollback() {
	tx.setAborted()
}

func (tx *Transaction) Table(tableName string) (*Table, error) {
	table, err := tx.manager.table(tableName)
	if err != nil {
		return nil, fmt.Errorf("Transaction: couldn't get table %s: %w", tableName, err)
	}

	return table, nil
}

func (tx *Transaction) CreateTable(schema *TableSchema) (*Table, error) {
	table, err := tx.manager.table(schema.Name)
	if err != nil {
		return nil, fmt.Errorf("Transaction: couldn't create table %s because of error during getting table: %w", schema.Name, err)
	}

	if table != nil {
		return nil, fmt.Errorf("Transaction: couldn't create table %s because it already exist", schema.Name)
	}

	table, err = tx.manager.createTable(schema)
	if err != nil {
		return nil, fmt.Errorf("Transaction: couldn't create table %s because of error during creating table: %w", schema.Name, err)
	}

	return table, nil
}

func (tx *Transaction) IsActive() bool {
	state := TransactionState(tx.state.Load())

	return state == PROCESSING || state == COMMITTING
}

func (tx *Transaction) setCommitting() bool {

	if TransactionState(tx.state.Load()) != PROCESSING {
		return false
	}

	return tx.state.CompareAndSwap(int32(PROCESSING), int32(COMMITTING))
}

func (tx *Transaction) setCommitted() bool {
	if TransactionState(tx.state.Load()) != COMMITTING {
		return false
	}

	return tx.state.CompareAndSwap(int32(COMMITTING), int32(COMMITTED))
}

func (tx *Transaction) setAborted() bool {
	if state := TransactionState(tx.state.Load()); state != COMMITTING {
		return false
	}

	return tx.state.CompareAndSwap(int32(COMMITTING), int32(ABORTED))
}

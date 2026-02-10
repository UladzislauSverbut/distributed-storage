package db

import (
	"context"
	"distributed-storage/internal/pager"
	"fmt"
	"sync/atomic"
)

type TransactionID uint64
type TransactionState int

type TransactionCommit struct {
	DatabaseVersion DatabaseVersion
	TransactionID   TransactionID
	ReadEvents      []TableEvent
	ChangeEvents    []TableEvent
	Response        chan<- TransactionCommitResponse
}

type TransactionCommitResponse struct {
	Success bool
	Error   error
}

type Transaction struct {
	id      TransactionID
	version DatabaseVersion
	state   TransactionState
	manager *TableManager

	commitQueue chan<- TransactionCommit
	ctx         context.Context
}

const (
	ACTIVE TransactionState = iota
	COMMITTING
	COMMITTED
	ABORTED
)

func NewTransaction(db *Database, ctx context.Context) (*Transaction, error) {
	db.mu.RLock()
	storage := db.storage
	pagesCount := db.pagesCount
	pageSize := db.config.PageSize
	db.mu.RUnlock()

	tx := &Transaction{
		id:      TransactionID(atomic.AddUint64((*uint64)(&db.nextTransactionID), 1)),
		version: db.version,

		state:   ACTIVE,
		manager: NewTableManager(db.root, pager.NewPageAllocator(storage, pagesCount, pageSize)),

		commitQueue: db.commitQueue,
		ctx:         ctx,
	}

	db.mu.Lock()
	db.transactions.Add(db.version, tx)
	db.mu.Unlock()

	return tx, nil
}

func (tx *Transaction) Commit() (err error) {
	defer func() {
		if panic := recover(); panic != nil {
			err = fmt.Errorf("Transaction: panic during commit: %v", panic)
		}

		// In any case we reset transaction changes once it is finished in any state
		tx.Rollback()
	}()

	if !tx.markCommitting() {
		return fmt.Errorf("Transaction: couldn't commit transaction with id %d because it is not active", tx.id)
	}

	// If there is nothing to write, then just return
	if len(tx.manager.ChangeEvents()) == 0 {
		tx.markCommitted()
		return nil
	}

	// Create channel to get response from db writer
	responseChannel := make(chan TransactionCommitResponse, 1)

	tx.commitQueue <- TransactionCommit{
		TransactionID: tx.id,
		ChangeEvents:  tx.manager.ChangeEvents(),
		Response:      responseChannel,
	}

	select {
	case response := <-responseChannel:
		if response.Success {
			tx.markCommitted()
			return nil
		} else {
			tx.markAborted()
			return response.Error
		}
	case <-tx.ctx.Done():
		tx.markAborted()
		return fmt.Errorf("Transaction: commit transaction with id %d cancelled by context", tx.id)
	}
}

func (tx *Transaction) Rollback() {
	tx.markAborted()
}

func (tx *Transaction) Table(tableName string) (*Table, error) {
	table, err := tx.manager.Table(tableName)
	if err != nil {
		return nil, fmt.Errorf("Transaction: couldn't get table %s: %w", tableName, err)
	}

	return table, nil
}

func (tx *Transaction) CreateTable(schema *TableSchema) (*Table, error) {
	table, err := tx.manager.Table(schema.Name)
	if err != nil {
		return nil, fmt.Errorf("Transaction: couldn't create table %s because of error during getting table: %w", schema.Name, err)
	}

	if table != nil {
		return nil, fmt.Errorf("Transaction: couldn't create table %s because it already exist", schema.Name)
	}

	table, err = tx.manager.CreateTable(schema)
	if err != nil {
		return nil, fmt.Errorf("Transaction: couldn't create table %s because of error during creating table: %w", schema.Name, err)
	}

	return table, nil
}

func (tx *Transaction) markCommitting() bool {
	if tx.state != ACTIVE {
		return false
	}

	tx.state = COMMITTING
	return true
}

func (tx *Transaction) markCommitted() bool {
	if tx.state != COMMITTING {
		return false
	}

	tx.state = COMMITTED
	return true
}

func (tx *Transaction) markAborted() bool {
	if tx.state != COMMITTING {
		return false
	}

	tx.state = ABORTED
	return true
}

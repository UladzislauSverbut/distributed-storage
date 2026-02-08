package db

import (
	"context"
	"distributed-storage/internal/events"
	"encoding/json"
	"fmt"
	"sync/atomic"
)

type TransactionID uint64
type TransactionState int

type TransactionCommitRequest struct {
	TransactionID TransactionID
	ReadEvents    []TableEvent
	ChangeEvents  []TableEvent
	Response      chan<- TransactionCommitResponse
}

type TransactionCommitResponse struct {
	Success bool
	Error   error
}

type Transaction struct {
	id           TransactionID
	state        TransactionState
	catalog      *Catalog
	changeEvents []TableEvent
	readEvents   []TableEvent

	commitQueue chan<- TransactionCommitRequest
	ctx         context.Context
}

const (
	ACTIVE TransactionState = iota
	COMMITTING
	COMMITTED
	ABORTED
)

func NewTransaction(db *Database, ctx context.Context) (*Transaction, error) {
	catalog := NewCatalog(db)

	tx := &Transaction{
		id: TransactionID(atomic.AddUint64((*uint64)(&db.nextTransactionID), 1)),

		state:   ACTIVE,
		catalog: catalog,

		commitQueue: db.commitQueue,
		ctx:         ctx,
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.transactions[tx.id] = tx

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
	if len(tx.changeEvents) == 0 {
		tx.markCommitted()
		return nil
	}

	// Create channel to get response from db writer
	responseChannel := make(chan TransactionCommitResponse, 1)

	tx.commitQueue <- TransactionCommitRequest{
		TransactionID: tx.id,
		ChangeEvents:  tx.changeEvents,
		ReadEvents:    tx.readEvents,
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
	table, err := tx.catalog.Table(tableName)
	if err != nil {
		return nil, fmt.Errorf("Transaction: couldn't get table %s: %w", tableName, err)
	}

	return table, nil
}

func (tx *Transaction) CreateTable(schema *TableSchema) (*Table, error) {
	table, err := tx.catalog.Table(schema.Name)
	if err != nil {
		return nil, fmt.Errorf("Transaction: couldn't create table %s because of error during getting table: %w", schema.Name, err)
	}

	if table != nil {
		return nil, fmt.Errorf("Transaction: couldn't create table %s because it already exist", schema.Name)
	}

	table, err = tx.catalog.CreateTable(schema)
	if err != nil {
		return nil, fmt.Errorf("Transaction: couldn't create table %s because of error during creating table: %w", schema.Name, err)
	}

	stringifiedSchema, _ := json.Marshal(schema)

	tx.changeEvents = append(tx.changeEvents, &events.CreateTable{
		TableName: table.Name(),
		Schema:    stringifiedSchema,
	})

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

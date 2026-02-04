package db

import (
	"context"
	"distributed-storage/internal/events"
	"distributed-storage/internal/pager"
	"encoding/json"
	"fmt"
	"sync/atomic"
)

type TransactionID uint64

type TransactionState int
type TransactionCommitRequest struct {
	Id             TransactionID
	Reads          []Event
	Writes         []Event
	AllocatedPages []pager.PagePointer
	Response       chan<- TransactionCommitResponse
}

type TransactionCommitResponse struct {
	Success bool
	Error   error
}

type Transaction struct {
	id             TransactionID
	state          TransactionState
	catalog        *Catalog
	pageManager    *pager.PageManager
	affectedTables map[string]*Table
	events         []Event

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
	root := db.catalog.Root()
	pageManager, err := pager.NewPageManager(db.storage, db.allocator, db.config.PageSize)
	if err != nil {
		return nil, err
	}

	catalog, err := NewCatalog(root, pageManager)
	if err != nil {
		return nil, err
	}

	tx := &Transaction{
		id: TransactionID(atomic.AddUint64((*uint64)(&db.nextTransactionID), 1)),

		state:          ACTIVE,
		catalog:        catalog,
		pageManager:    pageManager,
		affectedTables: map[string]*Table{},

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

		// in any case we reset transaction changes once it is finished in any state
		tx.Rollback()
	}()

	if !tx.markCommitting() {
		return fmt.Errorf("Transaction: couldn't commit transaction with id %d because it is not active", tx.id)
	}

	reads, writes := tx.collectEvents()

	// if there is nothing to write, than just return
	if len(writes) == 0 {
		tx.markCommitted()
		return nil
	}

	// create channel to get response from db writer
	responseChannel := make(chan TransactionCommitResponse, 1)

	tx.commitQueue <- TransactionCommitRequest{
		Id:       tx.id,
		Writes:   writes,
		Reads:    reads,
		Response: responseChannel,
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
	if table, ok := tx.affectedTables[tableName]; ok {
		return table, nil
	}

	table, err := tx.catalog.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("Transaction: couldn't get table %s: %w", tableName, err)
	}

	tx.affectedTables[tableName] = table

	return table, nil
}

func (tx *Transaction) CreateTable(schema *TableSchema) (*Table, error) {
	table, err := tx.catalog.GetTable(schema.Name)
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

	tx.affectedTables[schema.Name] = table
	stringifiedSchema, _ := json.Marshal(schema)

	tx.events = append(tx.events, &events.CreateTable{
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

func (tx *Transaction) collectEvents() (reads []Event, writes []Event) {
	writes = tx.events

	for _, table := range tx.affectedTables {
		// if table doesn't have events than table was not modified
		if len(table.events) == 0 {
			continue
		}
		writes = append(writes, table.events...)
	}

	return
}

// func (tx *Transaction) resetChanges() {
// 	tx.allocator.Free(tx.pageManager.State().AllocatedPages.Values())
// }

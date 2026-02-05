package db

import (
	"bytes"
	"context"
	"distributed-storage/internal/events"
	"distributed-storage/internal/kv"
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

const DEFAULT_DIRECTORY = "/var/lib/kv"
const DEFAULT_PAGE_SIZE = 16 * 1024 // 16KB
const HEADER_SIZE = 24

// bytes
const NUMBER_OF_PARALLEL_TRANSACTIONS = 1024 // max number of parallel transactions
const COMMIT_BATCH_SIZE = 16                 // number of transactions to commit in a single batch

const TRANSACTION_TIMEOUT = 30 * time.Second
const COMMIT_INTERVAL = 10 * time.Millisecond

type Database struct {
	catalog           *Catalog
	config            *DatabaseConfig
	storage           store.Storage
	transactions      map[TransactionID]*Transaction
	nextTransactionID TransactionID

	wal         *Wal
	allocator   *pager.PageAllocator
	commitQueue chan TransactionCommitRequest

	mu sync.RWMutex
}

type DatabaseConfig struct {
	Directory string
	InMemory  bool
	PageSize  int
}

func NewDatabase(config *DatabaseConfig) (*Database, error) {
	if config.Directory == "" {
		config.Directory = DEFAULT_DIRECTORY
	}

	if config.PageSize == 0 {
		config.PageSize = DEFAULT_PAGE_SIZE
	}

	walStorage, dbStorage, err := setupStorage(config)

	if err != nil {
		return nil, err
	}

	root, nextTransactionID, pagesCount := parseHeader(dbStorage.MemorySegment(0, HEADER_SIZE))
	allocator := pager.NewPageAllocator(pagesCount)
	wal := NewWal(walStorage)

	pager, err := pager.NewPageManager(dbStorage, allocator, config.PageSize)
	if err != nil {
		return nil, err
	}

	catalog, err := NewCatalog(root, pager)
	if err != nil {
		return nil, err
	}

	db := &Database{
		catalog:           catalog,
		config:            config,
		storage:           dbStorage,
		transactions:      make(map[TransactionID]*Transaction),
		nextTransactionID: nextTransactionID,

		wal:         wal,
		allocator:   allocator,
		commitQueue: make(chan TransactionCommitRequest, NUMBER_OF_PARALLEL_TRANSACTIONS),
	}

	go db.applyCommits()

	return db, nil
}

func (db *Database) StartTransaction(request func(*Transaction)) error {
	context, _ := context.WithTimeout(context.Background(), TRANSACTION_TIMEOUT)
	transaction, err := NewTransaction(db, context)

	if err != nil {
		return err
	}

	request(transaction)

	if err := transaction.Commit(); err != nil {
		return err
	}

	return nil
}

func (db *Database) applyCommits() {
	commits := make([]TransactionCommitRequest, 0, COMMIT_BATCH_SIZE)
	ticker := time.NewTicker(COMMIT_INTERVAL)

	for {
		select {
		case commit := <-db.commitQueue:
			commits = append(commits, commit)

			if len(commits) == COMMIT_BATCH_SIZE {
				ticker.Stop()
				db.processCommits(commits)
				commits = make([]TransactionCommitRequest, 0, COMMIT_BATCH_SIZE)
				ticker.Reset(COMMIT_INTERVAL)
			}

		case <-ticker.C:
			if len(commits) > 0 {
				ticker.Stop()
				db.processCommits(commits)
				commits = make([]TransactionCommitRequest, 0, COMMIT_BATCH_SIZE)
				ticker.Reset(COMMIT_INTERVAL)
			}
		}
	}
}

func (db *Database) processCommits(commits []TransactionCommitRequest) {
	abortedCommits := make([]TransactionCommitRequest, 0)
	approvedCommits := make([]TransactionCommitRequest, 0)

	for _, commit := range commits {
		for _, event := range commit.Writes {
			if err := applyEvent(db.catalog, event); err != nil {
				abortedCommits = append(abortedCommits, commit)
				break
			}
		}

		approvedCommits = append(approvedCommits, commit)
	}

	walEvents := make([]events.Event, 0)

	for _, commit := range approvedCommits {
		walEvents = append(walEvents, &events.StartTransaction{TxID: events.TxID(commit.Id)})
		walEvents = append(walEvents, commit.Writes...)
		walEvents = append(walEvents, &events.CommitTransaction{TxID: events.TxID(commit.Id)})
	}

	if err := db.wal.Write(walEvents); err != nil {
		db.abortCommits(commits, fmt.Errorf("WAL write failed: %w", err))
		return
	}

	db.catalog.Save()
	db.catalog.pageManager.Save()

	db.approveCommits(approvedCommits)
	db.storage.UpdateMemorySegment(0, buildHeader(db.catalog.Root(), db.nextTransactionID, db.allocator.Count()))

	db.storage.Flush()

}

// Move all this logic to a separate files to avoid circular imports between db and events packages
func applyEvent(catalog *Catalog, ev Event) error {
	switch e := ev.(type) {
	case *events.CreateTable:
		var schema TableSchema
		if err := json.Unmarshal(e.Schema, &schema); err != nil {
			return fmt.Errorf("CreateTable Apply: can't parse schema: %w", err)
		}

		// Check if table already exists
		table, err := catalog.GetTable(e.TableName)
		if err != nil {
			return fmt.Errorf("CreateTable Apply: %w", err)
		}

		if table != nil {
			return fmt.Errorf("CreateTable Apply: couldn't create table %s because it already exists", e.TableName)
		}

		if _, err := catalog.CreateTable(&schema); err != nil {
			return fmt.Errorf("CreateTable Apply: %w", err)
		}
		return nil

	case *events.DeleteTable:
		table, err := catalog.GetTable(e.TableName)
		if err != nil {
			return fmt.Errorf("DeleteTable Apply: %w", err)
		}
		if table == nil {
			return fmt.Errorf("DeleteTable Apply: table %s not found", e.TableName)
		}
		if err := catalog.DeleteTable(e.TableName); err != nil {
			return fmt.Errorf("DeleteTable Apply: %w", err)
		}
		return nil

	case *events.DeleteEntry:
		table, err := catalog.GetTable(e.TableName)
		if err != nil {
			return fmt.Errorf("DeleteEntry Apply: %w", err)
		}
		if table == nil {
			return fmt.Errorf("DeleteEntry Apply: table %s not found", e.TableName)
		}
		response, err := table.kv.Delete(&kv.DeleteRequest{Key: e.Key})
		if err != nil {
			return fmt.Errorf("DeleteEntry Apply: %w", err)
		}
		if !bytes.Equal(response.OldValue, e.Value) {
			return fmt.Errorf("DeleteEntry Apply: old value does not match expected value")
		}
		return nil

	case *events.UpdateEntry:
		table, err := catalog.GetTable(e.TableName)
		if err != nil {
			return fmt.Errorf("UpdateEntry Apply: %w", err)
		}
		if table == nil {
			return fmt.Errorf("UpdateEntry Apply: table %s not found", e.TableName)
		}
		response, err := table.kv.Set(&kv.SetRequest{Key: e.Key, Value: e.NewValue})
		if err != nil {
			return fmt.Errorf("UpdateEntry Apply: %w", err)
		}
		if !bytes.Equal(response.OldValue, e.OldValue) {
			return fmt.Errorf("UpdateEntry Apply: old value does not match expected value")
		}
		return nil

	case *events.InsertEntry:
		table, err := catalog.GetTable(e.TableName)
		if err != nil {
			return fmt.Errorf("InsertEntry Apply: %w", err)
		}
		if table == nil {
			return fmt.Errorf("InsertEntry Apply: table %s not found", e.TableName)
		}
		response, err := table.kv.Set(&kv.SetRequest{Key: e.Key, Value: e.Value})
		if err != nil {
			return fmt.Errorf("InsertEntry Apply: %w", err)
		}
		if response.Updated {
			return fmt.Errorf("InsertEntry Apply: expected insert but key already existed")
		}
		return nil

	case *events.StartTransaction:
		// Transaction boundaries are handled at a higher level; no-op for catalog.
		return nil

	case *events.CommitTransaction:
		// Also doesn't change catalog directly here.
		return nil

	case *events.FreePages:
		// Page freeing is coordinated via allocator; catalog isn't modified.
		return nil

	default:
		return fmt.Errorf("unknown event type %T", ev)
	}
}

func (db *Database) abortCommits(commits []TransactionCommitRequest, err error) {
	freePages := make([]pager.PagePointer, 0)

	for _, commit := range commits {
		freePages = append(freePages, commit.AllocatedPages...)

		commit.Response <- TransactionCommitResponse{
			Error:   err,
			Success: false,
		}
	}

	db.allocator.Free(freePages)
}

func (db *Database) approveCommits(commits []TransactionCommitRequest) {
	for _, commit := range commits {
		commit.Response <- TransactionCommitResponse{
			Error:   nil,
			Success: true,
		}
	}
}

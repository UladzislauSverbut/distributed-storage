package db

import (
	"context"
	"distributed-storage/internal/events"
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	"fmt"
	"sync"
	"time"
)

const DEFAULT_DIRECTORY = "/var/lib/kv"
const DEFAULT_PAGE_SIZE = 16 * 1024 // 16KB
const HEADER_SIZE = 24

// bytes
const NUMBER_OF_PARALLEL_TRANSACTIONS = 1024 // max number of parallel transactions
const COMMIT_BATCH_SIZE = 64                 // number of transactions to commit in a single batch

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
	ctx, cancel := context.WithTimeout(context.Background(), TRANSACTION_TIMEOUT)
	defer cancel()

	transaction, err := NewTransaction(db, ctx)
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
		if err := db.catalog.ApplyChangeEvents(commit.ChangeEvents); err != nil {
			abortedCommits = append(abortedCommits, commit)
		} else {
			approvedCommits = append(approvedCommits, commit)
		}
	}

	eventsToLog := make([]TableEvent, 0)

	for _, commit := range approvedCommits {
		eventsToLog = append(eventsToLog, &events.StartTransaction{ID: uint64(commit.TransactionID)})
		eventsToLog = append(eventsToLog, commit.ChangeEvents...)
		eventsToLog = append(eventsToLog, &events.CommitTransaction{ID: uint64(commit.TransactionID)})
	}

	if err := db.wal.Write(eventsToLog); err != nil {
		db.abortCommits(commits, fmt.Errorf("WAL write failed: %w", err))
		return
	}

	if err := db.catalog.PersistTables(); err != nil {
		db.abortCommits(commits, fmt.Errorf("Catalog persist failed: %w", err))
		return
	}

	db.approveCommits(approvedCommits)

	db.storage.UpdateMemorySegment(0, buildHeader(db.catalog.Root(), db.nextTransactionID, db.allocator.Count()))

	db.storage.Flush()
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

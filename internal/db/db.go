package db

import (
	"context"
	"distributed-storage/internal/events"
	"distributed-storage/internal/helpers"
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
const COMMIT_BATCH_SIZE = 256                // number of transactions to commit in a single batch

const TRANSACTION_TIMEOUT = 30 * time.Minute
const COMMIT_INTERVAL = 10 * time.Millisecond

type DatabaseVersion uint64

type Database struct {
	root              pager.PagePointer
	version           DatabaseVersion
	pagesCount        uint64
	nextTransactionID TransactionID

	wal     *Wal
	config  *DatabaseConfig
	storage store.Storage

	pagePool     *helpers.MinMap[DatabaseVersion, pager.PagePointer]
	transactions *helpers.MinMap[DatabaseVersion, *Transaction]
	commitQueue  chan TransactionCommit

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

	db := &Database{
		root:              root,
		pagesCount:        pagesCount,
		nextTransactionID: nextTransactionID,

		wal:     NewWal(walStorage),
		config:  config,
		storage: dbStorage,

		pagePool:     helpers.NewMinMap[DatabaseVersion, pager.PagePointer](func(i, j DatabaseVersion) bool { return i < j }),
		transactions: helpers.NewMinMap[DatabaseVersion, *Transaction](func(i, j DatabaseVersion) bool { return i < j }),
		commitQueue:  make(chan TransactionCommit, NUMBER_OF_PARALLEL_TRANSACTIONS),
	}

	go db.collectCommits()

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

func (db *Database) collectCommits() {
	commits := make([]TransactionCommit, 0, COMMIT_BATCH_SIZE)
	ticker := time.NewTicker(COMMIT_INTERVAL)

	for {
		select {
		case commit := <-db.commitQueue:
			commits = append(commits, commit)

			if len(commits) == COMMIT_BATCH_SIZE {
				ticker.Stop()
				db.processCommits(commits)
				commits = make([]TransactionCommit, 0, COMMIT_BATCH_SIZE)
				ticker.Reset(COMMIT_INTERVAL)
			}

		case <-ticker.C:
			if len(commits) > 0 {
				ticker.Stop()
				db.processCommits(commits)
				commits = make([]TransactionCommit, 0, COMMIT_BATCH_SIZE)
				ticker.Reset(COMMIT_INTERVAL)
			}
		}
	}
}

func (db *Database) processCommits(commits []TransactionCommit) {
	dbVersion := db.minimalActiveVersion()
	availablePages := db.unreachablePages(dbVersion)
	allocator := pager.NewPageAllocator(db.storage, db.pagesCount, db.config.PageSize, availablePages...)
	manager := NewTableManager(db.root, allocator)

	abortedCommits := make([]TransactionCommit, 0)
	approvedCommits := make([]TransactionCommit, 0)

	for _, commit := range commits {
		if err := manager.ApplyChangeEvents(commit.ChangeEvents); err != nil {
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

	if err := manager.PersistTables(); err != nil {
		db.abortCommits(commits, fmt.Errorf("Catalog persist failed: %w", err))
		return
	}

	db.approveCommits(approvedCommits)

	db.root = manager.Root()
	db.pagesCount = manager.allocator.TotalPages()
	db.markPagesUnreachable(db.version, manager.allocator.ReleasedPages())

	db.version++

	db.storage.UpdateMemorySegment(0, buildHeader(db.root, db.nextTransactionID, db.pagesCount))

	db.storage.Flush()
}

func (db *Database) abortCommits(commits []TransactionCommit, err error) {
	for _, commit := range commits {
		commit.Response <- TransactionCommitResponse{
			Error:   err,
			Success: false,
		}
	}
}

func (db *Database) approveCommits(commits []TransactionCommit) {
	for _, commit := range commits {
		commit.Response <- TransactionCommitResponse{
			Error:   nil,
			Success: true,
		}
	}
}

func (db *Database) unreachablePages(usedVersion DatabaseVersion) []pager.PagePointer {
	pages := make([]pager.PagePointer, 0)

	for {
		if version, _, ok := db.pagePool.PeekMin(); ok && version > usedVersion {
			_, unreachablePages, _ := db.pagePool.PopMin()
			pages = append(pages, unreachablePages...)
		} else {
			return pages
		}
	}
}

func (db *Database) markPagesUnreachable(version DatabaseVersion, pages []pager.PagePointer) {
	db.pagePool.AddMultiple(version, pages)
}

func (db *Database) minimalActiveVersion() DatabaseVersion {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if version, _, ok := db.transactions.PeekMin(); ok {
		return version
	}

	return db.version
}

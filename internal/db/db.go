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

	freePages    map[DatabaseVersion][]pager.PagePointer
	commitQueue  chan TransactionCommit
	transactions *helpers.MinMap[DatabaseVersion, *Transaction]

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

		freePages:    make(map[DatabaseVersion][]pager.PagePointer),
		commitQueue:  make(chan TransactionCommit, NUMBER_OF_PARALLEL_TRANSACTIONS),
		transactions: helpers.NewMinMap[DatabaseVersion, *Transaction](func(i, j DatabaseVersion) bool { return i < j }),
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
	var manager *TableManager
	root := db.root

	abortedCommits := make([]TransactionCommit, 0)
	approvedCommits := make([]TransactionCommit, 0)

	for _, commit := range commits {
		allocator := pager.NewPageAllocator(db.storage, db.pagesCount, db.config.PageSize)
		manager := NewTableManager(root, allocator)

		if err := manager.ApplyChangeEvents(commit.ChangeEvents); err != nil {
			abortedCommits = append(abortedCommits, commit)
		} else {
			approvedCommits = append(approvedCommits, commit)
		}

		root = manager.Root()
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

	db.mu.Lock()
	defer db.mu.Unlock()

	db.root = manager.Root()
	db.pagesCount = manager.allocator.TotalPages()

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

package db

import (
	"context"
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const DB_STORAGE_SIGNATURE = "DISTRIBUTED_DB_STORAGE" // Signature to identify and validate database storage file
const HEADER_SIZE = len(DB_STORAGE_SIGNATURE) + 32

// bytes
const NUMBER_OF_PARALLEL_TRANSACTIONS = 1024 // Max number of parallel transactions
const COMMIT_BATCH_SIZE = 256                // Number of transactions to commit in a single batch

const TRANSACTION_TIMEOUT = 30 * time.Minute
const COMMIT_INTERVAL = 10 * time.Millisecond

type DatabaseVersion uint64

type Database struct {
	root              pager.PagePointer
	version           DatabaseVersion
	pagesCount        uint64
	nextTransactionID atomic.Uint64 // Alias for TransactionID, but using atomic for thread-safe incrementing

	wal     *WAL
	config  DatabaseConfig
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

func NewDatabase(config DatabaseConfig) (*Database, error) {
	config = applyDefaults(config)
	walStorage, dbStorage, err := setupStorage(config)

	if err != nil {
		return nil, err
	}

	db := &Database{
		wal:     NewWAL(walStorage),
		config:  config,
		storage: dbStorage,

		pagePool:     helpers.NewMinMap[DatabaseVersion, pager.PagePointer](func(i, j DatabaseVersion) bool { return i < j }),
		transactions: helpers.NewMinMap[DatabaseVersion, *Transaction](func(i, j DatabaseVersion) bool { return i < j }),
		commitQueue:  make(chan TransactionCommit, NUMBER_OF_PARALLEL_TRANSACTIONS),
	}

	if err := db.readHeader(); err != nil {
		return nil, err
	}

	if err := db.recoverFromWAL(); err != nil {
		return nil, err
	}

	go db.runCommitLoop()

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

func (db *Database) runCommitLoop() {
	transactions := make([]TransactionCommit, 0, COMMIT_BATCH_SIZE)
	ticker := time.NewTicker(COMMIT_INTERVAL)

	for {
		select {
		case commit := <-db.commitQueue:
			transactions = append(transactions, commit)

			if len(transactions) == COMMIT_BATCH_SIZE {
				ticker.Stop()
				db.commitBatch(transactions)
				transactions = make([]TransactionCommit, 0, COMMIT_BATCH_SIZE)
				ticker.Reset(COMMIT_INTERVAL)
			}

		case <-ticker.C:
			if len(transactions) > 0 {
				ticker.Stop()
				db.commitBatch(transactions)
				transactions = make([]TransactionCommit, 0, COMMIT_BATCH_SIZE)
				ticker.Reset(COMMIT_INTERVAL)
			}
		}
	}
}

func (db *Database) commitBatch(transactions []TransactionCommit) {
	latestUnreachableVersion := db.latestUnreachableVersion()

	allocator := pager.NewPageAllocator(db.storage, db.pagesCount, db.config.PageSize, db.collectReleasedPages(latestUnreachableVersion)...)
	manager := NewTableManager(db.root, allocator)

	abortedTransactions := make([]TransactionCommit, 0)
	approvedTransactions := make([]TransactionCommit, 0)

	for _, transaction := range transactions {
		if err := manager.ApplyChangeEvents(transaction.ChangeEvents); err != nil {
			abortedTransactions = append(abortedTransactions, transaction)
		} else {
			approvedTransactions = append(approvedTransactions, transaction)
		}
	}

	if err := manager.WriteTables(); err != nil {
		db.rejectTransactions(transactions, fmt.Errorf("Catalog persist failed: %w", err))
		return
	}

	releasedPages := allocator.ReleasedPages() // These pages will be ready to safely reused only since next db version since they can be still used by active transactions in the current version
	reusablePages := allocator.ReusablePages() // These pages can be reused because they are not used by any active transaction (e.g. they were allocated and released in the same version)

	db.wal.WriteTransactions(transactions)
	db.wal.WriteFreePages(latestUnreachableVersion, reusablePages)
	db.wal.WriteFreePages(db.version+1, releasedPages)
	db.wal.WriteVersion(db.version + 1)

	if err := db.wal.Flush(); err != nil {
		db.rejectTransactions(transactions, fmt.Errorf("WAL flush failed: %w", err))
		return
	}

	db.releasePages(latestUnreachableVersion, reusablePages)
	db.releasePages(db.version+1, releasedPages)

	db.approveTransactions(approvedTransactions)

	db.mu.Lock()
	db.root = manager.Root()
	db.pagesCount = allocator.TotalPages()
	db.version++
	db.mu.Unlock()
}

func (db *Database) rejectTransactions(transactions []TransactionCommit, err error) {
	for _, transaction := range transactions {
		transaction.Response <- TransactionCommitResponse{
			Error:   err,
			Success: false,
		}
	}
}

func (db *Database) approveTransactions(transactions []TransactionCommit) {
	for _, transaction := range transactions {
		transaction.Response <- TransactionCommitResponse{
			Error:   nil,
			Success: true,
		}
	}
}

func (db *Database) collectReleasedPages(target DatabaseVersion) []pager.PagePointer {
	pages := make([]pager.PagePointer, 0)

	for {
		if version, _, ok := db.pagePool.PeekMin(); ok && version <= target {
			_, freedPages, _ := db.pagePool.PopMin()
			pages = append(pages, freedPages...)
		} else {
			return pages
		}
	}
}

func (db *Database) releasePages(version DatabaseVersion, pages []pager.PagePointer) {
	db.pagePool.AddMultiple(version, pages)
}

func (db *Database) latestUnreachableVersion() DatabaseVersion {
	db.mu.Lock()
	defer db.mu.Unlock()

	for {
		version, transactions, ok := db.transactions.PeekMin()
		if !ok {
			return db.version - 1
		}

		for _, transaction := range transactions {
			if transaction.IsActive() {
				return version - 1
			}
		}

		db.transactions.PopMin()
	}
}

func (db *Database) readHeader() error {
	header := db.storage.MemorySegment(0, HEADER_SIZE)
	signature := header[0:len(DB_STORAGE_SIGNATURE)]

	if helpers.IsZero(signature) {
		db.root = pager.NULL_PAGE
		db.version = 1
		db.pagesCount = 1 // reserve page for header
		db.nextTransactionID.Store(0)

		return nil
	}

	if string(signature) != DB_STORAGE_SIGNATURE {
		return errors.New("Database: couldn't parse storage file because of corrupted file")
	}

	signatureSize := len(DB_STORAGE_SIGNATURE)

	db.mu.Lock()
	db.root = pager.PagePointer(binary.LittleEndian.Uint64(header[signatureSize : signatureSize+8]))
	db.version = DatabaseVersion(binary.LittleEndian.Uint64(header[signatureSize+8 : signatureSize+16]))
	db.pagesCount = binary.LittleEndian.Uint64(header[signatureSize+16 : signatureSize+24])
	db.nextTransactionID.Store(binary.LittleEndian.Uint64(header[signatureSize+24 : signatureSize+32]))
	db.mu.Unlock()

	return nil
}

func (db *Database) writeHeader() error {
	header := make([]byte, HEADER_SIZE)
	signatureSize := len(DB_STORAGE_SIGNATURE)

	copy(header[0:signatureSize], []byte(DB_STORAGE_SIGNATURE))

	db.mu.RLock()
	binary.LittleEndian.PutUint64(header[signatureSize:signatureSize+8], uint64(db.root))
	binary.LittleEndian.PutUint64(header[signatureSize+8:signatureSize+16], uint64(db.version))
	binary.LittleEndian.PutUint64(header[signatureSize+16:signatureSize+24], uint64(db.nextTransactionID.Load()))
	binary.LittleEndian.PutUint64(header[signatureSize+24:signatureSize+32], uint64(db.pagesCount))
	db.mu.RUnlock()

	return db.storage.UpdateMemorySegment(0, header)
}

func (db *Database) recoverFromWAL() error {
	latestSavedVersion, err := db.wal.LatestVersion()
	if err != nil {
		return fmt.Errorf("Database: failed to get latest database version from WAL: %w", err)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if latestSavedVersion == db.version {
		return nil
	}

	return nil
	//missedEvents := db.wal.ChangesSince(db.version)
}

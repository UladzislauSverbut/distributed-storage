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

// Bytes
const NUMBER_OF_PARALLEL_TRANSACTIONS = 1024 // Max number of parallel transactions
const COMMIT_BATCH_SIZE = 256                // Number of transactions to commit in a single batch

const TRANSACTION_TIMEOUT = 30 * time.Minute
const COMMIT_INTERVAL = 10 * time.Millisecond
const SYNC_INTERVAL = 1 * time.Second

type DatabaseVersion uint64

type DatabaseHeader struct {
	root          pager.PagePointer
	version       DatabaseVersion
	pagesCount    uint64
	transactionID *atomic.Uint64 // It's reference to TransactionID but with atomic operations support
}

type Database struct {
	header *DatabaseHeader

	wal     *WAL
	config  DatabaseConfig
	storage store.Storage

	pagePool     *helpers.MinMap[DatabaseVersion, pager.PagePointer]
	transactions *helpers.MinMap[DatabaseVersion, *Transaction]
	commitQueue  chan TransactionCommit

	mu sync.RWMutex
}

type DatabaseConfig struct {
	Directory           string
	InMemory            bool
	PageSize            int
	WALSegmentSize      int
	WALDirectory        string
	WALArchiveDirectory string
}

func NewDatabase(config DatabaseConfig) (*Database, error) {
	config = applyDefaults(config)

	db := &Database{
		config: config,

		pagePool:     helpers.NewMinMap[DatabaseVersion, pager.PagePointer](func(i, j DatabaseVersion) bool { return i < j }),
		transactions: helpers.NewMinMap[DatabaseVersion, *Transaction](func(i, j DatabaseVersion) bool { return i < j }),
		commitQueue:  make(chan TransactionCommit, NUMBER_OF_PARALLEL_TRANSACTIONS),
	}

	var err error

	if err = setupFS(config); err != nil {
		return nil, fmt.Errorf("Database: failed to setup filesystem: %w", err)
	}

	if db.storage, err = newStorage(config); err != nil {
		return nil, fmt.Errorf("Database: failed to setup storage: %w", err)
	}

	if db.wal, err = newWAL(config); err != nil {
		return nil, fmt.Errorf("Database: failed to initialize WAL: %w", err)
	}

	if db.header, err = db.readHeader(); err != nil {
		return nil, fmt.Errorf("Database: failed to read header: %w", err)
	}

	if err = db.recoverFromWAL(); err != nil {
		return nil, fmt.Errorf("Database: failed to recover from WAL: %w", err)
	}

	go db.runCommitLoop()
	go db.runSyncLoop()

	return db, nil
}

func (db *Database) StartTransaction(request func(*Transaction)) error {
	ctx, cancel := context.WithTimeout(context.Background(), TRANSACTION_TIMEOUT)
	defer cancel()

	transaction, err := db.createTransaction(ctx)
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

func (db *Database) runSyncLoop() {
	ticker := time.NewTicker(SYNC_INTERVAL)

	for range ticker.C {
		ticker.Stop()

		if err := db.storage.Flush(); err != nil {
			fmt.Printf("Database: failed to flush storage: %s\n", err)
		}

		ticker.Reset(SYNC_INTERVAL)
	}
}

func (db *Database) commitBatch(transactions []TransactionCommit) {
	latestUnreachableVersion := db.latestUnreachableVersion()

	allocator := pager.NewPageAllocator(db.storage, db.header.pagesCount, db.config.PageSize, db.collectReleasedPages(latestUnreachableVersion)...)
	manager := newTableManager(db.header.root, allocator)

	abortedTransactions := make([]TransactionCommit, 0)
	approvedTransactions := make([]TransactionCommit, 0)

	for _, transaction := range transactions {
		if err := manager.applyChangeEvents(transaction.ChangeEvents); err != nil {
			abortedTransactions = append(abortedTransactions, transaction)
		} else {
			approvedTransactions = append(approvedTransactions, transaction)
		}
	}

	if err := manager.writeTables(); err != nil {
		db.rejectTransactions(transactions, fmt.Errorf("Database: failed to write tables to to storage: %w", err))
		return
	}

	releasedPages := allocator.ReleasedPages()
	reusablePages := allocator.ReusablePages()

	newHeader := &DatabaseHeader{
		root:          manager.root(),
		version:       db.header.version + 1,
		pagesCount:    allocator.TotalPages(),
		transactionID: db.header.transactionID,
	}

	changes := append(allocator.Changes(), store.SegmentUpdate{Offset: 0, Data: db.serializeHeader(newHeader)})

	if err := db.storage.UpdateSegments(changes); err != nil {
		db.rejectTransactions(transactions, fmt.Errorf("Database: failed to update storage segments: %w", err))
		return
	}

	db.wal.appendTransactions(approvedTransactions)
	db.wal.appendFreePages(latestUnreachableVersion, reusablePages) // These pages can be reused because they are not used by any active transaction (e.g. they were allocated and released in the same version)
	db.wal.appendFreePages(db.header.version, releasedPages)        // These pages will be ready to safely reused only since next db version since they can be still used by active transactions in the current version
	db.wal.appendVersionUpdate(db.header.version + 1)

	if err := db.wal.sync(); err != nil {
		db.rejectTransactions(transactions, fmt.Errorf("Database: WAL flush failed: %w", err))
		return
	}

	db.approveTransactions(approvedTransactions)
	db.rejectTransactions(abortedTransactions, errors.New("Database: transaction aborted due to conflicts with other transactions"))

	db.releasePages(latestUnreachableVersion, reusablePages)
	db.releasePages(db.header.version, releasedPages)

	db.mu.Lock()
	defer db.mu.Unlock()
	db.header = newHeader
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

func (db *Database) createTransaction(ctx context.Context) (*Transaction, error) {
	db.mu.RLock()
	header := db.header
	db.mu.RUnlock()

	tx := &Transaction{
		id:      TransactionID(header.transactionID.Add(1)),
		version: header.version,
		manager: newTableManager(header.root, pager.NewPageAllocator(db.storage, header.pagesCount, db.config.PageSize)),

		commitQueue: db.commitQueue,
		ctx:         ctx,
	}

	tx.state.Store(int32(PROCESSING))

	db.mu.Lock()
	db.transactions.Add(tx.version, tx)
	db.mu.Unlock()

	return tx, nil
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
			return db.header.version - 1
		}

		for _, transaction := range transactions {
			if transaction.IsActive() {
				return version - 1
			}
		}

		db.transactions.PopMin()
	}
}

func (db *Database) readHeader() (*DatabaseHeader, error) {
	headerBlock := db.storage.Segment(0, HEADER_SIZE)
	signature := headerBlock[0:len(DB_STORAGE_SIGNATURE)]

	if helpers.IsZero(signature) {
		return &DatabaseHeader{
			root:          pager.NULL_PAGE,
			version:       1,
			pagesCount:    1, // Reserve page for header
			transactionID: &atomic.Uint64{},
		}, nil
	}

	if string(signature) != DB_STORAGE_SIGNATURE {
		return nil, errors.New("Database: couldn't parse storage file because of corrupted file")
	}

	signatureSize := len(DB_STORAGE_SIGNATURE)

	header := &DatabaseHeader{
		root:          pager.PagePointer(binary.LittleEndian.Uint64(headerBlock[signatureSize : signatureSize+8])),
		version:       DatabaseVersion(binary.LittleEndian.Uint64(headerBlock[signatureSize+8 : signatureSize+16])),
		pagesCount:    binary.LittleEndian.Uint64(headerBlock[signatureSize+16 : signatureSize+24]),
		transactionID: &atomic.Uint64{},
	}

	header.transactionID.Store(binary.LittleEndian.Uint64(headerBlock[signatureSize+24 : signatureSize+32]))

	return header, nil
}

func (db *Database) serializeHeader(header *DatabaseHeader) []byte {
	headerBlock := make([]byte, HEADER_SIZE)
	signatureSize := len(DB_STORAGE_SIGNATURE)

	copy(headerBlock[0:signatureSize], []byte(DB_STORAGE_SIGNATURE))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize:signatureSize+8], uint64(header.root))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize+8:signatureSize+16], uint64(header.version))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize+16:signatureSize+24], header.pagesCount)
	binary.LittleEndian.PutUint64(headerBlock[signatureSize+24:signatureSize+32], uint64(header.transactionID.Load()))

	return headerBlock
}

func (db *Database) recoverFromWAL() error {
	latestSavedVersion, err := db.wal.latestUpdatedVersion()
	if err != nil {
		return fmt.Errorf("Database: failed to get latest database version from WAL: %w", err)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if latestSavedVersion == db.header.version {
		return nil
	}

	return nil
}

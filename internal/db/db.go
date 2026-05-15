package db

import (
	"context"
	"distributed-storage/internal/events"
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
const INITIAL_DB_VERSION DatabaseVersion = 1

// Bytes
const NUMBER_OF_PARALLEL_TRANSACTIONS = 1024 // Max number of parallel transactions
const COMMIT_BATCH_SIZE = 256                // Number of transactions to commit in a single batch

const TRANSACTION_TIMEOUT = 30 * time.Minute
const COMMIT_INTERVAL = 1 * time.Millisecond
const SYNC_INTERVAL = 100 * time.Millisecond

type DatabaseVersion uint64

type DatabaseHeader struct {
	root        pager.PagePointer
	version     DatabaseVersion
	tablesCount uint64
	pagesCount  uint64
}

type TableIDAllocator func() TableID

type Database struct {
	header        *DatabaseHeader
	syncedVersion DatabaseVersion
	nextTableID   atomic.Uint64

	wal     *WAL
	pager   *pager.Pager
	config  DatabaseConfig
	storage store.Storage

	pagePool     *helpers.MinMap[DatabaseVersion, pager.PageList]
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

		pagePool:     helpers.NewMinMap[DatabaseVersion, pager.PageList](func(i, j DatabaseVersion) bool { return i < j }),
		transactions: helpers.NewMinMap[DatabaseVersion, *Transaction](func(i, j DatabaseVersion) bool { return i < j }),

		commitQueue: make(chan TransactionCommit, NUMBER_OF_PARALLEL_TRANSACTIONS),
	}

	var err error

	if err = setupFS(config); err != nil {
		return nil, fmt.Errorf("Database: failed to setup filesystem: %w", err)
	}

	if db.storage, err = newStorage(config); err != nil {
		return nil, fmt.Errorf("Database: failed to setup storage: %w", err)
	}

	if db.header, err = db.readHeader(); err != nil {
		return nil, fmt.Errorf("Database: failed to read header: %w", err)
	}

	db.pager = pager.NewPager(db.storage, db.header.pagesCount, db.config.PageSize)

	if db.wal, err = newWAL(config); err != nil {
		return nil, fmt.Errorf("Database: failed to initialize WAL: %w", err)
	}

	if err = db.init(); err != nil {
		return nil, fmt.Errorf("Database: failed to initialize database: %w", err)
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
		db.mu.Lock()

		ticker.Stop()

		if err := db.storage.Flush(); err != nil {
			fmt.Printf("Database: failed to flush storage: %s\n", err)
		} else {
			db.syncedVersion = db.header.version
		}

		db.mu.Unlock()

		ticker.Reset(SYNC_INTERVAL)
	}
}

func (db *Database) commitBatch(transactions []TransactionCommit) {
	latestUnreachableVersion := db.latestUnreachableVersion()
	manager := db.tableManager(db.collectReleasedPages(latestUnreachableVersion))

	var abortedTransactions []TransactionCommit
	var approvedTransactions []TransactionCommit

	var (
		applyResult ApplyResult
		err         error
	)
	for _, transaction := range transactions {
		if applyResult, err = manager.ApplyChangeEvents(transaction.ChangeEvents); err != nil {
			abortedTransactions = append(abortedTransactions, transaction)
		} else {
			approvedTransactions = append(approvedTransactions, transaction)
		}
	}

	committedReleasedPages := applyResult.ReleasedPages
	reusablePages := applyResult.ReusablePages

	newHeader := &DatabaseHeader{
		root:        applyResult.Root,
		version:     db.header.version + 1,
		pagesCount:  applyResult.PagesCount,
		tablesCount: applyResult.TablesCount,
	}

	if err := manager.Commit(db.serializeHeader(newHeader)); err != nil {
		db.rejectTransactions(transactions, fmt.Errorf("Database: failed to commit changes: %w", err))
		return
	}

	db.wal.appendTransactions(approvedTransactions)
	db.wal.appendVersionUpdate(db.header.version + 1)

	db.wal.appendFreePages(latestUnreachableVersion, reusablePages)   // These pages can be reused because they are not used by any active transaction (e.g. they were allocated and released in the same version)
	db.wal.appendFreePages(db.header.version, committedReleasedPages) // These pages will be ready to safely reused only since next db version since they can be still used by active transactions in the current version

	if err := db.wal.sync(); err != nil {
		db.rejectTransactions(transactions, fmt.Errorf("Database: WAL flush failed: %w", err))
		return
	}

	db.approveTransactions(approvedTransactions)
	db.rejectTransactions(abortedTransactions, errors.New("Database: transaction aborted due to conflicts with other transactions"))

	db.releasePages(latestUnreachableVersion, reusablePages)
	db.releasePages(db.header.version, committedReleasedPages)

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
	manager := db.tableManager()

	tx := &Transaction{
		manager:     manager,
		commitQueue: db.commitQueue,
		ctx:         ctx,
	}

	tx.state.Store(int32(TRANSACTION_PROCESSING))

	db.mu.Lock()
	db.transactions.Add(manager.state.Version, tx)
	db.mu.Unlock()

	return tx, nil
}

func (db *Database) collectReleasedPages(target DatabaseVersion) pager.PageList {
	pageList := pager.NewPageList()

	for {
		if version, _, ok := db.pagePool.PeekMin(); ok && version <= target {
			_, freedLists, _ := db.pagePool.PopMin()
			for _, list := range freedLists {
				pageList.AddMany(list.Pages())
			}
		} else {
			return pageList
		}
	}
}

func (db *Database) releasePages(version DatabaseVersion, list pager.PageList) {
	db.pagePool.Add(version, list)
}

func (db *Database) latestUnreachableVersion() DatabaseVersion {
	db.mu.Lock()
	defer db.mu.Unlock()

	for {
		version, transactions, ok := db.transactions.PeekMin()
		if !ok {
			return db.syncedVersion
		}

		for _, transaction := range transactions {
			if transaction.IsActive() {
				return min(version-1, db.syncedVersion)
			}
		}

		db.transactions.PopMin()
	}
}

func (db *Database) readHeader() (*DatabaseHeader, error) {
	headerBlock := pager.NewPager(db.storage, 1, db.config.PageSize).Page(HEADER_PAGE)
	signature := headerBlock[0:len(DB_STORAGE_SIGNATURE)]

	header := &DatabaseHeader{
		root:        pager.NULL_PAGE,
		version:     INITIAL_DB_VERSION,
		pagesCount:  1, // Reserve 1 page for header
		tablesCount: 1, // Reserve 0 table for @catalog table
	}

	if helpers.IsZero(signature) {
		return header, nil
	}

	if string(signature) != DB_STORAGE_SIGNATURE {
		return nil, errors.New("Database: couldn't parse storage file because of corrupted file")
	}

	signatureSize := len(DB_STORAGE_SIGNATURE)

	header.root = pager.PagePointer(binary.LittleEndian.Uint64(headerBlock[signatureSize : signatureSize+8]))
	header.version = DatabaseVersion(binary.LittleEndian.Uint64(headerBlock[signatureSize+8 : signatureSize+16]))
	header.pagesCount = binary.LittleEndian.Uint64(headerBlock[signatureSize+16 : signatureSize+24])
	header.tablesCount = binary.LittleEndian.Uint64(headerBlock[signatureSize+24 : signatureSize+32])

	return header, nil
}

func (db *Database) serializeHeader(header *DatabaseHeader) []byte {
	headerBlock := make([]byte, HEADER_SIZE)
	signatureSize := len(DB_STORAGE_SIGNATURE)

	copy(headerBlock[0:signatureSize], []byte(DB_STORAGE_SIGNATURE))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize:signatureSize+8], uint64(header.root))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize+8:signatureSize+16], uint64(header.version))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize+16:signatureSize+24], uint64(header.pagesCount))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize+24:signatureSize+32], uint64(header.tablesCount))

	return headerBlock
}

func (db *Database) init() error {
	if db.empty() {
		return db.initWAL()
	} else {
		return db.recoverFromWAL()
	}
}

func (db *Database) initWAL() error {
	db.wal.appendVersionUpdate(db.header.version)

	if err := db.wal.sync(); err != nil {
		return fmt.Errorf("Database: failed to flush WAL after setup: %w", err)
	}

	db.syncedVersion = db.header.version
	db.nextTableID.Store(db.header.tablesCount)

	return nil
}

func (db *Database) recoverFromWAL() error {
	restoredEvents, err := db.wal.eventsSince(db.header.version)
	if err != nil {
		return fmt.Errorf("Database: failed to get latest database version from WAL: %w", err)
	}

	freePages := pager.NewPageList()

	// FreePages events stored in WAL at the beginning of the events list because they are written to WAL UpdateDBVersion event
	for _, event := range restoredEvents {
		if event, ok := event.(*events.FreePages); ok {
			freePages.AddMany(event.List.Pages())
		} else {
			break
		}
	}

	manager := db.tableManager(freePages)

	applyResult, err := manager.ApplyChangeEvents(restoredEvents)
	if err != nil {
		return fmt.Errorf("Database: failed to apply events from WAL: %w", err)
	}

	db.header.root = applyResult.Root
	db.header.version = applyResult.DatabaseVersion
	db.header.pagesCount = applyResult.PagesCount
	db.header.tablesCount = applyResult.TablesCount

	if err := manager.Commit(db.serializeHeader(db.header)); err != nil {
		return fmt.Errorf("Database: failed to commit restored state: %w", err)
	}

	if err := db.storage.Flush(); err != nil {
		return fmt.Errorf("Database: failed to flush restored tables from WAL: %w", err)
	}

	db.syncedVersion = db.header.version
	db.nextTableID.Store(db.header.tablesCount)

	return nil
}

func (db *Database) tableManager(freePages ...pager.PageList) *TableManager {
	db.mu.RLock()
	defer db.mu.RUnlock()

	state := TableManagerState{Root: db.header.root, Version: db.header.version}

	return newTableManager(
		state,
		func() TableID { return TableID(db.nextTableID.Add(1) - 1) },
		db.pager.Fork(db.header.pagesCount, freePages...),
	)
}

func (db *Database) empty() bool {
	return db.header.version == INITIAL_DB_VERSION && db.wal.empty()
}

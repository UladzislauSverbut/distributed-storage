package db

import (
	"context"
	"distributed-storage/internal/catalog"
	"distributed-storage/internal/events"
	"distributed-storage/internal/helpers"
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	transaction "distributed-storage/internal/tx"
	"distributed-storage/internal/wal"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const DB_STORAGE_SIGNATURE = "DISTRIBUTED_DB_STORAGE"
const INITIAL_DB_VERSION catalog.DatabaseVersion = 1

const NUMBER_OF_PARALLEL_TRANSACTIONS = 1024
const COMMIT_BATCH_SIZE = 256

const TRANSACTION_TIMEOUT = 30 * time.Minute
const COMMIT_INTERVAL = 1 * time.Millisecond
const SYNC_INTERVAL = 100 * time.Millisecond

type DatabaseHeader struct {
	root    pager.PagePointer
	version catalog.DatabaseVersion

	allocatedIds uint64
	pagesCount   uint64

	walIndex wal.EntryIndex

	freeListHead pager.PagePointer
	freeListTail pager.PagePointer
}

type IDAllocator func() uint64

type Database struct {
	header        *DatabaseHeader
	syncedVersion catalog.DatabaseVersion
	idAllocator   atomic.Uint64

	wal     *wal.WAL
	pager   *pager.Pager
	config  DatabaseConfig
	storage store.Storage

	txManager *transaction.TransactionManager

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
		config:    config,
		txManager: transaction.NewTransactionManager(NUMBER_OF_PARALLEL_TRANSACTIONS),
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

	if db.wal, err = wal.NewWAL(wal.WALConfig{
		Directory:        db.config.WALDirectory,
		ArchiveDirectory: db.config.WALArchiveDirectory,
		SegmentCapacity:  db.config.WALSegmentSize,
	}); err != nil {
		return nil, fmt.Errorf("Database: failed to initialize WAL: %w", err)
	}

	if err = db.init(); err != nil {
		return nil, fmt.Errorf("Database: failed to initialize database: %w", err)
	}

	go db.runCommitLoop()
	go db.runSyncLoop()

	return db, nil
}

func (db *Database) Begin(ctx context.Context) *transaction.Transaction {
	db.mu.RLock()
	pg := db.pager.Fork(db.header.pagesCount)
	cat := db.catalog(pg)
	db.mu.RUnlock()
	return db.txManager.Begin(cat, ctx)
}

func (db *Database) Run(request func(*transaction.Transaction) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), TRANSACTION_TIMEOUT)
	defer cancel()

	tx := db.Begin(ctx)

	if err := request(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (db *Database) runCommitLoop() {
	transactions := make([]transaction.TransactionCommit, 0, COMMIT_BATCH_SIZE)
	ticker := time.NewTicker(COMMIT_INTERVAL)

	for {
		select {
		case commit := <-db.txManager.CommitQueue():
			transactions = append(transactions, commit)

			if len(transactions) == COMMIT_BATCH_SIZE {
				ticker.Stop()
				db.commitBatch(transactions)
				transactions = make([]transaction.TransactionCommit, 0, COMMIT_BATCH_SIZE)
				ticker.Reset(COMMIT_INTERVAL)
			}

		case <-ticker.C:
			if len(transactions) > 0 {
				ticker.Stop()
				db.commitBatch(transactions)
				transactions = make([]transaction.TransactionCommit, 0, COMMIT_BATCH_SIZE)
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

func (db *Database) commitBatch(transactions []transaction.TransactionCommit) {

	latestUnreachableVersion := db.txManager.LatestUnreachableVersion(db.syncedVersion)
	manager := db.catalog(db.collectReleasedPages(latestUnreachableVersion))

	var abortedTransactions []transaction.TransactionCommit
	var approvedTransactions []transaction.TransactionCommit

	var (
		applyResult catalog.ApplyResult
		err         error
	)
	for _, transaction := range transactions {
		if applyResult, err = manager.ApplyChangeEvents(transaction.ChangeEvents); err != nil {
			abortedTransactions = append(abortedTransactions, transaction)
		} else {
			approvedTransactions = append(approvedTransactions, transaction)
		}
	}

	retiredPages := applyResult.PageChanges.RetiredPages
	reusablePages := applyResult.PageChanges.ReusablePages
	pagesCount := applyResult.PageChanges.PagesCount

	newHeader := &DatabaseHeader{
		root:         applyResult.Root,
		version:      db.header.version + 1,
		allocatedIds: db.idAllocator.Load(),
		pagesCount:   pagesCount,
	}

	if err := manager.Commit(db.serializeHeader(newHeader)); err != nil {
		db.txManager.Reject(transactions, fmt.Errorf("Database: failed to commit changes: %w", err))
		return
	}

	db.wal.appendTransactions(approvedTransactions)
	db.wal.appendVersionUpdate(db.header.version + 1)

	walIndex, err := db.wal.sync()
	if err != nil {
		db.txManager.Reject(transactions, fmt.Errorf("Database: WAL flush failed: %w", err))
		return
	}
	newHeader.walIndex = walIndex

	db.txManager.Approve(approvedTransactions)
	db.txManager.Reject(abortedTransactions, errors.New("Database: transaction aborted due to conflicts with other transactions"))

	db.releasePages(latestUnreachableVersion, reusablePages)
	db.releasePages(db.header.version, retiredPages)

	db.mu.Lock()
	defer db.mu.Unlock()
	db.header = newHeader
}

func (db *Database) setupCommit(maxVersion uint64) (*freeList, *pager.Pager, *catalog.Catalog, error) {
	db.mu.RLock()
	pg := db.pager.Fork(db.header.pagesCount)
	db.mu.RUnlock()

	fl := newFreeList(db.header.freeListHead, db.header.freeListTail, pg)
	reusable, err := fl.collect(maxVersion)
	if err != nil {
		return nil, nil, nil, err
	}

	db.mu.RLock()
	pg = db.pager.Fork(db.header.pagesCount, reusable)
	manager := db.catalog(pg)
	db.mu.RUnlock()

	fl.pager = pg
	return fl, pg, manager, nil
}

func (db *Database) applyTransactions(manager *catalog.Catalog, transactions []transaction.TransactionCommit) (
	approved, aborted []transaction.TransactionCommit, last catalog.ApplyResult,
) {
	var err error
	for _, tx := range transactions {
		if last, err = manager.ApplyChangeEvents(tx.ChangeEvents); err != nil {
			aborted = append(aborted, tx)
		} else {
			approved = append(approved, tx)
		}
	}
	return
}

func (db *Database) persistCommit(fl *freeList, pg *pager.Pager, manager *catalog.Catalog, result catalog.ApplyResult, header *DatabaseHeader, reusableVersion, retiredVersion uint64) error {
	if err := fl.push(reusableVersion, result.PageChanges.ReusablePages); err != nil {
		return fmt.Errorf("push reusable pages: %w", err)
	}
	if err := fl.push(retiredVersion, result.PageChanges.RetiredPages); err != nil {
		return fmt.Errorf("push retired pages: %w", err)
	}

	header.freeListHead = fl.head
	header.freeListTail = fl.tail
	header.pagesCount = pg.PagesCount()

	return manager.Commit(db.serializeHeader(header))
}

func (db *Database) readHeader() (*DatabaseHeader, error) {
	headerBlock := pager.NewPager(db.storage, 1, db.config.PageSize).Page(pager.PagePointer(0))
	signature := headerBlock[0:len(DB_STORAGE_SIGNATURE)]

	header := &DatabaseHeader{
		root:         pager.NULL_PAGE,
		version:      INITIAL_DB_VERSION,
		pagesCount:   1,
		allocatedIds: 1,
	}

	if helpers.IsZero(signature) {
		return header, nil
	}

	if string(signature) != DB_STORAGE_SIGNATURE {
		return nil, errors.New("Database: couldn't parse storage file because of corrupted file")
	}

	signatureSize := len(DB_STORAGE_SIGNATURE)

	header.root = pager.PagePointer(binary.LittleEndian.Uint64(headerBlock[signatureSize : signatureSize+8]))
	header.version = catalog.DatabaseVersion(binary.LittleEndian.Uint64(headerBlock[signatureSize+8 : signatureSize+16]))
	header.pagesCount = binary.LittleEndian.Uint64(headerBlock[signatureSize+16 : signatureSize+24])
	header.allocatedIds = binary.LittleEndian.Uint64(headerBlock[signatureSize+24 : signatureSize+32])
	header.walIndex = wal.EntryIndex(binary.LittleEndian.Uint64(headerBlock[signatureSize+32 : signatureSize+40]))
	header.freeListHead = pager.PagePointer(binary.LittleEndian.Uint64(headerBlock[signatureSize+40 : signatureSize+48]))
	header.freeListTail = pager.PagePointer(binary.LittleEndian.Uint64(headerBlock[signatureSize+48 : signatureSize+56]))

	return header, nil
}

func (db *Database) serializeHeader(header *DatabaseHeader) []byte {
	headerBlock := make([]byte, len(DB_STORAGE_SIGNATURE)+56)
	signatureSize := len(DB_STORAGE_SIGNATURE)

	copy(headerBlock[0:signatureSize], []byte(DB_STORAGE_SIGNATURE))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize:signatureSize+8], uint64(header.root))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize+8:signatureSize+16], uint64(header.version))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize+16:signatureSize+24], uint64(header.pagesCount))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize+24:signatureSize+32], uint64(header.allocatedIds))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize+32:signatureSize+40], uint64(header.walIndex))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize+40:signatureSize+48], uint64(header.freeListHead))
	binary.LittleEndian.PutUint64(headerBlock[signatureSize+48:signatureSize+56], uint64(header.freeListTail))

	return headerBlock
}

func (db *Database) init() error {
	if db.empty() {
		return db.initWAL()
	}
	return db.recoverFromWAL()
}

func (db *Database) initWAL() error {
	db.wal.Append(EncodeEvent(events.NewUpdateDBVersion(uint64(db.header.version))))

	walIndex, err := db.wal.Sync()
	if err != nil {
		return fmt.Errorf("Database: failed to flush WAL after setup: %w", err)
	}

	db.header.walIndex = walIndex
	db.syncedVersion = db.header.version
	db.idAllocator.Store(db.header.allocatedIds)

	return nil
}

func (db *Database) recoverFromWAL() error {
	var allEvents []events.Event
	for record, err := range db.wal.Since(wal.INITIAL_ENTRY_INDEX) {
		if err != nil {
			return fmt.Errorf("Database: failed to read WAL record: %w", err)
		}
		event, err := DecodeEvent(record)
		if err != nil {
			return fmt.Errorf("Database: failed to decode WAL event: %w", err)
		}
		allEvents = append(allEvents, event)
	}

	lastIdx := -1
	for i, event := range allEvents {
		if v, ok := event.(*events.UpdateDBVersion); ok && catalog.DatabaseVersion(v.Version) == db.header.version {
			lastIdx = i
		}
	}
	if lastIdx < 0 {
		return nil
	}
	restoredEvents := allEvents[lastIdx+1:]
	walIndex := db.wal.Last()

	fl, pg, manager, err := db.setupCommit(uint64(db.header.version))
	if err != nil {
		return fmt.Errorf("Database: failed to setup WAL recovery: %w", err)
	}

	applyResult, err := manager.ApplyChangeEvents(restoredEvents)
	if err != nil {
		return fmt.Errorf("Database: failed to apply events from WAL: %w", err)
	}

	db.header.root = applyResult.Root
	db.header.version = applyResult.DatabaseVersion

	if len(applyResult.SchemaChanges.CreatedTables) > 0 {
		db.header.allocatedIds = uint64(slices.Max(applyResult.SchemaChanges.CreatedTables)) + 1
	}

	if err = db.persistCommit(fl, pg, manager, applyResult, db.header, 0, 0); err != nil {
		return fmt.Errorf("Database: failed to persist restored state: %w", err)
	}

	if err := db.storage.Flush(); err != nil {
		return fmt.Errorf("Database: failed to flush restored tables from WAL: %w", err)
	}

	db.header.walIndex = walIndex
	db.syncedVersion = db.header.version
	db.idAllocator.Store(db.header.allocatedIds)

	return nil
}

func (db *Database) catalog(pg *pager.Pager) *catalog.Catalog {
	state := catalog.CatalogState{Root: db.header.root, Version: db.header.version}

	return catalog.NewCatalog(
		state,
		func() uint64 { return db.idAllocator.Add(1) - 1 },
		pg,
	)
}

func (db *Database) empty() bool {
	return db.header.version == INITIAL_DB_VERSION && db.wal.Empty()
}

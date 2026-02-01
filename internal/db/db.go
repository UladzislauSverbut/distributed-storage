package db

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	"sync"
	"sync/atomic"
)

const DEFAULT_DIRECTORY = "/var/lib/kv"
const DEFAULT_PAGE_SIZE = 16 * 1024 // 16KB
const HEADER_SIZE = 24              // bytes

type TransactionID uint64

type Database struct {
	root              atomic.Uint64
	storage           store.Storage
	transactions      map[TransactionID]*Transaction
	nextTransactionID TransactionID

	wal       *Wal
	allocator *pager.PageAllocator

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

	rootPage, nextTransactionID, pagesCount := parseHeader(dbStorage.MemorySegment(0, HEADER_SIZE))

	db := &Database{
		root:              atomic.Uint64{},
		storage:           dbStorage,
		transactions:      make(map[TransactionID]*Transaction),
		nextTransactionID: nextTransactionID,

		wal:       NewWal(walStorage),
		allocator: pager.NewPageAllocator(pagesCount),
	}

	db.root.Store(uint64(rootPage))

	return db, nil
}

func (db *Database) StartTransaction(request func(*Transaction)) error {
	transaction, err := NewTransaction(db)

	if err != nil {
		return err
	}

	for {
		request(transaction)
		if err := transaction.Commit(); err != nil {
			if err == ErrTransactionConflict {
				continue
			}
			return err
		}
	}
}

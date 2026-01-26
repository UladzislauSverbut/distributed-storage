package db

import (
	"distributed-storage/internal/pager"
	"encoding/binary"
	"sync/atomic"
)

const DEFAULT_DIRECTORY = "/var/lib/kv"

type DBVersion uint64
type TransactionID uint64

type TableDescriptor struct {
	Root   pager.PagePointer
	Name   string
	Size   uint64
	Schema *TableSchema
}

type Database struct {
	version           DBVersion
	nextTransactionID TransactionID

	pageManager *pager.PageManager

	schemas      *Table
	descriptors  map[string]*TableDescriptor
	transactions map[TransactionID]*Transaction
}

type DatabaseConfig struct {
	Directory string
	InMemory  bool
	PageSize  int
}

func NewDatabase(config *DatabaseConfig) (*Database, error) {
	pageManager, err := initializePageManager(config)

	if err != nil {
		return nil, err
	}

	header := pageManager.Header()

	root := binary.LittleEndian.Uint64(header[:8])
	version := binary.LittleEndian.Uint64(header[8:16])
	nextTransactionID := binary.LittleEndian.Uint64(header[16:24])

	return &Database{
		version:           DBVersion(version),
		nextTransactionID: TransactionID(nextTransactionID),
		pageManager:       pageManager,
		schemas:           initializeSchemaTable(pager.PagePointer(root), pageManager),
		descriptors:       map[string]*TableDescriptor{},
		transactions:      map[TransactionID]*Transaction{},
	}, nil
}

func (db *Database) StartTransaction(request func(*Transaction)) {
	transactionId := atomic.AddUint64((*uint64)(&db.nextTransactionID), 1)

	transaction := NewTransaction(db, TransactionID(transactionId))

	db.transactions[TransactionID(transactionId)] = transaction

	request(transaction)

	transaction.Commit()
}

func (db *Database) SaveChanges() error {
	if err := db.pageManager.SavePages(); err != nil {
		return err
	}

	db.version++

	header := db.pageManager.Header()

	binary.LittleEndian.PutUint64(header[:8], uint64(db.schemas.Root()))
	binary.LittleEndian.PutUint64(header[8:16], uint64(db.version))
	binary.LittleEndian.PutUint64(header[16:24], uint64(db.nextTransactionID))

	return db.pageManager.SaveHeader(header)
}

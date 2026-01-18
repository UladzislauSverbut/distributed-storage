package db

import (
	"distributed-storage/internal/kv"
	"distributed-storage/internal/pager"
	"distributed-storage/internal/vals"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const DEFAULT_DIRECTORY = "/var/lib/kv"
const META_TABLE_NAME = "@meta"
const SCHEMA_TABLE_NAME = "@schemas"

type DBVersion uint64
type TransactionID uint64

type Database struct {
	root              kv.KVRootPointer
	version           DBVersion
	pageManager       *pager.PageManager
	nextTransactionID TransactionID

	transactions sync.Map
	tables       sync.Map
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

	db := &Database{
		root:              kv.KVRootPointer(root),
		version:           DBVersion(version),
		pageManager:       pageManager,
		nextTransactionID: TransactionID(nextTransactionID),
	}

	db.initSystemTables()

	return db, nil
}

func (db *Database) StartTransaction(request func(*Transaction)) {
	transactionId := atomic.AddUint64((*uint64)(&db.nextTransactionID), 1)

	transaction := NewTransaction(db, TransactionID(transactionId))

	db.transactions.Store(transactionId, transaction)

	request(transaction)

	transaction.Commit()
}

func (db *Database) initSystemTables() {
	db.StartTransaction(func(tx *Transaction) {
		tx.CreateTable(
			&TableSchema{
				Name:         META_TABLE_NAME,
				ColumnNames:  []string{"key", "value"},
				PrimaryIndex: []string{"key"},
				ColumnTypes:  map[string]vals.ValueType{"key": vals.TYPE_STRING, "value": vals.TYPE_STRING},
			},
		)

		tx.CreateTable(
			&TableSchema{
				Name:         SCHEMA_TABLE_NAME,
				ColumnNames:  []string{"name", "definition"},
				PrimaryIndex: []string{"name"},
				ColumnTypes:  map[string]vals.ValueType{"name": vals.TYPE_STRING, "definition": vals.TYPE_STRING},
			},
		)

		tx.Commit()
	})
}

// func (db *Database) getTableSchema(tableName string) *TableSchema {
// 	schemaTable := db.Get(SCHEMA_TABLE_NAME)

// 	query := vals.NewObject().
// 		Set("name", vals.NewString(tableName))

// 	record, err := schemaTable.Get(query)

// 	if err != nil {
// 		panic(fmt.Errorf("Database: can`t read schema table %w", err))
// 	}

// 	if record == nil {
// 		return nil
// 	}

// 	tableSchema := &TableSchema{}

// 	if err := json.Unmarshal([]byte(record.Get("definition").(*vals.StringValue).Value()), tableSchema); err != nil {
// 		panic(fmt.Errorf("Database: can`t parse schema %w", err))
// 	}

// 	return tableSchema
// }

// func (db *Database) saveTableSchema(schema *TableSchema) error {
// 	transaction := NewTransaction(db, SCHEMA_TABLE_NAME)

// 	schemaTable := db.Get(SCHEMA_TABLE_NAME)
// 	stringifiedSchema, _ := json.Marshal(schema)

// 	query := vals.NewObject().
// 		Set("name", vals.NewString(schema.Name)).
// 		Set("definition", vals.NewString(string(stringifiedSchema)))

// 	if err := schemaTable.Insert(query); err != nil {
// 		transaction.Abort()

// 		return fmt.Errorf("Database: can't save schema %w", err)
// 	}

// 	return transaction.Commit()
// }

// func (db *Database) validateTableSchema(schema *TableSchema) error {
// 	return nil
// }

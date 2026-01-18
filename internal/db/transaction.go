package db

import (
	"distributed-storage/internal/kv"
	"distributed-storage/internal/vals"
	"encoding/json"
	"fmt"
	"sync"
)

type Transaction struct {
	id     TransactionID
	active bool
	db     *Database
	mu     sync.Mutex
}

func NewTransaction(db *Database, id TransactionID) *Transaction {
	tx := &Transaction{
		id:     id,
		db:     db,
		active: true,
	}

	db.transactions.Store(id, tx)

	return tx
}

func (tx *Transaction) Commit() error {
	tx.mu.Lock()

	if !tx.active {
		return fmt.Errorf("Transaction: couldn't commit transaction with id %d because it is not active")
	}

	tx.active = false
	tx.mu.Unlock()

	tx.db.transactions.Delete(tx.id)

	return nil
}

func (tx *Transaction) Rollback() {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if !tx.active {
		return
	}

	tx.db.transactions.Delete(tx.id)
}

func (tx *Transaction) Table(tableName string) *Table {
	table, exist := tx.db.tables.Load(tableName)

	if exist {
		return table.(*Table)
	}

	query := vals.NewObject().
		Set("name", vals.NewString(tableName))

	schemaTable, _ := tx.db.tables.Load(SCHEMA_TABLE_NAME)

	record, err := schemaTable.(*Table).Get(query)

	if err != nil {
		panic(fmt.Errorf("Transaction: can`t read schema table %w", err))
	}

	if record == nil {
		return nil
	}

	tableSchema := &TableSchema{}

	if err := json.Unmarshal([]byte(record.Get("definition").(*vals.StringValue).Value()), tableSchema); err != nil {
		panic(fmt.Errorf("Transaction: can`t parse schema %w", err))
	}

	table = &Table{schema: tableSchema, kv: kv.NewKeyValue(tx.db.root, tx.db.pageManager)}

	tx.db.tables.Store(tableName, table)

	return table.(*Table)
}

func (tx *Transaction) CreateTable(schema *TableSchema) (*Table, error) {
	if err := tx.validateTableSchema(schema); err != nil {
		return nil, err
	}

	table := &Table{schema: schema, kv: kv.NewKeyValue(tx.db.root, tx.db.pageManager)}

	_, exist := tx.db.tables.LoadOrStore(schema.Name, table)

	if exist {
		return nil, fmt.Errorf("Transaction: couldn't create table %s because it`s already exist", schema.Name)

	}

	return table, nil
}

func (tx *Transaction) List() []*vals.Object {
	schemaTable := tx.Table(SCHEMA_TABLE_NAME)

	return schemaTable.GetAll()
}

func (tx *Transaction) validateTableSchema(schema *TableSchema) error {
	if schema.Name == "" {
		return fmt.Errorf("Transaction: couldn't create table because schema must have a name")
	}

	if len(schema.ColumnNames) == 0 {
		return fmt.Errorf("Transaction: couldn't create table because schema must have at least one column")
	}

	if len(schema.PrimaryIndex) == 0 {
		return fmt.Errorf("Transaction: couldn't create table because schema must have a primary index")
	}

	return nil
}

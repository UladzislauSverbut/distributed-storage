package db

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/vals"
	"encoding/json"
	"fmt"
)

type Transaction struct {
	id     TransactionID
	active bool
	db     *Database
}

func NewTransaction(db *Database, id TransactionID) *Transaction {
	tx := &Transaction{
		id:     id,
		db:     db,
		active: true,
	}

	db.transactions[id] = tx

	return tx
}

func (tx *Transaction) Commit() error {
	if !tx.active {
		return fmt.Errorf("Transaction: couldn't commit transaction with id %d because it is not active")
	}

	if err := tx.db.SaveChanges(); err != nil {
		tx.Rollback()

		return err
	}

	tx.active = false

	delete(tx.db.transactions, tx.id)

	return nil
}

func (tx *Transaction) Rollback() {
	if !tx.active {
		return
	}

	delete(tx.db.transactions, tx.id)
}

func (tx *Transaction) Table(tableName string) (*Table, error) {
	table, exist := tx.db.tables[tableName]

	if exist {
		return table, nil
	}

	query := vals.NewObject().Set("name", vals.NewString(tableName))

	record, err := tx.db.schemas.Get(query)

	if err != nil {
		panic(fmt.Errorf("Transaction: can`t read schema table %w", err))
	}

	if record == nil {
		return nil, nil
	}

	tableSchema := &TableSchema{}

	definition := record.Get("definition").(*vals.StringValue).Value()
	pointer := record.Get("root").(*vals.IntValue[uint64]).Value()
	size := record.Get("size").(*vals.IntValue[uint64]).Value()

	if err := json.Unmarshal([]byte(definition), tableSchema); err != nil {
		fmt.Errorf("Transaction: can`t parse broken table schema %w", err)
	}

	table, err = NewTable(pager.PagePointer(pointer), tx.db.pageManager, tableSchema, size)

	if err != nil {
		return nil, err
	}

	tx.db.tables[tableName] = table

	return table, nil
}

func (tx *Transaction) CreateTable(schema *TableSchema) (*Table, error) {
	_, exist := tx.db.tables[schema.Name]

	if exist {
		return nil, fmt.Errorf("Transaction: couldn't create table %s because it`s already exist", schema.Name)
	}

	table, err := NewTable(pager.NULL_PAGE, tx.db.pageManager, schema, 0)

	if err != nil {
		return nil, err
	}

	stringifiedSchema, _ := json.Marshal(table.schema)

	query := vals.NewObject().
		Set("name", vals.NewString(table.schema.Name)).
		Set("definition", vals.NewString(string(stringifiedSchema))).
		Set("root", vals.NewInt(table.Root())).
		Set("size", vals.NewInt(table.Size()))

	if err := tx.db.schemas.Insert(query); err != nil {
		return nil, fmt.Errorf("Transaction: couldn't save table schema %w", err)
	}

	tx.db.tables[table.schema.Name] = table

	return table, nil
}

func (tx *Transaction) List() []*vals.Object {
	return tx.db.schemas.GetAll()
}

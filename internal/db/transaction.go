package db

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/vals"
	"encoding/json"
	"fmt"
)

type Transaction struct {
	id             TransactionID
	active         bool
	affectedTables map[string]*Table

	db *Database
}

func NewTransaction(db *Database, id TransactionID) *Transaction {
	tx := &Transaction{
		id:     id,
		db:     db,
		active: true,

		affectedTables: map[string]*Table{},
	}

	db.transactions[id] = tx

	return tx
}

func (tx *Transaction) Commit() error {
	if !tx.active {
		return fmt.Errorf("Transaction: couldn't commit transaction with id %d because it is not active")
	}

	for tableName, table := range tx.affectedTables {
		if tx.db.tables[tableName] != nil && tx.db.tables[tableName].Root == table.Root() {
			continue
		}

		stringifiedSchema, _ := json.Marshal(table.schema)

		schemaRecord := vals.NewObject().
			Set("name", vals.NewString(table.schema.Name)).
			Set("definition", vals.NewString(string(stringifiedSchema))).
			Set("root", vals.NewInt(table.kv.Root())).
			Set("size", vals.NewInt(table.size))

		if err := tx.db.schemas.Upsert(schemaRecord); err != nil {
			tx.Rollback()
			return fmt.Errorf("Transaction: couldn't save table %s: %w", tableName, err)
		}
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
	if table, exist := tx.affectedTables[tableName]; exist {
		return table, nil
	}

	if config, exist := tx.db.tables[tableName]; exist {
		table, _ := NewTable(config, tx.db.pageManager)

		tx.affectedTables[tableName] = table
		return table, nil
	}

	query := vals.NewObject().Set("name", vals.NewString(tableName))

	record, err := tx.db.schemas.Get(query)

	if err != nil {
		return nil, fmt.Errorf("Transaction: can`t read schema table %w because it's corrupted", err)
	}

	if record == nil {
		return nil, nil
	}

	tableSchema := &TableSchema{}

	definition := record.Get("definition").(*vals.StringValue).Value()
	pointer := record.Get("root").(*vals.IntValue[uint64]).Value()
	size := record.Get("size").(*vals.IntValue[uint64]).Value()

	if err := json.Unmarshal([]byte(definition), tableSchema); err != nil {
		return nil, fmt.Errorf("Transaction: can`t parse broken table schema %w", err)
	}

	config := &TableConfig{
		Root:   pager.PagePointer(pointer),
		Schema: tableSchema,
		Size:   size,
	}

	table, _ := NewTable(config, tx.db.pageManager)

	tx.db.tables[tableName] = config
	tx.affectedTables[tableName] = table

	return table, nil
}

func (tx *Transaction) CreateTable(schema *TableSchema) (*Table, error) {
	_, exist := tx.db.tables[schema.Name]

	if exist {
		return nil, fmt.Errorf("Transaction: couldn't create table %s because it`s already exist", schema.Name)
	}

	config := &TableConfig{
		Root:   pager.NULL_PAGE,
		Schema: schema,
		Size:   0,
	}

	table, err := NewTable(config, tx.db.pageManager)

	if err != nil {
		return nil, err
	}

	tx.db.tables[table.schema.Name] = config
	tx.affectedTables[schema.Name] = table

	return table, nil
}

func (tx *Transaction) List() []*vals.Object {
	return tx.db.schemas.GetAll()
}

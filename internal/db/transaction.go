package db

import (
	"distributed-storage/internal/pager"
	"errors"
	"fmt"
	"sync/atomic"
)

var ErrTransactionConflict = errors.New("Transaction: couldn't commit transaction because root page was modified by another transaction")

type Transaction struct {
	id             TransactionID
	root           pager.PagePointer
	active         bool
	catalog        *Catalog
	pageManager    *pager.PageManager
	affectedTables map[string]*Table

	db *Database
}

func NewTransaction(db *Database) (*Transaction, error) {
	root := db.root.Load()
	pageManager, err := pager.NewPageManager(db.storage, db.allocator, db.config.PageSize)
	if err != nil {
		return nil, err
	}

	catalog, err := NewCatalog(root, pageManager)
	if err != nil {
		return nil, err
	}

	tx := &Transaction{
		id: TransactionID(atomic.AddUint64((*uint64)(&db.nextTransactionID), 1)),

		active:         true,
		catalog:        catalog,
		pageManager:    pageManager,
		affectedTables: map[string]*Table{},

		db: db,
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	db.transactions[tx.id] = tx

	return tx, nil
}

func (tx *Transaction) Commit() (err error) {
	defer func() {
		if panic := recover(); panic != nil {
			err = fmt.Errorf("Transaction: panic during commit: %v", panic)
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	if !tx.active {
		return fmt.Errorf("Transaction: couldn't commit transaction with id %d because it is not active", tx.id)
	}

	events, err := tx.saveTableChanges()
	if err != nil {
		return err
	}

	if err := tx.db.wal.Write(events); err != nil {
		return err
	}

	if err := tx.pageManager.WritePages(); err != nil {
		return err
	}

	if ok := tx.db.root.CompareAndSwap(tx.root, tx.catalog.Root()); !ok {
		return ErrTransactionConflict
	}

	tx.active = false

	return nil
}

func (tx *Transaction) Rollback() {
	if !tx.active {
		return
	}

	tx.resetTableChanges()
	tx.active = false

	delete(tx.db.transactions, tx.id)
}

func (tx *Transaction) Table(tableName string) (*Table, error) {
	if table, ok := tx.affectedTables[tableName]; ok {
		return table, nil
	}

	descriptor, err := tx.catalog.getTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("Transaction: couldn't get table %s: %w", tableName, err)
	}

	table, err := NewTable(descriptor.root, tx.pageManager, descriptor.schema, descriptor.size)
	if err != nil {
		return nil, fmt.Errorf("Transaction: couldn't initialize table %s: %w", tableName, err)
	}

	tx.affectedTables[tableName] = table

	return table, nil
}

func (tx *Transaction) CreateTable(schema *TableSchema) (*Table, error) {
	descriptor, _ := tx.catalog.getTable(schema.Name)
	if descriptor != nil {
		return nil, fmt.Errorf("Transaction: couldn't create table %s because it's already exist", schema.Name)
	}

	table, err := NewTable(pager.NULL_PAGE, tx.pageManager, schema, 0)
	if err != nil {
		return nil, err
	}

	if err := tx.catalog.updateTable(schema.Name, &TableDescriptor{root: table.Root(), name: table.schema.Name, schema: schema, size: 0}); err != nil {
		return nil, err
	}

	tx.affectedTables[schema.Name] = table

	return table, nil
}

func (tx *Transaction) saveTableChanges() ([]Event, error) {
	events := []Event{&StartTransaction{TxID: tx.id}}

	for name, table := range tx.affectedTables {
		// if table doesn't have events than table was not modified
		if len(table.events) == 0 {
			continue
		}

		if err := tx.catalog.updateTable(name, &TableDescriptor{root: table.Root(), name: name, schema: table.schema, size: table.size}); err != nil {
			return nil, err
		}

		events = append(events, table.events...)
	}

	if len(events) == 1 {
		// nothing to commit
		return nil, nil
	}

	return append(events,
		&FreePages{TxID: tx.id, Pages: tx.pageManager.State().ReleasedPages.Values()},
		&CommitTransaction{TxID: tx.id},
	), nil

}

func (tx *Transaction) resetTableChanges() {
	tx.db.allocator.Free(tx.pageManager.State().AllocatedPages.Values())
}

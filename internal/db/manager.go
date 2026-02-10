package db

import (
	"bytes"
	"distributed-storage/internal/events"
	"distributed-storage/internal/kv"
	"distributed-storage/internal/pager"
	"distributed-storage/internal/vals"
	"encoding/json"
	"fmt"
)

var catalogSchema = TableSchema{
	Name:         "@catalog",
	ColumnNames:  []string{"name", "definition", "root", "size"},
	PrimaryIndex: []string{"name"},
	ColumnTypes:  map[string]vals.ValueType{"name": vals.TYPE_STRING, "definition": vals.TYPE_STRING, "root": vals.TYPE_UINT64, "size": vals.TYPE_UINT64},
}

type TableManager struct {
	catalog      *Table
	loadedTables map[string]*Table
	allocator    *pager.PageAllocator
}

func NewTableManager(db *Database) *TableManager {
	db.mu.RLock()
	defer db.mu.RUnlock()

	allocator := pager.NewPageAllocator(db.storage, db.pagesCount, db.config.PageSize)
	// The catalog is stored as a regular table with predefined schema, so it NewTable can't return error
	catalog, _ := NewTable(db.root, allocator, &catalogSchema)

	return &TableManager{
		catalog:      catalog,
		loadedTables: make(map[string]*Table),
		allocator:    allocator,
	}
}

func (manager *TableManager) Table(name string) (*Table, error) {
	if table, ok := manager.loadedTables[name]; ok {
		return table, nil
	}

	query := vals.NewObject().Set("name", vals.NewString(name))

	record, err := manager.catalog.Get(query)

	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't read table schema: %w", err)
	}

	if record == nil {
		return nil, nil
	}

	definition := record.Get("definition").(*vals.StringValue).Value()
	root := record.Get("root").(*vals.IntValue[uint64]).Value()
	schema := &TableSchema{}

	if err := json.Unmarshal([]byte(definition), schema); err != nil {
		return nil, fmt.Errorf("Catalog: couldn't parse table schema: %w", err)
	}

	manager.loadedTables[name], err = NewTable(root, manager.allocator, schema)
	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't initialize table %s: %w", name, err)
	}

	return manager.loadedTables[name], nil
}

func (manager *TableManager) UpdateTable(name string, table *Table) error {
	stringifiedSchema, _ := json.Marshal(table.schema)

	schemaRecord := vals.NewObject().
		Set("name", vals.NewString(table.schema.Name)).
		Set("definition", vals.NewString(string(stringifiedSchema))).
		Set("root", vals.NewInt(table.Root()))

	if err := manager.catalog.Upsert(schemaRecord); err != nil {
		return fmt.Errorf("Catalog: couldn't save table %s: %w", name, err)
	}

	manager.loadedTables[name] = table
	return nil
}

func (manager *TableManager) CreateTable(schema *TableSchema) (*Table, error) {
	table, err := NewTable(pager.NULL_PAGE, manager.allocator, schema)
	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't create table %s: %w", schema.Name, err)
	}

	if err := manager.UpdateTable(schema.Name, table); err != nil {
		return nil, fmt.Errorf("Catalog: couldn't register table %s in catalog: %w", schema.Name, err)
	}

	return table, nil
}

func (manager *TableManager) DeleteTable(name string) error {
	query := vals.NewObject().Set("name", vals.NewString(name))

	if err := manager.catalog.Delete(query); err != nil {
		return fmt.Errorf("Catalog: couldn't delete table %s from catalog: %w", name, err)
	}

	delete(manager.loadedTables, name)
	return nil
}

func (manager *TableManager) ChangeEvents() []TableEvent {
	events := make([]TableEvent, 0)

	for _, table := range manager.loadedTables {
		tableEvents := table.ChangeEvents()
		if len(tableEvents) > 0 {
			events = append(events, tableEvents...)
		}
	}

	return events
}

func (manager *TableManager) ApplyChangeEvents(changeEvents []TableEvent) (err error) {
	// Save previous state to be able to rollback in case of error during apply
	previousRootTable := manager.catalog.clone()

	defer func() {
		if err != nil {
			// If any error occurs during apply, rollback to the previous state of catalog
			manager.catalog = previousRootTable
		}
	}()

	for _, changeEvent := range changeEvents {
		switch event := changeEvent.(type) {
		case *events.CreateTable:
			if err = manager.applyCreateTableEvent(event); err != nil {
				return err
			}
		case *events.DeleteTable:
			if err = manager.applyDeleteTableEvent(event); err != nil {
				return err
			}
		case *events.DeleteEntry:
			if err = manager.applyDeleteEntryEvent(event); err != nil {
				return err
			}
		case *events.UpdateEntry:
			if err = manager.applyUpdateEntryEvent(event); err != nil {
				return err
			}
		case *events.InsertEntry:
			if err = manager.applyInsertEntryEvent(event); err != nil {
				return err
			}
		case *events.StartTransaction,
			*events.CommitTransaction,
			*events.FreePages:
			// These events are not related to table schema or entries, so we can ignore them during replay
			continue

		default:
			return fmt.Errorf("Catalog: couldn't apply unknown event %s", event.Name())
		}
	}

	return nil
}

func (manager *TableManager) PersistTables() error {
	for name, table := range manager.loadedTables {
		if err := manager.UpdateTable(name, table); err != nil {
			return fmt.Errorf("Catalog: couldn't save table %s: %w", name, err)
		}
	}

	if err := manager.allocator.Save(); err != nil {
		return fmt.Errorf("Catalog: couldn't save page manager state: %w", err)
	}

	return nil
}

func (manager *TableManager) Root() pager.PagePointer {
	return manager.catalog.Root()
}

func (manager *TableManager) applyCreateTableEvent(event *events.CreateTable) error {
	var schema TableSchema
	if err := json.Unmarshal(event.Schema, &schema); err != nil {
		return fmt.Errorf("CreateTable Apply: couldn't parse schema: %w", err)
	}

	table, err := manager.Table(event.TableName)
	if err != nil {
		return fmt.Errorf("CreateTable Replay: %w", err)
	}
	if table != nil {
		return fmt.Errorf("CreateTable Apply: couldn't create table %s because it already exists", event.TableName)
	}

	if _, err := manager.CreateTable(&schema); err != nil {
		return fmt.Errorf("CreateTable Apply: %w", err)
	}

	return nil
}

func (manager *TableManager) applyDeleteTableEvent(event *events.DeleteTable) error {
	table, err := manager.Table(event.TableName)
	if err != nil {
		return fmt.Errorf("DeleteTable Apply: %w", err)
	}
	if table == nil {
		return fmt.Errorf("DeleteTable Apply: table %s not found", event.TableName)
	}

	if err := manager.DeleteTable(event.TableName); err != nil {
		return fmt.Errorf("DeleteTable Apply: %w", err)
	}

	return nil
}

func (manager *TableManager) applyDeleteEntryEvent(event *events.DeleteEntry) error {
	table, err := manager.Table(event.TableName)
	if err != nil {
		return fmt.Errorf("DeleteEntry Apply: %w", err)
	}
	if table == nil {
		return fmt.Errorf("DeleteEntry Apply: table %s not found", event.TableName)
	}

	response, err := table.kv.Delete(&kv.DeleteRequest{Key: event.Key})
	if err != nil {
		return fmt.Errorf("DeleteEntry Apply: %w", err)
	}
	if !bytes.Equal(response.OldValue, event.Value) {
		return fmt.Errorf("DeleteEntry Apply: old value does not match expected value")
	}

	return nil
}

func (manager *TableManager) applyUpdateEntryEvent(event *events.UpdateEntry) error {
	table, err := manager.Table(event.TableName)
	if err != nil {
		return fmt.Errorf("UpdateEntry Apply: %w", err)
	}
	if table == nil {
		return fmt.Errorf("UpdateEntry Apply: table %s not found", event.TableName)
	}

	response, err := table.kv.Set(&kv.SetRequest{Key: event.Key, Value: event.NewValue})
	if err != nil {
		return fmt.Errorf("UpdateEntry Apply: %w", err)
	}
	if !bytes.Equal(response.OldValue, event.OldValue) {
		return fmt.Errorf("UpdateEntry Apply: old value does not match expected value")
	}

	return nil
}

func (manager *TableManager) applyInsertEntryEvent(event *events.InsertEntry) error {
	table, err := manager.Table(event.TableName)
	if err != nil {
		return fmt.Errorf("InsertEntry Apply: %w", err)
	}
	if table == nil {
		return fmt.Errorf("InsertEntry Apply: table %s not found", event.TableName)
	}

	response, err := table.kv.Set(&kv.SetRequest{Key: event.Key, Value: event.Value})
	if err != nil {
		return fmt.Errorf("InsertEntry Apply: %w", err)
	}
	if response.Updated {
		return fmt.Errorf("InsertEntry Apply: expected insert but key already existed")
	}

	return nil
}

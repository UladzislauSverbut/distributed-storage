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

type Catalog struct {
	catalogTable *Table
	loadedTables map[string]*Table
	pageManager  *pager.PageManager
}

func NewCatalog(db *Database) *Catalog {
	db.mu.RLock()
	defer db.mu.RUnlock()

	pageManager := pager.NewPageManager(db.storage, db.pagesCount, db.config.PageSize)

	// The catalog is stored as a regular table with predefined schema, so it NewTable can't return error
	table, _ := NewTable(db.root, pageManager, &catalogSchema)

	return &Catalog{
		catalogTable: table,
		loadedTables: make(map[string]*Table),
		pageManager:  pageManager,
	}
}

func (catalog *Catalog) Table(name string) (*Table, error) {
	if table, ok := catalog.loadedTables[name]; ok {
		return table, nil
	}

	query := vals.NewObject().Set("name", vals.NewString(name))

	record, err := catalog.catalogTable.Get(query)

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

	catalog.loadedTables[name], err = NewTable(root, catalog.pageManager, schema)
	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't initialize table %s: %w", name, err)
	}

	return catalog.loadedTables[name], nil
}

func (catalog *Catalog) UpdateTable(name string, table *Table) error {
	stringifiedSchema, _ := json.Marshal(table.schema)

	schemaRecord := vals.NewObject().
		Set("name", vals.NewString(table.schema.Name)).
		Set("definition", vals.NewString(string(stringifiedSchema))).
		Set("root", vals.NewInt(table.Root()))

	if err := catalog.catalogTable.Upsert(schemaRecord); err != nil {
		return fmt.Errorf("Catalog: couldn't save table %s: %w", name, err)
	}

	catalog.loadedTables[name] = table

	return nil
}

func (catalog *Catalog) CreateTable(schema *TableSchema) (*Table, error) {
	table, err := NewTable(pager.NULL_PAGE, catalog.pageManager, schema)
	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't create table %s: %w", schema.Name, err)
	}

	if err := catalog.UpdateTable(schema.Name, table); err != nil {
		return nil, fmt.Errorf("Catalog: couldn't register table %s in catalog: %w", schema.Name, err)
	}

	return table, nil
}

func (catalog *Catalog) DeleteTable(name string) error {
	query := vals.NewObject().Set("name", vals.NewString(name))

	if err := catalog.catalogTable.Delete(query); err != nil {
		return fmt.Errorf("Catalog: couldn't delete table %s from catalog: %w", name, err)
	}

	delete(catalog.loadedTables, name)
	return nil
}

func (catalog *Catalog) ChangeEvents() []TableEvent {
	events := make([]TableEvent, 0)

	for _, table := range catalog.loadedTables {
		tableEvents := table.ChangeEvents()
		if len(tableEvents) > 0 {
			events = append(events, tableEvents...)
		}
	}

	return events
}

func (catalog *Catalog) ApplyChangeEvents(changeEvents []TableEvent) (err error) {
	// Save previous state to be able to rollback in case of error during apply
	previousRootTable := catalog.catalogTable.clone()

	defer func() {
		if err != nil {
			// If any error occurs during apply, rollback to the previous state of catalog
			catalog.catalogTable = previousRootTable
		}
	}()

	for _, changeEvent := range changeEvents {
		switch event := changeEvent.(type) {
		case *events.CreateTable:
			if err = catalog.applyCreateTableEvent(event); err != nil {
				return err
			}
		case *events.DeleteTable:
			if err = catalog.applyDeleteTableEvent(event); err != nil {
				return err
			}
		case *events.DeleteEntry:
			if err = catalog.applyDeleteEntryEvent(event); err != nil {
				return err
			}
		case *events.UpdateEntry:
			if err = catalog.applyUpdateEntryEvent(event); err != nil {
				return err
			}
		case *events.InsertEntry:
			if err = catalog.applyInsertEntryEvent(event); err != nil {
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

func (catalog *Catalog) PersistTables() error {
	for name, table := range catalog.loadedTables {
		if err := catalog.UpdateTable(name, table); err != nil {
			return fmt.Errorf("Catalog: couldn't save table %s: %w", name, err)
		}
	}

	if err := catalog.pageManager.Save(); err != nil {
		return fmt.Errorf("Catalog: couldn't save page manager state: %w", err)
	}

	return nil
}

func (catalog *Catalog) Root() pager.PagePointer {
	return catalog.catalogTable.Root()
}

func (catalog *Catalog) applyCreateTableEvent(event *events.CreateTable) error {
	var schema TableSchema
	if err := json.Unmarshal(event.Schema, &schema); err != nil {
		return fmt.Errorf("CreateTable Apply: couldn't parse schema: %w", err)
	}

	// Check if table already exists
	table, err := catalog.Table(event.TableName)
	if err != nil {
		return fmt.Errorf("CreateTable Replay: %w", err)
	}

	if table != nil {
		return fmt.Errorf("CreateTable Apply: couldn't create table %s because it already exists", event.TableName)
	}

	if _, err := catalog.CreateTable(&schema); err != nil {
		return fmt.Errorf("CreateTable Apply: %w", err)
	}
	return nil
}

func (catalog *Catalog) applyDeleteTableEvent(event *events.DeleteTable) error {
	table, err := catalog.Table(event.TableName)
	if err != nil {
		return fmt.Errorf("DeleteTable Apply: %w", err)
	}
	if table == nil {
		return fmt.Errorf("DeleteTable Apply: table %s not found", event.TableName)
	}
	if err := catalog.DeleteTable(event.TableName); err != nil {
		return fmt.Errorf("DeleteTable Apply: %w", err)
	}
	return nil
}

func (catalog *Catalog) applyDeleteEntryEvent(event *events.DeleteEntry) error {
	table, err := catalog.Table(event.TableName)
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

func (catalog *Catalog) applyUpdateEntryEvent(event *events.UpdateEntry) error {
	table, err := catalog.Table(event.TableName)
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

func (catalog *Catalog) applyInsertEntryEvent(event *events.InsertEntry) error {
	table, err := catalog.Table(event.TableName)
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

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

	allocator *pager.PageAllocator
}

func NewTableManager(root pager.PagePointer, allocator *pager.PageAllocator) *TableManager {
	catalog, _ := NewTable(root, allocator, &catalogSchema)

	return &TableManager{
		catalog:      catalog,
		loadedTables: make(map[string]*Table),

		allocator: allocator,
	}
}

func (manager *TableManager) Table(name string) (*Table, error) {
	if table, ok := manager.loadedTables[name]; ok {
		return table, nil
	}

	record, err := manager.catalog.Get(manager.constructTableQuery(name))

	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't read table schema: %w", err)
	}

	if record == nil {
		return nil, nil
	}

	manager.loadedTables[name] = manager.recordToTable(record)

	return manager.loadedTables[name], nil
}

func (manager *TableManager) UpdateTable(name string, table *Table) error {
	record := manager.tableToRecord(table)

	oldRecord, err := manager.catalog.Update(record)
	if err != nil {
		return fmt.Errorf("Catalog: couldn't save table %s: %w", name, err)
	}

	manager.loadedTables[name] = table

	table.changeEvents = append(table.changeEvents, events.NewUpdateTable(
		table.Name(),
		record.Get("definition").Serialize(),
		oldRecord.Get("definition").Serialize(),
	))

	return nil
}

func (manager *TableManager) CreateTable(schema *TableSchema) (*Table, error) {
	table, err := NewTable(pager.NULL_PAGE, manager.allocator, schema)
	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't create table %s: %w", schema.Name, err)
	}

	record := manager.tableToRecord(table)

	if err := manager.catalog.Insert(record); err != nil {
		return nil, fmt.Errorf("Catalog: couldn't create table %s: %w", schema.Name, err)
	}

	manager.loadedTables[schema.Name] = table

	table.changeEvents = append(table.changeEvents, events.NewCreateTable(table.Name(), record.Get("definition").Serialize()))

	return table, nil
}

func (manager *TableManager) DeleteTable(name string) error {

	oldRecord, err := manager.catalog.Delete(manager.constructTableQuery(name))
	if err != nil {
		return fmt.Errorf("Catalog: couldn't delete table %s from catalog: %w", name, err)
	}

	if manager.loadedTables[name] == nil {
		manager.loadedTables[name] = manager.recordToTable(oldRecord)
	}

	table := manager.loadedTables[name]

	table.changeEvents = append(table.changeEvents, events.NewDeleteTable(name))

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

func (manager *TableManager) WriteTables() error {
	for name, table := range manager.loadedTables {
		if err := manager.UpdateTable(name, table); err != nil {
			return fmt.Errorf("Catalog: couldn't update table %s: %w", name, err)
		}
	}

	if err := manager.allocator.Save(); err != nil {
		return fmt.Errorf("Catalog: couldn't save state: %w", err)
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

func (manager *TableManager) constructTableQuery(name string) *vals.Object {
	return vals.NewObject().Set("name", vals.NewString(name))
}

func (manager *TableManager) recordToTable(record *vals.Object) *Table {
	definition := record.Get("definition").(*vals.StringValue).Value()
	root := record.Get("root").(*vals.IntValue[uint64]).Value()

	schema := &TableSchema{}
	json.Unmarshal([]byte(definition), schema)

	table, _ := NewTable(root, manager.allocator, schema)
	return table
}

func (manager *TableManager) tableToRecord(table *Table) *vals.Object {
	stringifiedSchema, _ := json.Marshal(table.schema)

	return vals.NewObject().
		Set("name", vals.NewString(table.schema.Name)).
		Set("definition", vals.NewString(string(stringifiedSchema))).
		Set("root", vals.NewInt(table.Root()))
}

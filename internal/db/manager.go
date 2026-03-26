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

const HEADER_SIZE = len(DB_STORAGE_SIGNATURE) + 32
const HEADER_PAGE = pager.PagePointer(0)

var catalogSchema = TableSchema{
	Name:         "@catalog",
	ColumnNames:  []string{"name", "definition", "root", "size"},
	PrimaryIndex: []string{"name"},
	ColumnTypes:  map[string]vals.ValueType{"name": vals.TYPE_STRING, "definition": vals.TYPE_STRING, "root": vals.TYPE_UINT64, "size": vals.TYPE_UINT64},
}

type TableManager struct {
	catalog      *Table
	loadedTables map[string]*Table

	pager *pager.Pager
}

func newTableManager(root pager.PagePointer, pager *pager.Pager) *TableManager {
	catalog, _ := newTable(root, pager, &catalogSchema)

	return &TableManager{
		catalog:      catalog,
		loadedTables: make(map[string]*Table),

		pager: pager,
	}
}

func (manager *TableManager) table(name string) (*Table, error) {
	if table, ok := manager.loadedTables[name]; ok {
		return table, nil
	}

	record, err := manager.catalog.Get(manager.tableQuery(name))

	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't read table schema: %w", err)
	}

	if record == nil {
		return nil, nil
	}

	manager.loadedTables[name] = manager.decodeTable(record)

	return manager.loadedTables[name], nil
}

func (manager *TableManager) updateTable(name string, table *Table) error {
	record := manager.encodeTable(table)

	oldRecord, err := manager.catalog.Update(record)
	if err != nil {
		return fmt.Errorf("Catalog: couldn't update table %s: %w", name, err)
	}

	manager.loadedTables[name] = table

	table.changeEvents = append(table.changeEvents, events.NewUpdateTable(
		table.Name(),
		[]byte(oldRecord.GetString("definition")),
		[]byte(record.GetString("definition")),
	))

	return nil
}

func (manager *TableManager) createTable(schema *TableSchema) (*Table, error) {
	table, err := newTable(pager.NULL_PAGE, manager.pager, schema)
	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't create table %s: %w", schema.Name, err)
	}

	record := manager.encodeTable(table)

	if err := manager.catalog.Insert(record); err != nil {
		return nil, fmt.Errorf("Catalog: couldn't create table %s: %w", schema.Name, err)
	}

	manager.loadedTables[schema.Name] = table

	table.changeEvents = append(table.changeEvents, events.NewCreateTable(
		table.Name(),
		[]byte(record.GetString("definition")),
	))

	return table, nil
}

func (manager *TableManager) deleteTable(name string) error {

	oldRecord, err := manager.catalog.Delete(manager.tableQuery(name))
	if err != nil {
		return fmt.Errorf("Catalog: couldn't delete table %s from catalog: %w", name, err)
	}

	if manager.loadedTables[name] == nil {
		manager.loadedTables[name] = manager.decodeTable(oldRecord)
	}

	table := manager.loadedTables[name]

	table.changeEvents = append(table.changeEvents, events.NewDeleteTable(name))

	return nil
}

func (manager *TableManager) changeEvents() []TableEvent {
	events := make([]TableEvent, 0)

	for _, table := range manager.loadedTables {
		tableEvents := table.ChangeEvents()
		if len(tableEvents) > 0 {
			events = append(events, tableEvents...)
		}
	}

	return events
}

func (manager *TableManager) applyChangeEvents(changeEvents []TableEvent) (err error) {
	previousRoot := manager.catalog.Root()
	snapshot := manager.pager.Snapshot()

	defer func() {
		if err != nil {
			manager.pager.Restore(snapshot)
			manager.catalog, _ = newTable(previousRoot, manager.pager, &catalogSchema) // In case of error we restore previous catalog state
			manager.loadedTables = make(map[string]*Table)                             //In case of error we discard all cached tables
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
			*events.UpdateDBVersion,
			*events.FreePages:
			// These events are not related to table schema or entries, so we can ignore them during replay
			continue

		default:
			return fmt.Errorf("Catalog: couldn't apply unknown event %s", event.Name())
		}
	}

	return nil
}

func (manager *TableManager) updateTables() error {
	for name, table := range manager.loadedTables {
		// We only need to update tables that were changed
		if len(table.changeEvents) > 0 {
			if err := manager.updateTable(name, table); err != nil {
				return fmt.Errorf("Catalog: couldn't write table %s: %w", name, err)
			}
		}
	}

	return nil
}

func (manager *TableManager) commit(headerData []byte) error {
	if err := manager.updateTables(); err != nil {
		return fmt.Errorf("TableManager: failed to write tables: %w", err)
	}
	if err := manager.pager.UpdatePage(HEADER_PAGE, headerData); err != nil {
		return fmt.Errorf("TableManager: failed to update header page: %w", err)
	}
	if err := manager.pager.SaveChanges(); err != nil {
		return fmt.Errorf("TableManager: failed to save changes: %w", err)
	}

	return nil
}

func (manager *TableManager) root() pager.PagePointer {
	return manager.catalog.Root()
}

func (manager *TableManager) totalPages() uint64 {
	return manager.pager.TotalPages()
}

func (manager *TableManager) releasedPages() []pager.PagePointer {
	return manager.pager.ReleasedPages()
}

func (manager *TableManager) reusablePages() []pager.PagePointer {
	return manager.pager.ReusablePages()
}

func (manager *TableManager) applyCreateTableEvent(event *events.CreateTable) error {
	var schema TableSchema
	if err := json.Unmarshal(event.Schema, &schema); err != nil {
		return fmt.Errorf("CreateTable Apply: couldn't parse schema: %w", err)
	}

	table, err := manager.table(event.TableName)
	if err != nil {
		return fmt.Errorf("CreateTable Replay: %w", err)
	}
	if table != nil {
		return fmt.Errorf("CreateTable Apply: couldn't create table %s because it already exists", event.TableName)
	}

	if _, err := manager.createTable(&schema); err != nil {
		return fmt.Errorf("CreateTable Apply: %w", err)
	}

	return nil
}

func (manager *TableManager) applyDeleteTableEvent(event *events.DeleteTable) error {
	table, err := manager.table(event.TableName)
	if err != nil {
		return fmt.Errorf("DeleteTable Apply: %w", err)
	}
	if table == nil {
		return fmt.Errorf("DeleteTable Apply: table %s not found", event.TableName)
	}

	if err := manager.deleteTable(event.TableName); err != nil {
		return fmt.Errorf("DeleteTable Apply: %w", err)
	}

	return nil
}

func (manager *TableManager) applyDeleteEntryEvent(event *events.DeleteEntry) error {
	table, err := manager.table(event.TableName)
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
	table, err := manager.table(event.TableName)
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
	table, err := manager.table(event.TableName)
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

func (manager *TableManager) tableQuery(name string) *vals.Object {
	return vals.NewObject().Set("name", vals.NewString(name))
}

func (manager *TableManager) decodeTable(record *vals.Object) *Table {
	definition := record.GetString("definition")
	root := record.GetUint64("root")

	schema := &TableSchema{}
	json.Unmarshal([]byte(definition), schema)

	table, _ := newTable(root, manager.pager, schema)
	return table
}

func (manager *TableManager) encodeTable(table *Table) *vals.Object {
	stringifiedSchema, _ := json.Marshal(table.schema)

	return vals.NewObject().
		Set("name", vals.NewString(table.schema.Name)).
		Set("definition", vals.NewString(string(stringifiedSchema))).
		Set("root", vals.NewInt(table.Root()))
}

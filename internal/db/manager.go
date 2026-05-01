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
	Name:             "@catalog",
	PrimaryIndex:     []string{"id"},
	SecondaryIndexes: [][]string{{"name", "state"}},
	IndexedColumns:   map[string]vals.ValueType{"id": vals.TYPE_UINT64, "name": vals.TYPE_STRING, "state": vals.TYPE_UINT32},
}

type ApplyResult struct {
	Root             pager.PagePointer
	DBVersion        DatabaseVersion
	LastCreatedTable TableID

	TotalPages    uint64
	ReleasedPages pager.PageList
	ReusablePages pager.PageList
}

type TableManager struct {
	catalog      *Table
	loadedTables map[TableID]*Table

	pager *pager.Pager
}

func newTableManager(root pager.PagePointer, pager *pager.Pager) *TableManager {
	catalog, _ := newTable(0, root, pager, &catalogSchema)

	return &TableManager{
		catalog:      catalog,
		loadedTables: make(map[TableID]*Table),

		pager: pager,
	}
}

func (manager *TableManager) table(name string) (*Table, error) {
	for _, table := range manager.loadedTables {
		if table.schema.Name == name {
			return table, nil
		}
	}

	records, err := manager.catalog.Find(manager.buildTableQueryByName(name))

	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't read table schema: %w", err)
	}

	if len(records) == 0 {
		return nil, nil
	}

	table := manager.decodeTable(records[0])
	manager.loadedTables[table.id] = table

	return table, nil
}

func (manager *TableManager) tableByID(id TableID) (*Table, error) {
	if table, ok := manager.loadedTables[id]; ok {
		return table, nil
	}

	records, err := manager.catalog.Find(manager.buildTableQueryByID(id))
	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't read table schema: %w", err)
	}

	if len(records) == 0 {
		return nil, nil
	}

	table := manager.decodeTable(records[0])
	manager.loadedTables[id] = table

	return table, nil
}

func (manager *TableManager) updateTable(table *Table) error {
	record := manager.encodeTable(table)

	oldRecord, err := manager.catalog.Update(record)
	if err != nil {
		return fmt.Errorf("Catalog: couldn't update 	table %s: %w", table.schema.Name, err)
	}

	manager.loadedTables[table.id] = table

	table.changeEvents = append(table.changeEvents, events.NewUpdateTable(
		uint64(table.id),
		[]byte(oldRecord.GetString("definition")),
		[]byte(record.GetString("definition")),
	))

	return nil
}

func (manager *TableManager) createTable(id TableID, schema *TableSchema) (*Table, error) {
	table, err := manager.table(schema.Name)
	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't check if table %s already exists: %w", schema.Name, err)
	}

	if table != nil {
		return nil, fmt.Errorf("Catalog: couldn't create table %s because it already exists", schema.Name)
	}

	table, err = newTable(id, pager.NULL_PAGE, manager.pager, schema)
	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't create table %s: %w", schema.Name, err)
	}

	record := manager.encodeTable(table)
	if err := manager.catalog.Insert(record); err != nil {
		return nil, fmt.Errorf("Catalog: couldn't create table %s: %w", schema.Name, err)
	}

	manager.loadedTables[table.id] = table

	table.changeEvents = append(table.changeEvents, events.NewCreateTable(
		record.GetUint64("id"),
		[]byte(record.GetString("definition")),
	))

	return table, nil
}

func (manager *TableManager) deleteTable(name string) error {
	res, err := manager.catalog.Patch(manager.buildTableQueryByName(name), vals.NewObject().Set("state", vals.NewUint32(uint32(TABLE_DROPPING))))
	if err != nil {
		return fmt.Errorf("Catalog: couldn't delete table %s from catalog: %w", name, err)
	}

	if len(res) == 0 {
		return nil
	}

	table := manager.decodeTable(res[0])
	delete(manager.loadedTables, table.id)

	table.changeEvents = append(table.changeEvents, events.NewDeleteTable(uint64(table.id)))

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

func (manager *TableManager) applyChangeEvents(changeEvents []TableEvent) (result *ApplyResult, err error) {
	result = &ApplyResult{}

	previousRoot := manager.catalog.Root()
	snapshot := manager.pager.Snapshot()

	defer func() {
		if err != nil {
			manager.pager.Restore(snapshot)
			manager.catalog, _ = newTable(0, previousRoot, manager.pager, &catalogSchema) // In case of error we restore previous catalog state
			manager.loadedTables = make(map[TableID]*Table)                               //In case of error we discard all cached tables
		}

		result.Root = manager.catalog.Root()
		result.TotalPages = manager.pager.TotalPages()
		result.ReleasedPages = manager.pager.ReleasedPages()
		result.ReusablePages = manager.pager.ReusablePages()
	}()

	for _, changeEvent := range changeEvents {
		switch event := changeEvent.(type) {
		case *events.CreateTable:
			if err = manager.applyCreateTableEvent(event); err != nil {
				return result, err
			}
			result.LastCreatedTable = TableID(event.TableID)

		case *events.DeleteTable:
			if err = manager.applyDeleteTableEvent(event); err != nil {
				return result, err
			}

		case *events.DeleteEntry:
			if err = manager.applyDeleteEntryEvent(event); err != nil {
				return result, err
			}

		case *events.UpdateEntry:
			if err = manager.applyUpdateEntryEvent(event); err != nil {
				return result, err
			}

		case *events.InsertEntry:
			if err = manager.applyInsertEntryEvent(event); err != nil {
				return result, err
			}

		case *events.UpdateDBVersion:
			result.DBVersion = DatabaseVersion(event.Version)

		case *events.StartTransaction,
			*events.CommitTransaction,
			*events.FreePages:
			// These events are not related to table schema or entries, so we can ignore them during replay
			continue

		default:
			return result, fmt.Errorf("Catalog: couldn't apply unknown event %s", event.Name())
		}
	}

	return result, manager.saveChanges()
}

func (manager *TableManager) saveChanges() error {
	// We only need to update tables that were changed
	for _, table := range manager.loadedTables {
		if err := manager.updateTable(table); err != nil {
			return fmt.Errorf("Catalog: couldn't write table %s: %w", table.schema.Name, err)
		}
	}

	return nil
}

func (manager *TableManager) commit(headerData []byte) error {
	if err := manager.pager.UpdatePage(HEADER_PAGE, headerData); err != nil {
		return fmt.Errorf("TableManager: failed to update header page: %w", err)
	}
	if err := manager.pager.SaveChanges(); err != nil {
		return fmt.Errorf("TableManager: failed to save changes: %w", err)
	}

	return nil
}

func (manager *TableManager) applyCreateTableEvent(event *events.CreateTable) error {
	schema := &TableSchema{}
	if err := json.Unmarshal(event.Schema, schema); err != nil {
		return fmt.Errorf("CreateTable Apply: couldn't parse schema: %w", err)
	}

	if _, err := manager.createTable(TableID(event.TableID), schema); err != nil {
		return fmt.Errorf("CreateTable Apply: %w", err)
	}

	return nil
}

func (manager *TableManager) applyDeleteTableEvent(event *events.DeleteTable) error {
	table, err := manager.tableByID(TableID(event.TableID))
	if err != nil {
		return fmt.Errorf("DeleteTable Apply: %w", err)
	}
	if table == nil {
		return fmt.Errorf("DeleteTable Apply: table with ID %d not found", event.TableID)
	}

	if err := manager.deleteTable(table.schema.Name); err != nil {
		return fmt.Errorf("DeleteTable Apply: %w", err)
	}

	return nil
}

func (manager *TableManager) applyDeleteEntryEvent(event *events.DeleteEntry) error {
	table, err := manager.tableByID(TableID(event.TableID))
	if err != nil {
		return fmt.Errorf("DeleteEntry Apply: %w", err)
	}
	if table == nil {
		return fmt.Errorf("DeleteEntry Apply: table with ID %d not found", event.TableID)
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
	table, err := manager.tableByID(TableID(event.TableID))
	if err != nil {
		return fmt.Errorf("UpdateEntry Apply: %w", err)
	}
	if table == nil {
		return fmt.Errorf("UpdateEntry Apply: table with ID %d not found", event.TableID)
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
	table, err := manager.tableByID(TableID(event.TableID))
	if err != nil {
		return fmt.Errorf("InsertEntry Apply: %w", err)
	}
	if table == nil {
		return fmt.Errorf("InsertEntry Apply: table with ID %d not found", event.TableID)
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

func (manager *TableManager) buildTableQueryByName(name string) *vals.Object {
	return vals.NewObject().Set("name", vals.NewString(name)).Set("state", vals.NewUint32(uint32(TABLE_ACTIVE)))
}

func (manager *TableManager) buildTableQueryByID(id TableID) *vals.Object {
	return vals.NewObject().Set("id", vals.NewUint64(uint64(id)))
}

func (manager *TableManager) decodeTable(record *vals.Object) *Table {
	id := TableID(record.GetUint64("id"))
	state := TableState(record.GetUint32("state"))
	definition := record.GetString("definition")
	root := record.GetUint64("root")

	schema := &TableSchema{}
	json.Unmarshal([]byte(definition), schema)

	table, _ := newTable(id, root, manager.pager, schema)
	table.state = state

	return table
}

func (manager *TableManager) encodeTable(table *Table) *vals.Object {
	stringifiedSchema, _ := json.Marshal(table.schema)

	return vals.NewObject().
		Set("id", vals.NewUint64(uint64(table.id))).
		Set("name", vals.NewString(table.schema.Name)).
		Set("state", vals.NewUint32(uint32(TABLE_ACTIVE))).
		Set("definition", vals.NewString(string(stringifiedSchema))).
		Set("root", vals.NewUint64(table.Root()))
}

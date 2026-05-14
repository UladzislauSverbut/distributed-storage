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
const CATALOG_TABLE_ID = TableID(0)

var catalogSchema = TableSchema{
	Name:             "@catalog",
	PrimaryIndex:     []string{"id"},
	SecondaryIndexes: [][]string{{"name", "state"}},
	IndexedColumns:   map[string]vals.ValueType{"id": vals.TYPE_UINT64, "name": vals.TYPE_STRING, "state": vals.TYPE_UINT32},
}

type ApplyResult struct {
	Root            pager.PagePointer
	DatabaseVersion DatabaseVersion
	TablesCount     uint64
	PagesCount      uint64

	ReleasedPages pager.PageList
	ReusablePages pager.PageList
}

type TableManagerState struct {
	Root    pager.PagePointer
	Version DatabaseVersion
}

type TableManager struct {
	state   TableManagerState
	tableID TableIDAllocator

	catalog      *Table
	loadedTables map[TableID]*Table

	pager *pager.Pager
}

func newTableManager(state TableManagerState, tableID TableIDAllocator, pager *pager.Pager) *TableManager {
	catalog, err := newTable(CATALOG_TABLE_ID, state.Root, pager, &catalogSchema)
	if err != nil {
		panic(fmt.Sprintf("newTableManager: couldn't initialize catalog: %v", err))
	}

	return &TableManager{
		state:   state,
		tableID: tableID,

		catalog:      catalog,
		loadedTables: make(map[TableID]*Table),

		pager: pager,
	}
}

func (manager *TableManager) Table(name string) (*Table, error) {
	for _, table := range manager.loadedTables {
		if table.schema.Name == name {
			return table, nil
		}
	}

	records, err := manager.catalog.Find(manager.buildTableQueryByName(name))
	if err != nil {
		return nil, fmt.Errorf("Table %s: couldn't read schema from catalog: %w", name, err)
	}

	if len(records) == 0 {
		return nil, nil
	}

	table, err := manager.decodeTable(records[0])
	if err != nil {
		return nil, fmt.Errorf("Table %s: couldn't decode schema from catalog: %w", name, err)
	}
	manager.loadedTables[table.id] = table

	return table, nil
}

func (manager *TableManager) TableByID(id TableID) (*Table, error) {
	if table, ok := manager.loadedTables[id]; ok {
		return table, nil
	}

	records, err := manager.catalog.Find(manager.buildTableQueryByID(id))
	if err != nil {
		return nil, fmt.Errorf("Table ID %d: couldn't read schema from catalog: %w", id, err)
	}

	if len(records) == 0 {
		return nil, nil
	}

	table, err := manager.decodeTable(records[0])
	if err != nil {
		return nil, fmt.Errorf("TableByID %d: couldn't decode schema from catalog: %w", id, err)
	}
	manager.loadedTables[id] = table

	return table, nil
}

func (manager *TableManager) UpdateTable(table *Table) error {
	record := manager.encodeTable(table)

	oldRecord, err := manager.catalog.Update(record)
	if err != nil {
		return fmt.Errorf("UpdateTable %q: couldn't update catalog entry: %w", table.schema.Name, err)
	}

	manager.loadedTables[table.id] = table

	table.changeEvents = append(table.changeEvents, events.NewUpdateTable(
		uint64(table.id),
		[]byte(oldRecord.GetString("definition")),
		[]byte(record.GetString("definition")),
	))

	return nil
}

func (manager *TableManager) CreateTable(schema *TableSchema) (*Table, error) {
	table, err := manager.Table(schema.Name)
	if err != nil {
		return nil, fmt.Errorf("CreateTable %q: couldn't check if table already exists: %w", schema.Name, err)
	}
	if table != nil {
		return nil, fmt.Errorf("CreateTable %q: table already exists", schema.Name)
	}

	table, err = newTable(manager.tableID(), pager.NULL_PAGE, manager.pager, schema)
	if err != nil {
		return nil, fmt.Errorf("CreateTable %q: couldn't initialize table: %w", schema.Name, err)
	}

	record := manager.encodeTable(table)
	if err := manager.catalog.Insert(record); err != nil {
		return nil, fmt.Errorf("CreateTable %q: couldn't insert into catalog: %w", schema.Name, err)
	}

	manager.loadedTables[table.id] = table

	table.changeEvents = append(table.changeEvents, events.NewCreateTable(
		record.GetUint64("id"),
		[]byte(record.GetString("definition")),
	))

	return table, nil
}

func (manager *TableManager) DeleteTable(name string) error {
	table, err := manager.Table(name)
	if err != nil {
		return fmt.Errorf("DeleteTable %q: couldn't check if table exists: %w", name, err)
	}
	if table == nil {
		return nil
	}

	table.state = TABLE_DROPPING

	if _, err := manager.catalog.Update(manager.encodeTable(table)); err != nil {
		return fmt.Errorf("DeleteTable %q: couldn't update catalog entry: %w", name, err)
	}

	delete(manager.loadedTables, table.id)

	table.changeEvents = append(table.changeEvents, events.NewDeleteTable(uint64(table.id)))

	return nil
}

func (manager *TableManager) ChangeEvents() []TableEvent {
	var events []TableEvent

	for _, table := range manager.loadedTables {
		tableEvents := table.ChangeEvents()
		if len(tableEvents) > 0 {
			events = append(events, tableEvents...)
		}
	}

	return events
}

func (manager *TableManager) ApplyChangeEvents(changeEvents []TableEvent) (res ApplyResult, err error) {
	root := manager.catalog.Root()
	snapshot := manager.pager.Snapshot()

	defer func() {
		if err != nil {
			manager.pager.Restore(snapshot)
			manager.catalog, _ = newTable(CATALOG_TABLE_ID, root, manager.pager, &catalogSchema) // In case of error we restore previous catalog state
			manager.loadedTables = make(map[TableID]*Table)
		}

		res.Root = manager.catalog.Root()
		res.ReleasedPages = manager.pager.ReleasedPages()
		res.ReusablePages = manager.pager.ReusablePages()
		res.PagesCount = manager.pager.PagesCount()
	}()

	for _, changeEvent := range changeEvents {
		switch event := changeEvent.(type) {
		case *events.UpdateDBVersion:
			res.DatabaseVersion = DatabaseVersion(event.Version)

		case *events.CreateTable:
			if err = manager.applyCreateTableEvent(event); err != nil {
				return
			}
			res.TablesCount = event.TableID + 1

		case *events.DeleteTable:
			if err = manager.applyDeleteTableEvent(event); err != nil {
				return
			}

		case *events.DeleteEntry:
			if err = manager.applyDeleteEntryEvent(event); err != nil {
				return
			}

		case *events.UpdateEntry:
			if err = manager.applyUpdateEntryEvent(event); err != nil {
				return
			}

		case *events.InsertEntry:
			if err = manager.applyInsertEntryEvent(event); err != nil {
				return
			}

		case *events.StartTransaction,
			*events.CommitTransaction,
			*events.FreePages:
			// These events are not related to table schema or entries, so we can ignore them during replay

		default:
			err = fmt.Errorf("ApplyChangeEvents: unknown event %q", event.Name())
			return
		}
	}

	err = manager.saveChanges()

	return
}

func (manager *TableManager) Commit(headerData []byte) error {
	if err := manager.pager.UpdatePage(HEADER_PAGE, headerData); err != nil {
		return fmt.Errorf("Commit: couldn't update header page: %w", err)
	}
	if err := manager.pager.SaveChanges(); err != nil {
		return fmt.Errorf("Commit: couldn't save changes: %w", err)
	}

	return nil
}

func (manager *TableManager) saveChanges() error {
	for _, table := range manager.loadedTables {
		if err := manager.UpdateTable(table); err != nil {
			return fmt.Errorf("saveChanges: %w", err)
		}
	}

	return nil
}

func (manager *TableManager) applyCreateTableEvent(event *events.CreateTable) error {
	schema := &TableSchema{}
	if err := json.Unmarshal(event.Schema, schema); err != nil {
		return fmt.Errorf("CreateTable Apply: couldn't parse schema: %w", err)
	}

	existing, err := manager.TableByID(TableID(event.TableID))
	if err != nil {
		return fmt.Errorf("CreateTable Apply: couldn't check if table %q already exists: %w", schema.Name, err)
	}
	if existing != nil {
		return fmt.Errorf("CreateTable Apply: table %q already exists", schema.Name)
	}

	table, err := newTable(TableID(event.TableID), pager.NULL_PAGE, manager.pager, schema)
	if err != nil {
		return fmt.Errorf("CreateTable Apply: couldn't create table %q: %w", schema.Name, err)
	}

	if err := manager.catalog.Insert(manager.encodeTable(table)); err != nil {
		return fmt.Errorf("CreateTable Apply: couldn't insert table %q into catalog: %w", schema.Name, err)
	}

	manager.loadedTables[table.id] = table

	return nil
}

func (manager *TableManager) applyDeleteTableEvent(event *events.DeleteTable) error {
	table, err := manager.TableByID(TableID(event.TableID))
	if err != nil {
		return err
	}
	if table == nil {
		return nil
	}

	if err := manager.DeleteTable(table.schema.Name); err != nil {
		return fmt.Errorf("DeleteTable Apply: %w", err)
	}

	return nil
}

func (manager *TableManager) applyDeleteEntryEvent(event *events.DeleteEntry) error {
	table, err := manager.TableByID(TableID(event.TableID))
	if err != nil {
		return err
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
	table, err := manager.TableByID(TableID(event.TableID))
	if err != nil {
		return err
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
	table, err := manager.TableByID(TableID(event.TableID))
	if err != nil {
		return err
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

func (manager *TableManager) decodeTable(record *vals.Object) (*Table, error) {
	id := TableID(record.GetUint64("id"))
	state := TableState(record.GetUint32("state"))
	definition := record.GetString("definition")
	root := record.GetUint64("root")

	schema := &TableSchema{}
	if err := json.Unmarshal([]byte(definition), schema); err != nil {
		return nil, fmt.Errorf("decodeTable: couldn't parse schema for table ID %d: %w", id, err)
	}

	table, err := newTable(id, root, manager.pager, schema)
	if err != nil {
		return nil, fmt.Errorf("decodeTable: couldn't initialize table ID %d: %w", id, err)
	}
	table.state = state

	return table, nil
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

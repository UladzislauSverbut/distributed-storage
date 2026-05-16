package db

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/vals"
	"testing"
)

func newTestManager(t *testing.T) *TableManager {
	t.Helper()
	p := newTestPager()
	state := TableManagerState{Root: pager.NULL_PAGE, Version: 1}
	nextID := uint64(1)
	return newTableManager(state, func() TableID {
		nextID++
		return TableID(nextID)
	}, p)
}

func TestNewTableManager_CreatesCatalog(t *testing.T) {
	m := newTestManager(t)
	if m.catalog == nil {
		t.Error("expected catalog to be initialized")
	}
}

func TestTableManager_Table_Nonexistent_ReturnsNil(t *testing.T) {
	m := newTestManager(t)
	table, err := m.Table("nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if table != nil {
		t.Error("expected nil for nonexistent table")
	}
}

func TestTableManager_CreateTable_Success(t *testing.T) {
	m := newTestManager(t)
	schema := &TableSchema{
		Name:         "orders",
		PrimaryIndex: []string{"id"},
		IndexedColumns: map[string]vals.ValueType{
			"id": vals.TYPE_UINT64,
		},
	}
	table, err := m.CreateTable(schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	if table == nil {
		t.Fatal("expected non-nil table")
	}
	if table.schema.Name != "orders" {
		t.Errorf("expected table name 'orders', got %q", table.schema.Name)
	}
}

func TestTableManager_CreateTable_DuplicateName_ReturnsError(t *testing.T) {
	m := newTestManager(t)
	schema := &TableSchema{
		Name:         "users",
		PrimaryIndex: []string{"id"},
		IndexedColumns: map[string]vals.ValueType{
			"id": vals.TYPE_UINT64,
		},
	}
	if _, err := m.CreateTable(schema); err != nil {
		t.Fatalf("first CreateTable failed: %v", err)
	}
	if _, err := m.CreateTable(schema); err == nil {
		t.Error("expected error when creating table with duplicate name")
	}
}

func TestTableManager_Table_FindByName_AfterCacheClear(t *testing.T) {
	m := newTestManager(t)
	schema := &TableSchema{
		Name:         "products",
		PrimaryIndex: []string{"id"},
		IndexedColumns: map[string]vals.ValueType{
			"id": vals.TYPE_UINT64,
		},
	}
	if _, err := m.CreateTable(schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	// Clear in-memory cache to force catalog lookup
	m.loadedTables = make(map[TableID]*Table)

	table, err := m.Table("products")
	if err != nil {
		t.Fatalf("Table failed: %v", err)
	}
	if table == nil {
		t.Fatal("expected non-nil table after catalog lookup")
	}
	if table.schema.Name != "products" {
		t.Errorf("expected 'products', got %q", table.schema.Name)
	}
}

func TestTableManager_DropTable_Existing(t *testing.T) {
	m := newTestManager(t)
	schema := &TableSchema{
		Name:         "temp",
		PrimaryIndex: []string{"id"},
		IndexedColumns: map[string]vals.ValueType{
			"id": vals.TYPE_UINT64,
		},
	}
	if _, err := m.CreateTable(schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	if err := m.DropTable("temp"); err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}
	if len(m.loadedTables) != 0 {
		t.Errorf("expected 0 loaded tables after drop, got %d", len(m.loadedTables))
	}
}

func TestTableManager_DropTable_Nonexistent_NoError(t *testing.T) {
	m := newTestManager(t)
	if err := m.DropTable("ghost"); err != nil {
		t.Errorf("unexpected error dropping nonexistent table: %v", err)
	}
}

func TestTableManager_ChangeEvents_EmptyInitially(t *testing.T) {
	m := newTestManager(t)
	if events := m.ChangeEvents(); len(events) != 0 {
		t.Errorf("expected 0 events on fresh manager, got %d", len(events))
	}
}

func TestTableManager_ChangeEvents_AfterCreateTable(t *testing.T) {
	m := newTestManager(t)
	schema := &TableSchema{
		Name:         "events_test",
		PrimaryIndex: []string{"id"},
		IndexedColumns: map[string]vals.ValueType{
			"id": vals.TYPE_UINT64,
		},
	}
	if _, err := m.CreateTable(schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	if events := m.ChangeEvents(); len(events) == 0 {
		t.Error("expected at least one change event after CreateTable")
	}
}

func TestTableManager_TableByID_Nonexistent(t *testing.T) {
	m := newTestManager(t)
	table, err := m.TableByID(TableID(999))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if table != nil {
		t.Error("expected nil for nonexistent table ID")
	}
}

func TestTableManager_UpdateTable(t *testing.T) {
	m := newTestManager(t)
	schema := &TableSchema{
		Name:         "updatable",
		PrimaryIndex: []string{"id"},
		IndexedColumns: map[string]vals.ValueType{
			"id": vals.TYPE_UINT64,
		},
	}
	table, err := m.CreateTable(schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	// UpdateTable should not return an error for a valid table
	if err := m.UpdateTable(table); err != nil {
		t.Errorf("UpdateTable failed: %v", err)
	}
}

package db

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	"distributed-storage/internal/vals"
	"testing"
)

const testPageSize = 16 * 1024

func newTestPager() *pager.Pager {
	storage := store.NewMemoryStorage(testPageSize * 100)
	return pager.NewPager(storage, 1, testPageSize) // Start with 1 page, because one page is always reserved for the table header
}

func basicSchema() *TableSchema {
	return &TableSchema{
		Name:         "users",
		PrimaryIndex: []string{"id"},
		IndexedColumns: map[string]vals.ValueType{
			"id": vals.TYPE_UINT64,
		},
	}
}

func schemaWithSecondaryIndex() *TableSchema {
	return &TableSchema{
		Name:             "users",
		PrimaryIndex:     []string{"id"},
		SecondaryIndexes: [][]string{{"email"}},
		IndexedColumns: map[string]vals.ValueType{
			"id":    vals.TYPE_UINT64,
			"email": vals.TYPE_STRING,
		},
	}
}

func newTestTable(t *testing.T) *Table {
	t.Helper()
	table, err := newTable(TableID(1), pager.NULL_PAGE, newTestPager(), basicSchema())
	if err != nil {
		t.Fatalf("newTable: %v", err)
	}
	return table
}

func newTableWithSecondaryIndex(t *testing.T) *Table {
	t.Helper()
	table, err := newTable(TableID(2), pager.NULL_PAGE, newTestPager(), schemaWithSecondaryIndex())
	if err != nil {
		t.Fatalf("newTable: %v", err)
	}
	return table
}

func userRecord(id uint64, name string) *vals.Object {
	return vals.NewObject().
		Set("id", vals.NewUint64(id)).
		Set("name", vals.NewString(name))
}

func userRecordWithEmail(id uint64, name, email string) *vals.Object {
	return vals.NewObject().
		Set("id", vals.NewUint64(id)).
		Set("name", vals.NewString(name)).
		Set("email", vals.NewString(email))
}

// --- newTable ---

func TestNewTable_ValidSchema(t *testing.T) {
	_, err := newTable(TableID(1), pager.NULL_PAGE, newTestPager(), basicSchema())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewTable_MissingName(t *testing.T) {
	schema := &TableSchema{
		PrimaryIndex:   []string{"id"},
		IndexedColumns: map[string]vals.ValueType{"id": vals.TYPE_UINT64},
	}
	_, err := newTable(TableID(1), pager.NULL_PAGE, newTestPager(), schema)
	if err == nil {
		t.Fatal("expected error for missing schema name")
	}
}

func TestNewTable_MissingPrimaryIndex(t *testing.T) {
	schema := &TableSchema{
		Name:           "users",
		IndexedColumns: map[string]vals.ValueType{},
	}
	_, err := newTable(TableID(1), pager.NULL_PAGE, newTestPager(), schema)
	if err == nil {
		t.Fatal("expected error for missing primary index")
	}
}

// --- Accessors ---

func TestTable_ID(t *testing.T) {
	table := newTestTable(t)
	if table.ID() != TableID(1) {
		t.Errorf("expected ID 1, got %v", table.ID())
	}
}

func TestTable_Name(t *testing.T) {
	table := newTestTable(t)
	if table.Name() != "users" {
		t.Errorf("expected name 'users', got %q", table.Name())
	}
}

func TestTable_Schema(t *testing.T) {
	table := newTestTable(t)
	if table.Schema() == nil {
		t.Fatal("expected non-nil schema")
	}
	if table.Schema().Name != "users" {
		t.Errorf("unexpected schema name %q", table.Schema().Name)
	}
}

func TestTable_Root_NullBeforeInsert(t *testing.T) {
	table := newTestTable(t)
	if table.Root() != pager.NULL_PAGE {
		t.Errorf("expected NULL_PAGE root before any insert, got %v", table.Root())
	}
}

func TestTable_Root_NonNullAfterInsert(t *testing.T) {
	table := newTestTable(t)
	if err := table.Insert(userRecord(1, "Alice")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if table.Root() == pager.NULL_PAGE {
		t.Error("expected non-null root after insert")
	}
}

// --- Insert ---

func TestTable_Insert(t *testing.T) {
	table := newTestTable(t)
	if err := table.Insert(userRecord(1, "Alice")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
}

func TestTable_Insert_Duplicate(t *testing.T) {
	table := newTestTable(t)
	if err := table.Insert(userRecord(1, "Alice")); err != nil {
		t.Fatalf("first Insert: %v", err)
	}
	if err := table.Insert(userRecord(1, "Bob")); err == nil {
		t.Fatal("expected error on duplicate insert")
	}
}

func TestTable_Insert_MissingPrimaryKey(t *testing.T) {
	table := newTestTable(t)
	noID := vals.NewObject().Set("name", vals.NewString("Alice"))
	if err := table.Insert(noID); err == nil {
		t.Fatal("expected error on insert without primary key")
	}
}

// --- Get ---

func TestTable_Get_Existing(t *testing.T) {
	table := newTestTable(t)
	if err := table.Insert(userRecord(1, "Alice")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, err := table.Get(vals.NewObject().Set("id", vals.NewUint64(1)))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected record, got nil")
	}
	if got.GetUint64("id") != 1 {
		t.Errorf("expected id=1, got %v", got.GetUint64("id"))
	}
	if got.GetString("name") != "Alice" {
		t.Errorf("expected name='Alice', got %q", got.GetString("name"))
	}
}

func TestTable_Get_NonExisting(t *testing.T) {
	table := newTestTable(t)

	got, err := table.Get(vals.NewObject().Set("id", vals.NewUint64(99)))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for non-existing record, got %v", got)
	}
}

func TestTable_Get_MissingPrimaryKey(t *testing.T) {
	table := newTestTable(t)

	_, err := table.Get(vals.NewObject().Set("name", vals.NewString("Alice")))
	if err == nil {
		t.Fatal("expected error when primary key is missing from query")
	}
}

// --- GetAll ---

func TestTable_GetAll_EmptyTable(t *testing.T) {
	table := newTestTable(t)
	records := table.GetAll()
	if len(records) != 0 {
		t.Errorf("expected 0 records, got %d", len(records))
	}
}

func TestTable_GetAll(t *testing.T) {
	table := newTestTable(t)
	for i := uint64(1); i <= 3; i++ {
		if err := table.Insert(userRecord(i, "user")); err != nil {
			t.Fatalf("Insert id=%d: %v", i, err)
		}
	}

	records := table.GetAll()
	if len(records) != 3 {
		t.Errorf("expected 3 records, got %d", len(records))
	}
}

// --- Find ---

func TestTable_Find_ByPrimaryKey(t *testing.T) {
	table := newTestTable(t)
	if err := table.Insert(userRecord(1, "Alice")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := table.Insert(userRecord(2, "Bob")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	results, err := table.Find(vals.NewObject().Set("id", vals.NewUint64(1)))
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].GetUint64("id") != 1 {
		t.Errorf("expected id=1, got %v", results[0].GetUint64("id"))
	}
}

func TestTable_Find_NoMatch(t *testing.T) {
	table := newTestTable(t)
	if err := table.Insert(userRecord(1, "Alice")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	results, err := table.Find(vals.NewObject().Set("id", vals.NewUint64(99)))
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestTable_Find_BySecondaryIndex(t *testing.T) {
	table := newTableWithSecondaryIndex(t)
	if err := table.Insert(userRecordWithEmail(1, "Alice", "alice@example.com")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := table.Insert(userRecordWithEmail(2, "Bob", "bob@example.com")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	results, err := table.Find(vals.NewObject().Set("email", vals.NewString("alice@example.com")))
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].GetUint64("id") != 1 {
		t.Errorf("expected id=1, got %v", results[0].GetUint64("id"))
	}
}

// --- Delete ---

func TestTable_Delete_Existing(t *testing.T) {
	table := newTestTable(t)
	if err := table.Insert(userRecord(1, "Alice")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := table.Insert(userRecord(2, "Bob")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	old, err := table.Delete(vals.NewObject().Set("id", vals.NewUint64(1)))
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if old == nil {
		t.Fatal("expected old record on delete, got nil")
	}
	if old.GetUint64("id") != 1 {
		t.Errorf("expected deleted record id=1, got %v", old.GetUint64("id"))
	}

	// Verify the remaining record is still accessible.
	got, err := table.Get(vals.NewObject().Set("id", vals.NewUint64(2)))
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if got == nil || got.GetUint64("id") != 2 {
		t.Error("expected record id=2 to still exist after deleting id=1")
	}
}

func TestTable_Delete_LastRecord(t *testing.T) {
	table := newTestTable(t)
	if err := table.Insert(userRecord(1, "Alice")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if _, err := table.Delete(vals.NewObject().Set("id", vals.NewUint64(1))); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, err := table.Get(vals.NewObject().Set("id", vals.NewUint64(1)))
	if err != nil {
		t.Fatalf("Get after last delete: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil after deleting last record, got %v", got)
	}
}

func TestTable_Delete_NonExisting(t *testing.T) {
	table := newTestTable(t)

	old, err := table.Delete(vals.NewObject().Set("id", vals.NewUint64(99)))
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if old != nil {
		t.Errorf("expected nil for non-existing record, got %v", old)
	}
}

func TestTable_Delete_MissingPrimaryKey(t *testing.T) {
	table := newTestTable(t)

	_, err := table.Delete(vals.NewObject().Set("name", vals.NewString("Alice")))
	if err == nil {
		t.Fatal("expected error when primary key is missing from record")
	}
}

// --- DeleteMany ---

func TestTable_DeleteMany(t *testing.T) {
	table := newTableWithSecondaryIndex(t)
	if err := table.Insert(userRecordWithEmail(1, "Alice", "shared@example.com")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := table.Insert(userRecordWithEmail(2, "Bob", "other@example.com")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := table.Insert(userRecordWithEmail(3, "Eve", "shared@example.com")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	deleted, err := table.DeleteMany(vals.NewObject().Set("email", vals.NewString("shared@example.com")))
	if err != nil {
		t.Fatalf("DeleteMany: %v", err)
	}
	if len(deleted) != 2 {
		t.Errorf("expected 2 deleted records, got %d", len(deleted))
	}

	remaining := table.GetAll()
	if len(remaining) != 1 {
		t.Errorf("expected 1 remaining record, got %d", len(remaining))
	}
	if remaining[0].GetUint64("id") != 2 {
		t.Errorf("expected remaining record id=2, got %v", remaining[0].GetUint64("id"))
	}
}

// --- Update ---

func TestTable_Update_Existing(t *testing.T) {
	table := newTestTable(t)
	if err := table.Insert(userRecord(1, "Alice")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	update := vals.NewObject().Set("id", vals.NewUint64(1)).Set("name", vals.NewString("Alicia"))
	old, err := table.Update(update)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if old == nil {
		t.Fatal("expected old record, got nil")
	}
	if old.GetString("name") != "Alice" {
		t.Errorf("expected old name='Alice', got %q", old.GetString("name"))
	}

	got, err := table.Get(vals.NewObject().Set("id", vals.NewUint64(1)))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.GetString("name") != "Alicia" {
		t.Errorf("expected updated name='Alicia', got %q", got.GetString("name"))
	}
}

func TestTable_Update_NonExisting(t *testing.T) {
	table := newTestTable(t)

	_, err := table.Update(userRecord(99, "Ghost"))
	if err == nil {
		t.Fatal("expected error updating non-existing record")
	}
}

func TestTable_Update_MissingPrimaryKey(t *testing.T) {
	table := newTestTable(t)

	_, err := table.Update(vals.NewObject().Set("name", vals.NewString("Alice")))
	if err == nil {
		t.Fatal("expected error when primary key is missing")
	}
}

// --- Upsert ---

func TestTable_Upsert_NewRecord(t *testing.T) {
	table := newTestTable(t)

	old, err := table.Upsert(userRecord(1, "Alice"))
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if old != nil {
		t.Errorf("expected nil old value on insert-upsert, got %v", old)
	}

	got, err := table.Get(vals.NewObject().Set("id", vals.NewUint64(1)))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil || got.GetString("name") != "Alice" {
		t.Errorf("expected inserted record with name='Alice', got %v", got)
	}
}

func TestTable_Upsert_ExistingRecord(t *testing.T) {
	table := newTestTable(t)
	if err := table.Insert(userRecord(1, "Alice")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	old, err := table.Upsert(userRecord(1, "Alicia"))
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if old == nil {
		t.Fatal("expected old record on update-upsert, got nil")
	}
	if old.GetString("name") != "Alice" {
		t.Errorf("expected old name='Alice', got %q", old.GetString("name"))
	}

	got, err := table.Get(vals.NewObject().Set("id", vals.NewUint64(1)))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.GetString("name") != "Alicia" {
		t.Errorf("expected updated name='Alicia', got %q", got.GetString("name"))
	}
}

func TestTable_Upsert_MissingPrimaryKey(t *testing.T) {
	table := newTestTable(t)

	_, err := table.Upsert(vals.NewObject().Set("name", vals.NewString("Alice")))
	if err == nil {
		t.Fatal("expected error when primary key is missing")
	}
}

// --- UpdateMany ---

func TestTable_UpdateMany(t *testing.T) {
	table := newTableWithSecondaryIndex(t)
	if err := table.Insert(userRecordWithEmail(1, "Alice", "group@example.com")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := table.Insert(userRecordWithEmail(2, "Bob", "group@example.com")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := table.Insert(userRecordWithEmail(3, "Eve", "other@example.com")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	query := vals.NewObject().Set("email", vals.NewString("group@example.com"))
	update := vals.NewObject().Set("name", vals.NewString("Updated"))
	old, err := table.UpdateMany(query, update)
	if err != nil {
		t.Fatalf("UpdateMany: %v", err)
	}
	if len(old) != 2 {
		t.Errorf("expected 2 old records, got %d", len(old))
	}

	results, err := table.Find(query)
	if err != nil {
		t.Fatalf("Find after UpdateMany: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results after UpdateMany, got %d", len(results))
	}
	for _, r := range results {
		if r.GetString("name") != "Updated" {
			t.Errorf("expected name='Updated', got %q", r.GetString("name"))
		}
	}
}

// --- ChangeEvents ---

func TestTable_ChangeEvents_AfterInsert(t *testing.T) {
	table := newTestTable(t)
	if err := table.Insert(userRecord(1, "Alice")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	if len(table.ChangeEvents()) == 0 {
		t.Fatal("expected change events after insert")
	}
}

func TestTable_ChangeEvents_AfterDelete(t *testing.T) {
	table := newTestTable(t)
	if err := table.Insert(userRecord(1, "Alice")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	beforeDelete := len(table.ChangeEvents())

	if _, err := table.Delete(vals.NewObject().Set("id", vals.NewUint64(1))); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if len(table.ChangeEvents()) <= beforeDelete {
		t.Error("expected additional change event after delete")
	}
}

func TestTable_ChangeEvents_AfterUpdate(t *testing.T) {
	table := newTestTable(t)
	if err := table.Insert(userRecord(1, "Alice")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	beforeUpdate := len(table.ChangeEvents())

	update := vals.NewObject().Set("id", vals.NewUint64(1)).Set("name", vals.NewString("Alicia"))
	if _, err := table.Update(update); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if len(table.ChangeEvents()) <= beforeUpdate {
		t.Error("expected additional change event after update")
	}
}

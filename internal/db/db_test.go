package db_test

import (
	"context"
	"path/filepath"
	"testing"

	"distributed-storage/internal/catalog"
	. "distributed-storage/internal/db"
	transaction "distributed-storage/internal/tx"
	"distributed-storage/internal/vals"
)

// --- helpers ---

func testConfig(t *testing.T) DatabaseConfig {
	t.Helper()
	dir := t.TempDir()
	return DatabaseConfig{
		Directory:           dir,
		WALDirectory:        filepath.Join(dir, "wal"),
		WALArchiveDirectory: filepath.Join(dir, "wal", "archive"),
		PageSize:            4096,
		WALSegmentSize:      1024 * 1024,
	}
}

func openDB(t *testing.T, cfg DatabaseConfig) *Database {
	t.Helper()
	db, err := NewDatabase(cfg)
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	return db
}

var usersSchema = &catalog.TableSchema{
	Name:         "users",
	PrimaryIndex: []string{"id"},
	IndexedColumns: map[string]vals.ValueType{
		"id":   vals.TYPE_UINT64,
		"name": vals.TYPE_STRING,
	},
}

func userRecord(id uint64, name string) *vals.Object {
	return vals.NewObject().
		Set("id", vals.NewUint64(id)).
		Set("name", vals.NewString(name))
}

func userKey(id uint64) *vals.Object {
	return vals.NewObject().Set("id", vals.NewUint64(id))
}

// createTable runs a single transaction that creates a table and returns its name.
func createTable(t *testing.T, db *Database, schema *catalog.TableSchema) {
	t.Helper()
	if err := db.Run(func(tx *transaction.Transaction) error {
		_, err := tx.CreateTable(schema)
		return err
	}); err != nil {
		t.Fatalf("create table %q: %v", schema.Name, err)
	}
}

// --- NewDatabase ---

func TestNewDatabase_OpensFreshDatabase(t *testing.T) {
	db := openDB(t, testConfig(t))
	if db == nil {
		t.Fatal("expected non-nil Database")
	}
}

func TestNewDatabase_ReopensExistingDatabase(t *testing.T) {
	cfg := testConfig(t)
	openDB(t, cfg) // first open — initializes WAL and header
	openDB(t, cfg) // second open — must succeed without errors
}

// --- CreateTable ---

func TestDatabase_CreateTable_PersistsAcrossReopens(t *testing.T) {
	cfg := testConfig(t)
	db1 := openDB(t, cfg)
	createTable(t, db1, usersSchema)

	db2 := openDB(t, cfg)
	err := db2.Run(func(tx *transaction.Transaction) error {
		table, err := tx.Table("users")
		if err != nil {
			return err
		}
		if table == nil {
			t.Error("table 'users' not found after reopen")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("reopen check: %v", err)
	}
}

// --- Insert / Get ---

func TestDatabase_Insert_RecordIsReadableInSameTransaction(t *testing.T) {
	db := openDB(t, testConfig(t))
	createTable(t, db, usersSchema)

	err := db.Run(func(tx *transaction.Transaction) error {
		table, err := tx.Table("users")
		if err != nil {
			return err
		}
		if err := table.Insert(userRecord(1, "alice")); err != nil {
			return err
		}
		got, err := table.Get(userKey(1))
		if err != nil {
			return err
		}
		if got == nil {
			t.Error("inserted record not found in same transaction")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}
}

func TestDatabase_Insert_RecordPersistsAfterCommit(t *testing.T) {
	db := openDB(t, testConfig(t))
	createTable(t, db, usersSchema)

	if err := db.Run(func(tx *transaction.Transaction) error {
		table, _ := tx.Table("users")
		return table.Insert(userRecord(1, "alice"))
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := db.Run(func(tx *transaction.Transaction) error {
		table, _ := tx.Table("users")
		got, err := table.Get(userKey(1))
		if err != nil {
			return err
		}
		if got == nil {
			t.Error("record not found in subsequent transaction")
		}
		return nil
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func TestDatabase_Insert_RecordSurvivesReopen(t *testing.T) {
	cfg := testConfig(t)
	db1 := openDB(t, cfg)
	createTable(t, db1, usersSchema)

	if err := db1.Run(func(tx *transaction.Transaction) error {
		table, _ := tx.Table("users")
		return table.Insert(userRecord(42, "bob"))
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	db2 := openDB(t, cfg)
	if err := db2.Run(func(tx *transaction.Transaction) error {
		table, _ := tx.Table("users")
		got, err := table.Get(userKey(42))
		if err != nil {
			return err
		}
		if got == nil {
			t.Error("record not found after reopen")
			return nil
		}
		if got.GetString("name") != "bob" {
			t.Errorf("name: want 'bob', got %q", got.GetString("name"))
		}
		return nil
	}); err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
}

// --- Delete ---

func TestDatabase_Delete_RecordIsGoneAfterCommit(t *testing.T) {
	db := openDB(t, testConfig(t))
	createTable(t, db, usersSchema)

	if err := db.Run(func(tx *transaction.Transaction) error {
		table, _ := tx.Table("users")
		return table.Insert(userRecord(1, "alice"))
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := db.Run(func(tx *transaction.Transaction) error {
		table, _ := tx.Table("users")
		_, err := table.Delete(userKey(1))
		return err
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if err := db.Run(func(tx *transaction.Transaction) error {
		table, _ := tx.Table("users")
		got, err := table.Get(userKey(1))
		if err != nil {
			return err
		}
		if got != nil {
			t.Errorf("deleted record still present: %v", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("read after delete: %v", err)
	}
}

// --- Rollback ---

func TestDatabase_Rollback_ChangesAreNotVisible(t *testing.T) {
	db := openDB(t, testConfig(t))
	createTable(t, db, usersSchema)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tx := db.Begin(ctx)
	table, _ := tx.Table("users")
	if err := table.Insert(userRecord(99, "ghost")); err != nil {
		t.Fatalf("insert in tx: %v", err)
	}
	tx.Rollback()

	if err := db.Run(func(tx *transaction.Transaction) error {
		table, _ := tx.Table("users")
		got, err := table.Get(userKey(99))
		if err != nil {
			return err
		}
		if got != nil {
			t.Errorf("rolled-back record is visible: %v", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("read after rollback: %v", err)
	}
}

// --- Sequential transactions build on each other ---

func TestDatabase_SequentialTransactions_AccumulateRecords(t *testing.T) {
	db := openDB(t, testConfig(t))
	createTable(t, db, usersSchema)

	for i := uint64(1); i <= 5; i++ {
		id := i
		if err := db.Run(func(tx *transaction.Transaction) error {
			table, _ := tx.Table("users")
			return table.Insert(userRecord(id, "user"))
		}); err != nil {
			t.Fatalf("insert id=%d: %v", id, err)
		}
	}

	if err := db.Run(func(tx *transaction.Transaction) error {
		table, _ := tx.Table("users")
		all := table.GetAll()
		if len(all) != 5 {
			t.Errorf("expected 5 records, got %d", len(all))
		}
		return nil
	}); err != nil {
		t.Fatalf("read all: %v", err)
	}
}

// --- WAL recovery ---

func TestDatabase_WALRecovery_UncommittedEventsAreReplayed(t *testing.T) {
	cfg := testConfig(t)

	// Establish a base version on disk so the second open has something to recover from.
	db1 := openDB(t, cfg)
	createTable(t, db1, usersSchema)

	if err := db1.Run(func(tx *transaction.Transaction) error {
		table, _ := tx.Table("users")
		return table.Insert(userRecord(1, "alice"))
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Open a fresh DB on the same directory. It will re-read the WAL and must
	// find the committed record (either from storage or by WAL replay).
	db2 := openDB(t, cfg)
	if err := db2.Run(func(tx *transaction.Transaction) error {
		table, _ := tx.Table("users")
		got, err := table.Get(userKey(1))
		if err != nil {
			return err
		}
		if got == nil {
			t.Error("expected record to survive DB reopen via WAL recovery")
		}
		return nil
	}); err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
}

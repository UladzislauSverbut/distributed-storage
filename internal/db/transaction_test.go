package db

import (
	"context"
	"distributed-storage/internal/pager"
	"distributed-storage/internal/vals"
	"testing"
)

func newTestTransaction(t *testing.T) *Transaction {
	t.Helper()
	p := newTestPager()
	state := TableManagerState{Root: pager.NULL_PAGE, Version: 1}
	nextID := uint64(1)
	manager := newTableManager(state, func() TableID {
		nextID++
		return TableID(nextID)
	}, p)
	queue := make(chan TransactionCommit, 1)
	tx := &Transaction{
		manager:     manager,
		commitQueue: queue,
		ctx:         context.Background(),
	}
	tx.state.Store(int32(TRANSACTION_PROCESSING))
	return tx
}

func TestTransaction_IsActive_WhenProcessing(t *testing.T) {
	tx := newTestTransaction(t)
	if !tx.IsActive() {
		t.Error("expected IsActive=true in PROCESSING state")
	}
}

func TestTransaction_IsActive_WhenCommitting(t *testing.T) {
	tx := newTestTransaction(t)
	tx.state.Store(int32(TRANSACTION_COMMITTING))
	if !tx.IsActive() {
		t.Error("expected IsActive=true in COMMITTING state")
	}
}

func TestTransaction_IsActive_WhenCommitted(t *testing.T) {
	tx := newTestTransaction(t)
	tx.state.Store(int32(TRANSACTION_COMMITTED))
	if tx.IsActive() {
		t.Error("expected IsActive=false in COMMITTED state")
	}
}

func TestTransaction_IsActive_WhenAborted(t *testing.T) {
	tx := newTestTransaction(t)
	tx.state.Store(int32(TRANSACTION_ABORTED))
	if tx.IsActive() {
		t.Error("expected IsActive=false in ABORTED state")
	}
}

func TestTransaction_Rollback_SetsAborted(t *testing.T) {
	tx := newTestTransaction(t)
	// Rollback only transitions from COMMITTING → ABORTED
	tx.state.Store(int32(TRANSACTION_COMMITTING))
	tx.Rollback()
	state := TransactionState(tx.state.Load())
	if state != TRANSACTION_ABORTED {
		t.Errorf("expected ABORTED state after Rollback, got %d", state)
	}
	if tx.IsActive() {
		t.Error("expected IsActive=false after Rollback")
	}
}

func TestTransaction_Commit_NoChanges_SetsCommitted(t *testing.T) {
	tx := newTestTransaction(t)
	// No change events → Commit should succeed immediately without the commit loop
	if err := tx.Commit(); err != nil {
		t.Fatalf("unexpected error on empty commit: %v", err)
	}
	state := TransactionState(tx.state.Load())
	if state != TRANSACTION_COMMITTED {
		t.Errorf("expected COMMITTED state, got %d", state)
	}
}

func TestTransaction_Commit_AlreadyCommitted_ReturnsError(t *testing.T) {
	tx := newTestTransaction(t)
	if err := tx.Commit(); err != nil {
		t.Fatalf("first Commit failed: %v", err)
	}
	// Second Commit should fail — state is no longer PROCESSING
	if err := tx.Commit(); err == nil {
		t.Error("expected error on second Commit")
	}
}

func TestTransaction_Table_Nonexistent(t *testing.T) {
	tx := newTestTransaction(t)
	table, err := tx.Table("nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if table != nil {
		t.Error("expected nil for nonexistent table")
	}
}

func TestTransaction_CreateTable_Success(t *testing.T) {
	tx := newTestTransaction(t)
	schema := &TableSchema{
		Name:         "orders",
		PrimaryIndex: []string{"id"},
		IndexedColumns: map[string]vals.ValueType{
			"id": vals.TYPE_UINT64,
		},
	}
	table, err := tx.CreateTable(schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	if table == nil {
		t.Error("expected non-nil table")
	}
}

func TestTransaction_CreateTable_ThenTable_FindsIt(t *testing.T) {
	tx := newTestTransaction(t)
	schema := &TableSchema{
		Name:         "items",
		PrimaryIndex: []string{"id"},
		IndexedColumns: map[string]vals.ValueType{
			"id": vals.TYPE_UINT64,
		},
	}
	if _, err := tx.CreateTable(schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	table, err := tx.Table("items")
	if err != nil {
		t.Fatalf("Table failed: %v", err)
	}
	if table == nil {
		t.Error("expected to find 'items' table after creating it")
	}
}

func TestTransaction_DropTable(t *testing.T) {
	tx := newTestTransaction(t)
	schema := &TableSchema{
		Name:         "temp",
		PrimaryIndex: []string{"id"},
		IndexedColumns: map[string]vals.ValueType{
			"id": vals.TYPE_UINT64,
		},
	}
	if _, err := tx.CreateTable(schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	if err := tx.DropTable("temp"); err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}
}

func TestTransaction_CancelledContext_CommitReturnsError(t *testing.T) {
	p := newTestPager()
	state := TableManagerState{Root: pager.NULL_PAGE, Version: 1}
	nextID := uint64(1)
	manager := newTableManager(state, func() TableID {
		nextID++
		return TableID(nextID)
	}, p)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	queue := make(chan TransactionCommit, 1)
	tx := &Transaction{
		manager:     manager,
		commitQueue: queue,
		ctx:         ctx,
	}
	tx.state.Store(int32(TRANSACTION_PROCESSING))

	// Force a change event so Commit actually tries to send to the queue
	schema := &TableSchema{
		Name:         "ctx_test",
		PrimaryIndex: []string{"id"},
		IndexedColumns: map[string]vals.ValueType{
			"id": vals.TYPE_UINT64,
		},
	}
	if _, err := tx.CreateTable(schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Drain the queue so the send blocks, causing context cancellation to fire
	err := tx.Commit()
	if err == nil {
		t.Error("expected error from Commit on cancelled context")
	}
}

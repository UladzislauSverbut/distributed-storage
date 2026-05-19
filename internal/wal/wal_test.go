package wal

import (
	"distributed-storage/internal/events"
	"path/filepath"
	"testing"
)

func newTestWAL(t *testing.T) *WAL {
	t.Helper()
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	archiveDir := filepath.Join(walDir, "archive")

	if err := setupFS(DatabaseConfig{
		Directory:           dir,
		WALDirectory:        walDir,
		WALArchiveDirectory: archiveDir,
	}); err != nil {
		t.Fatalf("setupFS failed: %v", err)
	}

	wal, err := newWAL(DatabaseConfig{
		WALDirectory:        walDir,
		WALArchiveDirectory: archiveDir,
		WALSegmentSize:      1 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("newWAL failed: %v", err)
	}
	return wal
}

func TestNewWAL_CreatesSegmentFile(t *testing.T) {
	wal := newTestWAL(t)
	if wal.segment == nil {
		t.Error("expected non-nil segment file")
	}
}

func TestNewWAL_InitialSegmentID(t *testing.T) {
	wal := newTestWAL(t)
	if wal.segmentID != INITIAL_SEGMENT_ID {
		t.Errorf("expected segmentID=%d, got %d", INITIAL_SEGMENT_ID, wal.segmentID)
	}
}

func TestWAL_Empty_NewWALIsEmpty(t *testing.T) {
	wal := newTestWAL(t)
	if !wal.empty() {
		t.Error("expected new WAL to be empty")
	}
}

func TestWAL_Empty_FalseAfterSync(t *testing.T) {
	wal := newTestWAL(t)
	wal.appendVersionUpdate(DatabaseVersion(1))
	if err := wal.sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if wal.empty() {
		t.Error("WAL should not be empty after writing and syncing events")
	}
}

func TestWAL_Sync_ClearsPendingLog(t *testing.T) {
	wal := newTestWAL(t)
	wal.appendVersionUpdate(DatabaseVersion(1))
	if err := wal.sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if len(wal.pendingLog) != 0 {
		t.Errorf("expected pendingLog to be cleared after sync, got %d bytes", len(wal.pendingLog))
	}
}

func TestWAL_Sync_UpdatesSegmentCapacity(t *testing.T) {
	wal := newTestWAL(t)
	wal.appendVersionUpdate(DatabaseVersion(1))
	if err := wal.sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if wal.segmentCapacity == 0 {
		t.Error("expected segmentCapacity > 0 after sync")
	}
}

func TestWAL_EncodeDecodeEvent_RoundTrip(t *testing.T) {
	wal := newTestWAL(t)
	original := events.NewUpdateDBVersion(42)

	encoded := wal.encodeEvent(original)
	event, consumed, err := wal.decodeEvent(encoded)
	if err != nil {
		t.Fatalf("decodeEvent failed: %v", err)
	}
	if consumed != len(encoded) {
		t.Errorf("expected consumed=%d, got %d", len(encoded), consumed)
	}
	if event == nil {
		t.Fatal("expected non-nil event")
	}
	versionEvent, ok := event.(*events.UpdateDBVersion)
	if !ok {
		t.Fatalf("expected *events.UpdateDBVersion, got %T", event)
	}
	if versionEvent.Version != 42 {
		t.Errorf("expected version=42, got %d", versionEvent.Version)
	}
}

func TestWAL_DecodeEvent_TooShort_ReturnsNil(t *testing.T) {
	wal := newTestWAL(t)
	event, consumed, err := wal.decodeEvent([]byte{1, 2, 3}) // fewer than 8 bytes
	if err != nil || event != nil || consumed != 0 {
		t.Errorf("expected nil event/error for short row: event=%v consumed=%d err=%v", event, consumed, err)
	}
}

func TestWAL_DecodeEvent_CorruptChecksum_ReturnsError(t *testing.T) {
	wal := newTestWAL(t)
	encoded := wal.encodeEvent(events.NewUpdateDBVersion(1))
	// Corrupt the checksum bytes (bytes 4-7)
	encoded[4] ^= 0xFF
	_, _, err := wal.decodeEvent(encoded)
	if err == nil {
		t.Error("expected error for corrupted checksum")
	}
}

func TestWAL_AppendTransactions_Empty_NoOp(t *testing.T) {
	wal := newTestWAL(t)
	before := len(wal.pendingLog)
	wal.appendTransactions([]TransactionCommit{})
	if len(wal.pendingLog) != before {
		t.Error("appendTransactions with empty slice should not modify pendingLog")
	}
}

func TestWAL_AppendVersionUpdate_AddsToLog(t *testing.T) {
	wal := newTestWAL(t)
	before := len(wal.pendingLog)
	wal.appendVersionUpdate(DatabaseVersion(5))
	if len(wal.pendingLog) <= before {
		t.Error("expected pendingLog to grow after appendVersionUpdate")
	}
}

func TestWAL_SegmentName_Format(t *testing.T) {
	wal := newTestWAL(t)
	name := wal.segmentName("/some/dir", SegmentID(1))
	expected := "/some/dir/" + "segment_0000000001.wal"
	if name != expected {
		t.Errorf("expected %q, got %q", expected, name)
	}
}

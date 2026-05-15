package db

import (
	"path/filepath"
	"testing"
)

func TestApplyDefaults_FillsEmptyValues(t *testing.T) {
	cfg := applyDefaults(DatabaseConfig{})
	if cfg.Directory != DEFAULT_DIRECTORY {
		t.Errorf("expected Directory=%q, got %q", DEFAULT_DIRECTORY, cfg.Directory)
	}
	if cfg.PageSize != DEFAULT_PAGE_SIZE {
		t.Errorf("expected PageSize=%d, got %d", DEFAULT_PAGE_SIZE, cfg.PageSize)
	}
	if cfg.WALSegmentSize != DEFAULT_WAL_SEGMENT_SIZE {
		t.Errorf("expected WALSegmentSize=%d, got %d", DEFAULT_WAL_SEGMENT_SIZE, cfg.WALSegmentSize)
	}
	if cfg.WALDirectory != DEFAULT_WAL_DIRECTORY {
		t.Errorf("expected WALDirectory=%q, got %q", DEFAULT_WAL_DIRECTORY, cfg.WALDirectory)
	}
	if cfg.WALArchiveDirectory != DEFAULT_WAL_ARCHIVE_DIRECTORY {
		t.Errorf("expected WALArchiveDirectory=%q, got %q", DEFAULT_WAL_ARCHIVE_DIRECTORY, cfg.WALArchiveDirectory)
	}
}

func TestApplyDefaults_DoesNotOverwriteSetValues(t *testing.T) {
	cfg := applyDefaults(DatabaseConfig{
		Directory:           "/custom/dir",
		PageSize:            4096,
		WALSegmentSize:      1024,
		WALDirectory:        "/custom/wal",
		WALArchiveDirectory: "/custom/wal/archive",
	})
	if cfg.Directory != "/custom/dir" {
		t.Errorf("expected Directory=/custom/dir, got %q", cfg.Directory)
	}
	if cfg.PageSize != 4096 {
		t.Errorf("expected PageSize=4096, got %d", cfg.PageSize)
	}
	if cfg.WALSegmentSize != 1024 {
		t.Errorf("expected WALSegmentSize=1024, got %d", cfg.WALSegmentSize)
	}
	if cfg.WALDirectory != "/custom/wal" {
		t.Errorf("expected WALDirectory=/custom/wal, got %q", cfg.WALDirectory)
	}
	if cfg.WALArchiveDirectory != "/custom/wal/archive" {
		t.Errorf("expected WALArchiveDirectory=/custom/wal/archive, got %q", cfg.WALArchiveDirectory)
	}
}

func TestSetupFS_CreatesDirectories(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	archiveDir := filepath.Join(walDir, "archive")

	cfg := DatabaseConfig{
		Directory:           dir,
		WALDirectory:        walDir,
		WALArchiveDirectory: archiveDir,
	}

	if err := setupFS(cfg); err != nil {
		t.Fatalf("setupFS failed: %v", err)
	}
	// Idempotent — second call must not fail
	if err := setupFS(cfg); err != nil {
		t.Errorf("setupFS should be idempotent but failed: %v", err)
	}
}

func TestSetupFS_InvalidPath_ReturnsError(t *testing.T) {
	cfg := DatabaseConfig{
		Directory:           "/nonexistent/totally/invalid/path",
		WALDirectory:        "/nonexistent/totally/invalid/path/wal",
		WALArchiveDirectory: "/nonexistent/totally/invalid/path/wal/archive",
	}
	if err := setupFS(cfg); err == nil {
		t.Error("expected error for invalid nested path")
	}
}

func TestNewStorage_InMemory(t *testing.T) {
	cfg := DatabaseConfig{
		InMemory: true,
		PageSize: testPageSize,
	}
	s, err := newStorage(cfg)
	if err != nil {
		t.Fatalf("newStorage failed: %v", err)
	}
	if s == nil {
		t.Error("expected non-nil storage")
	}
	if s.Size() != testPageSize*10 {
		t.Errorf("expected size=%d, got %d", testPageSize*10, s.Size())
	}
}

func TestNewStorage_FileStorage(t *testing.T) {
	cfg := DatabaseConfig{
		InMemory:  false,
		Directory: t.TempDir(),
		PageSize:  testPageSize,
	}
	s, err := newStorage(cfg)
	if err != nil {
		t.Fatalf("newStorage (file) failed: %v", err)
	}
	if s == nil {
		t.Error("expected non-nil storage")
	}
}

func TestNewStorage_FileInvalidPath_ReturnsError(t *testing.T) {
	cfg := DatabaseConfig{
		InMemory:  false,
		Directory: "/nonexistent/path",
		PageSize:  testPageSize,
	}
	_, err := newStorage(cfg)
	if err == nil {
		t.Error("expected error for invalid file path")
	}
}

package store

import (
	"bytes"
	"os"
	"testing"
)

func TestFileStorage_NewFileStorage_SizeAtLeastInitialSize(t *testing.T) {
	s, err := NewFileStorage(tempFilePath(t), 100)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	if s.Size() < 100 {
		t.Errorf("expected Size >= 100, got %d", s.Size())
	}
}

func TestFileStorage_NewFileStorage_InvalidPath_ReturnsError(t *testing.T) {
	_, err := NewFileStorage("/nonexistent/dir/file.db", 64)
	if err == nil {
		t.Error("expected error for invalid path, got nil")
	}
}

func TestFileStorage_NewFileStorage_CreatesFileOnDisk(t *testing.T) {
	path := tempFilePath(t)

	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatal("expected file to not exist before NewFileStorage")
	}
	if _, err := NewFileStorage(path, 64); err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
		t.Error("expected file to exist after NewFileStorage")
	}
}

func TestFileStorage_Segment_FreshStorage_ReturnsZeros(t *testing.T) {
	s, err := NewFileStorage(tempFilePath(t), 64)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	got := s.Segment(0, 8)
	if !bytes.Equal(got, make([]byte, 8)) {
		t.Errorf("expected zero bytes, got %v", got)
	}
}

func TestFileStorage_Segment_OutOfRange_Panics(t *testing.T) {
	s, err := NewFileStorage(tempFilePath(t), 64)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	shouldPanic(t, func() {
		s.Segment(0, s.Size()+1)
	})
}

func TestFileStorage_UpdateSegments_WritesAndReadsBack(t *testing.T) {
	s, err := NewFileStorage(tempFilePath(t), 64)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	data := []byte("hello")
	if err := s.UpdateSegments([]SegmentUpdate{{Offset: 10, Data: data}}); err != nil {
		t.Fatalf("UpdateSegments: %v", err)
	}
	got := s.Segment(10, len(data))
	if !bytes.Equal(got, data) {
		t.Errorf("expected %v, got %v", data, got)
	}
}

func TestFileStorage_UpdateSegments_MultipleUpdatesInOneCall(t *testing.T) {
	s, err := NewFileStorage(tempFilePath(t), 64)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	updates := []SegmentUpdate{
		{Offset: 0, Data: []byte("aaa")},
		{Offset: 10, Data: []byte("bbb")},
	}
	if err := s.UpdateSegments(updates); err != nil {
		t.Fatalf("UpdateSegments: %v", err)
	}
	if !bytes.Equal(s.Segment(0, 3), []byte("aaa")) {
		t.Error("first update not stored correctly")
	}
	if !bytes.Equal(s.Segment(10, 3), []byte("bbb")) {
		t.Error("second update not stored correctly")
	}
}

func TestFileStorage_UpdateSegments_GrowsStorageOnDemand(t *testing.T) {
	s, err := NewFileStorage(tempFilePath(t), 16)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	before := s.Size()

	// Write a single byte just past the current (page-aligned) allocation to
	// force the storage to grow by at least one more page.
	if err := s.UpdateSegments([]SegmentUpdate{{Offset: before, Data: []byte{0xFF}}}); err != nil {
		t.Fatalf("UpdateSegments: %v", err)
	}
	if s.Size() <= before {
		t.Errorf("expected storage to grow: before=%d after=%d", before, s.Size())
	}
	if got := s.Segment(before, 1); got[0] != 0xFF {
		t.Errorf("expected 0xFF at offset %d, got 0x%X", before, got[0])
	}
}

func TestFileStorage_Flush_Succeeds(t *testing.T) {
	s, err := NewFileStorage(tempFilePath(t), 64)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Errorf("Flush: %v", err)
	}
}

func TestFileStorage_Size_IncreasesAfterGrowth(t *testing.T) {
	s, err := NewFileStorage(tempFilePath(t), 64)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	before := s.Size()
	// Write one byte past the current (page-aligned) boundary to force growth.
	s.UpdateSegments([]SegmentUpdate{{Offset: before, Data: []byte{0x01}}})
	if s.Size() <= before {
		t.Errorf("expected Size to increase: before=%d after=%d", before, s.Size())
	}
}

func TestFileStorage_PersistsDataAcrossReopens(t *testing.T) {
	path := tempFilePath(t)
	data := []byte("persistent data")

	// Write and flush with the first instance.
	s1, err := NewFileStorage(path, 64)
	if err != nil {
		t.Fatalf("first NewFileStorage: %v", err)
	}
	if err := s1.UpdateSegments([]SegmentUpdate{{Offset: 0, Data: data}}); err != nil {
		t.Fatalf("UpdateSegments: %v", err)
	}
	if err := s1.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Reopen and verify the data is present.
	s2, err := NewFileStorage(path, 0)
	if err != nil {
		t.Fatalf("second NewFileStorage: %v", err)
	}
	got := s2.Segment(0, len(data))
	if !bytes.Equal(got, data) {
		t.Errorf("after reopen: expected %q, got %q", data, got)
	}
}

func TestFileStorage_OpenExistingFile_SizePreserved(t *testing.T) {
	path := tempFilePath(t)

	s1, err := NewFileStorage(path, 256)
	if err != nil {
		t.Fatalf("first NewFileStorage: %v", err)
	}
	size1 := s1.Size()
	s1.Flush()

	s2, err := NewFileStorage(path, 0)
	if err != nil {
		t.Fatalf("second NewFileStorage: %v", err)
	}
	if s2.Size() < size1 {
		t.Errorf("reopened size %d < original size %d", s2.Size(), size1)
	}
}

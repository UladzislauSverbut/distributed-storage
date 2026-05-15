package store

import (
	"bytes"
	"testing"
)

func TestMemoryStorage_NewMemoryStorage_SizeAtLeastInitialSize(t *testing.T) {
	s := NewMemoryStorage(100)
	if s.Size() < 100 {
		t.Errorf("expected Size >= 100, got %d", s.Size())
	}
}

func TestMemoryStorage_NewMemoryStorage_ZeroInitialSize(t *testing.T) {
	s := NewMemoryStorage(0)
	if s.Size() < 0 {
		t.Errorf("unexpected negative size: %d", s.Size())
	}
}

func TestMemoryStorage_Segment_FreshStorage_ReturnsZeros(t *testing.T) {
	s := NewMemoryStorage(64)
	got := s.Segment(0, 8)
	if !bytes.Equal(got, make([]byte, 8)) {
		t.Errorf("expected zero bytes, got %v", got)
	}
}

func TestMemoryStorage_Segment_OutOfRange_Panics(t *testing.T) {
	s := NewMemoryStorage(64)
	shouldPanic(t, func() {
		s.Segment(0, s.Size()+1)
	})
}

func TestMemoryStorage_UpdateSegments_WritesAndReadsBack(t *testing.T) {
	s := NewMemoryStorage(64)
	data := []byte("hello")
	if err := s.UpdateSegments([]SegmentUpdate{{Offset: 10, Data: data}}); err != nil {
		t.Fatalf("UpdateSegments: %v", err)
	}
	got := s.Segment(10, len(data))
	if !bytes.Equal(got, data) {
		t.Errorf("expected %v, got %v", data, got)
	}
}

func TestMemoryStorage_UpdateSegments_MultipleUpdatesInOneCall(t *testing.T) {
	s := NewMemoryStorage(64)
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

func TestMemoryStorage_UpdateSegments_OverwritesExistingData(t *testing.T) {
	s := NewMemoryStorage(64)
	s.UpdateSegments([]SegmentUpdate{{Offset: 0, Data: []byte("hello world")}})
	s.UpdateSegments([]SegmentUpdate{{Offset: 6, Data: []byte("Go   ")}})

	got := s.Segment(0, 11)
	want := []byte("hello Go   ")
	if !bytes.Equal(got, want) {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func TestMemoryStorage_UpdateSegments_GrowsStorageOnDemand(t *testing.T) {
	s := NewMemoryStorage(16)
	before := s.Size()

	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if err := s.UpdateSegments([]SegmentUpdate{{Offset: 0, Data: data}}); err != nil {
		t.Fatalf("UpdateSegments: %v", err)
	}
	if s.Size() <= before {
		t.Errorf("expected storage to grow: before=%d after=%d", before, s.Size())
	}
	if !bytes.Equal(s.Segment(0, len(data)), data) {
		t.Error("data mismatch after growth")
	}
}

func TestMemoryStorage_UpdateSegments_OffsetBeyondCurrentSize_Grows(t *testing.T) {
	s := NewMemoryStorage(0)
	data := []byte("far away")
	if err := s.UpdateSegments([]SegmentUpdate{{Offset: 1000, Data: data}}); err != nil {
		t.Fatalf("UpdateSegments: %v", err)
	}
	got := s.Segment(1000, len(data))
	if !bytes.Equal(got, data) {
		t.Errorf("expected %v, got %v", data, got)
	}
}

func TestMemoryStorage_Flush_ReturnsNil(t *testing.T) {
	s := NewMemoryStorage(64)
	if err := s.Flush(); err != nil {
		t.Errorf("Flush: %v", err)
	}
}

func TestMemoryStorage_Size_IncreasesAfterGrowth(t *testing.T) {
	s := NewMemoryStorage(64)
	before := s.Size()
	s.UpdateSegments([]SegmentUpdate{{Offset: 0, Data: make([]byte, 1024)}})
	if s.Size() <= before {
		t.Errorf("expected Size to increase: before=%d after=%d", before, s.Size())
	}
}

func TestMemoryStorage_Segment_ReturnsIndependentCopy(t *testing.T) {
	s := NewMemoryStorage(64)
	s.UpdateSegments([]SegmentUpdate{{Offset: 0, Data: []byte("original")}})

	// Mutate the returned slice; the stored data should be unchanged.
	got := s.Segment(0, 8)
	got[0] = 'X'

	got2 := s.Segment(0, 8)
	if got2[0] == 'X' {
		t.Error("Segment returned a reference into internal storage; expected a copy")
	}
}

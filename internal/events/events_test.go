package events

import (
	"bytes"
	"testing"

	"distributed-storage/internal/pager"
)

// wrongPrefix is long enough to avoid index-out-of-bounds in all prefix checks.
const wrongPrefix = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

// ── StartTransaction ───────────────────────────────────────────────────────

func TestStartTransaction_Name(t *testing.T) {
	if NewStartTransaction().Name() != START_TRANSACTION_EVENT {
		t.Errorf("expected %q", START_TRANSACTION_EVENT)
	}
}

func TestStartTransaction_Serialize_HasNamePrefix(t *testing.T) {
	b := NewStartTransaction().Serialize()
	if !bytes.HasPrefix(b, []byte(START_TRANSACTION_EVENT)) {
		t.Error("serialized StartTransaction should start with event name")
	}
}

func TestStartTransaction_SerializeParse_Roundtrip(t *testing.T) {
	parsed, err := ParseStartTransaction(NewStartTransaction().Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.Name() != START_TRANSACTION_EVENT {
		t.Errorf("expected name %q, got %q", START_TRANSACTION_EVENT, parsed.Name())
	}
}

func TestStartTransaction_Parse_ErrorOnWrongPrefix(t *testing.T) {
	if _, err := ParseStartTransaction([]byte(wrongPrefix)); err == nil {
		t.Error("expected error on wrong prefix")
	}
}

// ── CommitTransaction ──────────────────────────────────────────────────────

func TestCommitTransaction_Name(t *testing.T) {
	if NewCommitTransaction().Name() != COMMIT_TRANSACTION_EVENT {
		t.Errorf("expected %q", COMMIT_TRANSACTION_EVENT)
	}
}

func TestCommitTransaction_Serialize_HasNamePrefix(t *testing.T) {
	b := NewCommitTransaction().Serialize()
	if !bytes.HasPrefix(b, []byte(COMMIT_TRANSACTION_EVENT)) {
		t.Error("serialized CommitTransaction should start with event name")
	}
}

func TestCommitTransaction_SerializeParse_Roundtrip(t *testing.T) {
	parsed, err := ParseCommitTransaction(NewCommitTransaction().Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.Name() != COMMIT_TRANSACTION_EVENT {
		t.Errorf("expected name %q, got %q", COMMIT_TRANSACTION_EVENT, parsed.Name())
	}
}

func TestCommitTransaction_Parse_ErrorOnWrongPrefix(t *testing.T) {
	if _, err := ParseCommitTransaction([]byte(wrongPrefix)); err == nil {
		t.Error("expected error on wrong prefix")
	}
}

// ── CreateTable ────────────────────────────────────────────────────────────

func TestCreateTable_Name(t *testing.T) {
	if NewCreateTable(1, nil).Name() != CREATE_TABLE_EVENT {
		t.Errorf("expected %q", CREATE_TABLE_EVENT)
	}
}

func TestCreateTable_SerializeParse_PreservesTableID(t *testing.T) {
	original := NewCreateTable(42, []byte(`{"name":"users"}`))
	parsed, err := ParseCreateTable(original.Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.TableID != 42 {
		t.Errorf("expected TableID 42, got %d", parsed.TableID)
	}
}

func TestCreateTable_SerializeParse_PreservesSchema(t *testing.T) {
	schema := []byte(`{"name":"users","cols":["id","name"]}`)
	parsed, err := ParseCreateTable(NewCreateTable(1, schema).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(parsed.Schema, schema) {
		t.Errorf("schema mismatch: got %q", parsed.Schema)
	}
}

func TestCreateTable_SerializeParse_EmptySchema(t *testing.T) {
	parsed, err := ParseCreateTable(NewCreateTable(7, []byte{}).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.TableID != 7 {
		t.Errorf("expected TableID 7, got %d", parsed.TableID)
	}
}

func TestCreateTable_Parse_ErrorOnWrongPrefix(t *testing.T) {
	if _, err := ParseCreateTable([]byte(wrongPrefix)); err == nil {
		t.Error("expected error on wrong prefix")
	}
}

// ── DeleteTable ────────────────────────────────────────────────────────────

func TestDeleteTable_Name(t *testing.T) {
	if NewDeleteTable(1).Name() != DELETE_TABLE_EVENT {
		t.Errorf("expected %q", DELETE_TABLE_EVENT)
	}
}

func TestDeleteTable_SerializeParse_PreservesTableID(t *testing.T) {
	original := NewDeleteTable(99)
	parsed, err := ParseDeleteTable(original.Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.TableID != 99 {
		t.Errorf("expected TableID 99, got %d", parsed.TableID)
	}
}

func TestDeleteTable_Parse_ErrorOnWrongPrefix(t *testing.T) {
	if _, err := ParseDeleteTable([]byte(wrongPrefix)); err == nil {
		t.Error("expected error on wrong prefix")
	}
}

// ── UpdateTable ────────────────────────────────────────────────────────────

func TestUpdateTable_Name(t *testing.T) {
	if NewUpdateTable(1, nil, nil).Name() != UPDATE_TABLE_EVENT {
		t.Errorf("expected %q", UPDATE_TABLE_EVENT)
	}
}

func TestUpdateTable_SerializeParse_PreservesTableID(t *testing.T) {
	parsed, err := ParseUpdateTable(NewUpdateTable(55, []byte("old"), []byte("new")).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.TableID != 55 {
		t.Errorf("expected TableID 55, got %d", parsed.TableID)
	}
}

func TestUpdateTable_SerializeParse_PreservesSchemas(t *testing.T) {
	oldSchema := []byte(`{"v":1}`)
	newSchema := []byte(`{"v":2,"extra":"field"}`)
	parsed, err := ParseUpdateTable(NewUpdateTable(3, oldSchema, newSchema).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(parsed.OldSchema, oldSchema) {
		t.Errorf("OldSchema mismatch: got %q", parsed.OldSchema)
	}
	if !bytes.Equal(parsed.NewSchema, newSchema) {
		t.Errorf("NewSchema mismatch: got %q", parsed.NewSchema)
	}
}

func TestUpdateTable_Parse_ErrorOnWrongPrefix(t *testing.T) {
	if _, err := ParseUpdateTable([]byte(wrongPrefix)); err == nil {
		t.Error("expected error on wrong prefix")
	}
}

// ── InsertEntry ────────────────────────────────────────────────────────────

func TestInsertEntry_Name(t *testing.T) {
	if NewInsertEntry(1, nil, nil).Name() != INSERT_ENTRY_EVENT {
		t.Errorf("expected %q", INSERT_ENTRY_EVENT)
	}
}

func TestInsertEntry_SerializeParse_PreservesTableID(t *testing.T) {
	parsed, err := ParseInsertEntry(NewInsertEntry(7, []byte("key"), []byte("val")).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.TableID != 7 {
		t.Errorf("expected TableID 7, got %d", parsed.TableID)
	}
}

func TestInsertEntry_SerializeParse_PreservesKeyAndValue(t *testing.T) {
	key := []byte("my-key")
	value := []byte("my-value-payload")
	parsed, err := ParseInsertEntry(NewInsertEntry(1, key, value).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(parsed.Key, key) {
		t.Errorf("Key mismatch: got %q", parsed.Key)
	}
	if !bytes.Equal(parsed.Value, value) {
		t.Errorf("Value mismatch: got %q", parsed.Value)
	}
}

func TestInsertEntry_SerializeParse_BinaryKeyValue(t *testing.T) {
	key := []byte{0x00, 0x01, 0xFF}
	value := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	parsed, err := ParseInsertEntry(NewInsertEntry(2, key, value).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(parsed.Key, key) || !bytes.Equal(parsed.Value, value) {
		t.Error("binary key/value roundtrip failed")
	}
}

func TestInsertEntry_Parse_ErrorOnWrongPrefix(t *testing.T) {
	if _, err := ParseInsertEntry([]byte(wrongPrefix)); err == nil {
		t.Error("expected error on wrong prefix")
	}
}

// ── DeleteEntry ────────────────────────────────────────────────────────────

func TestDeleteEntry_Name(t *testing.T) {
	if NewDeleteEntry(1, nil, nil).Name() != DELETE_ENTRY_EVENT {
		t.Errorf("expected %q", DELETE_ENTRY_EVENT)
	}
}

func TestDeleteEntry_SerializeParse_PreservesFields(t *testing.T) {
	key := []byte("del-key")
	value := []byte("old-val")
	parsed, err := ParseDeleteEntry(NewDeleteEntry(10, key, value).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.TableID != 10 {
		t.Errorf("expected TableID 10, got %d", parsed.TableID)
	}
	if !bytes.Equal(parsed.Key, key) {
		t.Errorf("Key mismatch: got %q", parsed.Key)
	}
	if !bytes.Equal(parsed.Value, value) {
		t.Errorf("Value mismatch: got %q", parsed.Value)
	}
}

func TestDeleteEntry_Parse_ErrorOnWrongPrefix(t *testing.T) {
	if _, err := ParseDeleteEntry([]byte(wrongPrefix)); err == nil {
		t.Error("expected error on wrong prefix")
	}
}

// ── UpdateEntry ────────────────────────────────────────────────────────────

func TestUpdateEntry_Name(t *testing.T) {
	if NewUpdateEntry(1, nil, nil, nil).Name() != UPDATE_ENTRY_EVENT {
		t.Errorf("expected %q", UPDATE_ENTRY_EVENT)
	}
}

func TestUpdateEntry_SerializeParse_PreservesTableID(t *testing.T) {
	parsed, err := ParseUpdateEntry(NewUpdateEntry(20, []byte("k"), []byte("old"), []byte("new")).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.TableID != 20 {
		t.Errorf("expected TableID 20, got %d", parsed.TableID)
	}
}

func TestUpdateEntry_SerializeParse_PreservesKeyAndValues(t *testing.T) {
	key := []byte("the-key")
	oldVal := []byte("before")
	newVal := []byte("after-update")
	parsed, err := ParseUpdateEntry(NewUpdateEntry(5, key, oldVal, newVal).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(parsed.Key, key) {
		t.Errorf("Key mismatch: got %q", parsed.Key)
	}
	if !bytes.Equal(parsed.OldValue, oldVal) {
		t.Errorf("OldValue mismatch: got %q", parsed.OldValue)
	}
	if !bytes.Equal(parsed.NewValue, newVal) {
		t.Errorf("NewValue mismatch: got %q", parsed.NewValue)
	}
}

func TestUpdateEntry_Parse_ErrorOnWrongPrefix(t *testing.T) {
	if _, err := ParseUpdateEntry([]byte(wrongPrefix)); err == nil {
		t.Error("expected error on wrong prefix")
	}
}

// ── UpdateDBVersion ────────────────────────────────────────────────────────

func TestUpdateDBVersion_Name(t *testing.T) {
	if NewUpdateDBVersion(1).Name() != UPDATE_DB_VERSION {
		t.Errorf("expected %q", UPDATE_DB_VERSION)
	}
}

func TestUpdateDBVersion_SerializeParse_PreservesVersion(t *testing.T) {
	parsed, err := ParseUpdateDBVersion(NewUpdateDBVersion(123456).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.Version != 123456 {
		t.Errorf("expected Version 123456, got %d", parsed.Version)
	}
}

func TestUpdateDBVersion_SerializeParse_ZeroVersion(t *testing.T) {
	parsed, err := ParseUpdateDBVersion(NewUpdateDBVersion(0).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.Version != 0 {
		t.Errorf("expected Version 0, got %d", parsed.Version)
	}
}

func TestUpdateDBVersion_Parse_ErrorOnWrongPrefix(t *testing.T) {
	if _, err := ParseUpdateDBVersion([]byte(wrongPrefix)); err == nil {
		t.Error("expected error on wrong prefix")
	}
}

// ── FreePages ──────────────────────────────────────────────────────────────

func TestFreePages_Name(t *testing.T) {
	if NewFreePages(1, pager.NewPageList()).Name() != FREE_PAGES_EVENT {
		t.Errorf("expected %q", FREE_PAGES_EVENT)
	}
}

func TestFreePages_SerializeParse_PreservesVersion(t *testing.T) {
	original := NewFreePages(77, pager.NewPageList())
	parsed, err := ParseFreePages(original.Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.Version != 77 {
		t.Errorf("expected Version 77, got %d", parsed.Version)
	}
}

func TestFreePages_SerializeParse_EmptyPageList(t *testing.T) {
	original := NewFreePages(1, pager.NewPageList())
	parsed, err := ParseFreePages(original.Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.List.Pages()) != 0 {
		t.Errorf("expected 0 intervals, got %d", len(parsed.List.Pages()))
	}
}

func TestFreePages_SerializeParse_PreservesIntervals(t *testing.T) {
	intervals := []pager.PageInterval{
		{Start: 10, End: 20},
		{Start: 30, End: 40},
	}
	original := NewFreePages(5, pager.NewPageList(intervals...))
	parsed, err := ParseFreePages(original.Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pages := parsed.List.Pages()
	if len(pages) != 2 {
		t.Fatalf("expected 2 intervals, got %d", len(pages))
	}
	if pages[0].Start != 10 || pages[0].End != 20 {
		t.Errorf("interval 0: got {%d, %d}", pages[0].Start, pages[0].End)
	}
	if pages[1].Start != 30 || pages[1].End != 40 {
		t.Errorf("interval 1: got {%d, %d}", pages[1].Start, pages[1].End)
	}
}

func TestFreePages_Parse_ErrorOnWrongPrefix(t *testing.T) {
	if _, err := ParseFreePages([]byte(wrongPrefix)); err == nil {
		t.Error("expected error on wrong prefix")
	}
}

// ── Parse (router) ─────────────────────────────────────────────────────────

func TestParse_StartTransaction(t *testing.T) {
	ev, err := Parse(NewStartTransaction().Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Name() != START_TRANSACTION_EVENT {
		t.Errorf("expected %q, got %q", START_TRANSACTION_EVENT, ev.Name())
	}
}

func TestParse_CommitTransaction(t *testing.T) {
	ev, err := Parse(NewCommitTransaction().Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Name() != COMMIT_TRANSACTION_EVENT {
		t.Errorf("expected %q, got %q", COMMIT_TRANSACTION_EVENT, ev.Name())
	}
}

func TestParse_InsertEntry(t *testing.T) {
	ev, err := Parse(NewInsertEntry(1, []byte("k"), []byte("v")).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Name() != INSERT_ENTRY_EVENT {
		t.Errorf("expected %q, got %q", INSERT_ENTRY_EVENT, ev.Name())
	}
}

func TestParse_DeleteEntry(t *testing.T) {
	ev, err := Parse(NewDeleteEntry(1, []byte("k"), []byte("v")).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Name() != DELETE_ENTRY_EVENT {
		t.Errorf("expected %q, got %q", DELETE_ENTRY_EVENT, ev.Name())
	}
}

func TestParse_UpdateEntry(t *testing.T) {
	ev, err := Parse(NewUpdateEntry(1, []byte("k"), []byte("old"), []byte("new")).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Name() != UPDATE_ENTRY_EVENT {
		t.Errorf("expected %q, got %q", UPDATE_ENTRY_EVENT, ev.Name())
	}
}

func TestParse_CreateTable(t *testing.T) {
	ev, err := Parse(NewCreateTable(1, []byte("{}")).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Name() != CREATE_TABLE_EVENT {
		t.Errorf("expected %q, got %q", CREATE_TABLE_EVENT, ev.Name())
	}
}

func TestParse_DeleteTable(t *testing.T) {
	ev, err := Parse(NewDeleteTable(1).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Name() != DELETE_TABLE_EVENT {
		t.Errorf("expected %q, got %q", DELETE_TABLE_EVENT, ev.Name())
	}
}

func TestParse_UpdateTable(t *testing.T) {
	ev, err := Parse(NewUpdateTable(1, []byte("old"), []byte("new")).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Name() != UPDATE_TABLE_EVENT {
		t.Errorf("expected %q, got %q", UPDATE_TABLE_EVENT, ev.Name())
	}
}

func TestParse_UpdateDBVersion(t *testing.T) {
	ev, err := Parse(NewUpdateDBVersion(1).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Name() != UPDATE_DB_VERSION {
		t.Errorf("expected %q, got %q", UPDATE_DB_VERSION, ev.Name())
	}
}

func TestParse_FreePages(t *testing.T) {
	ev, err := Parse(NewFreePages(1, pager.NewPageList()).Serialize())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Name() != FREE_PAGES_EVENT {
		t.Errorf("expected %q, got %q", FREE_PAGES_EVENT, ev.Name())
	}
}

func TestParse_UnknownType_ReturnsError(t *testing.T) {
	if _, err := Parse([]byte("UNKNOWN_EVENT_TYPE_XXXXX")); err == nil {
		t.Error("expected error for unknown event type")
	}
}

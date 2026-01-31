package db

import "distributed-storage/internal/pager"

type Event interface {
	EventName() string
}

const (
	START_TRANSACTION_EVENT  = "START_TRANSACTION"
	COMMIT_TRANSACTION_EVENT = "COMMIT_TRANSACTION"
	ABORT_TRANSACTION_EVENT  = "ABORT_TRANSACTION"
	CREATE_TABLE_EVENT       = "CREATE_TABLE"
	DELETE_TABLE_EVENT       = "DELETE_TABLE"
	UPSERT_ENTRY_EVENT       = "UPSERT_ENTRY"
	DELETE_ENTRY_EVENT       = "DELETE_ENTRY"
	UPDATE_ENTRY_EVENT       = "UPDATE_ENTRY"
	INSERT_ENTRY_EVENT       = "INSERT_ENTRY"
	FREE_PAGES_EVENT         = "FREE_PAGES"
)

type StartTransaction struct {
	TxID TransactionID
}

func (e *StartTransaction) EventName() string {
	return START_TRANSACTION_EVENT
}

type CommitTransaction struct {
	TxID TransactionID
}

func (e *CommitTransaction) EventName() string {
	return COMMIT_TRANSACTION_EVENT
}

type AbortTransaction struct {
	TxID TransactionID
}

func (e *AbortTransaction) EventName() string {
	return ABORT_TRANSACTION_EVENT
}

type CreateTable struct {
	TableName string
	Schema    []byte
}

func (e *CreateTable) EventName() string {
	return CREATE_TABLE_EVENT
}

type DeleteTable struct {
	TableName string
	TableRoot pager.PagePointer
}

func (e *DeleteTable) EventName() string {
	return DELETE_TABLE_EVENT
}

type DeleteEntry struct {
	TableName string
	Key       []byte
	Value     []byte
}

func (e *DeleteEntry) EventName() string {
	return DELETE_ENTRY_EVENT
}

type UpdateEntry struct {
	TableName string
	Key       []byte
	NewValue  []byte
	OldValue  []byte
}

func (e *UpdateEntry) EventName() string {
	return UPDATE_ENTRY_EVENT
}

type UpsertEntry struct {
	TableName string
	Key       []byte
	NewValue  []byte
	OldValue  []byte
}

func (e *UpsertEntry) EventName() string {
	return UPSERT_ENTRY_EVENT
}

type InsertEntry struct {
	TableName string
	Key       []byte
	Value     []byte
}

func (e *InsertEntry) EventName() string {
	return INSERT_ENTRY_EVENT
}

type FreePages struct {
	TxID  TransactionID
	Pages []pager.PagePointer
}

func (e *FreePages) EventName() string {
	return FREE_PAGES_EVENT
}

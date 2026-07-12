package transaction

import (
	"context"
	"distributed-storage/internal/catalog"
	"distributed-storage/internal/helpers"
	"sync"
)

type TransactionManager struct {
	commitQueue  chan TransactionCommit
	transactions *helpers.MinMap[catalog.DatabaseVersion, *Transaction]
	mu           sync.Mutex
}

func NewTransactionManager(queueSize int) *TransactionManager {
	return &TransactionManager{
		commitQueue:  make(chan TransactionCommit, queueSize),
		transactions: helpers.NewMinMap[catalog.DatabaseVersion, *Transaction](func(i, j catalog.DatabaseVersion) bool { return i < j }),
	}
}

// Begin creates a new transaction for the given manager, registers it, and returns it.
func (m *TransactionManager) Begin(manager *catalog.Catalog, ctx context.Context) *Transaction {
	tx := NewTransaction(manager, m.commitQueue, ctx)
	m.mu.Lock()
	m.transactions.Add(manager.Version(), tx)
	m.mu.Unlock()
	return tx
}

// CommitQueue returns the read-only channel that the commit loop should listen on.
func (m *TransactionManager) CommitQueue() <-chan TransactionCommit {
	return m.commitQueue
}

// LatestUnreachableVersion returns the latest database version that no active
// transaction is reading from, bounded by syncedVersion.
func (m *TransactionManager) LatestUnreachableVersion(syncedVersion catalog.DatabaseVersion) catalog.DatabaseVersion {
	m.mu.Lock()
	defer m.mu.Unlock()

	for {
		version, transactions, ok := m.transactions.PeekMin()
		if !ok {
			return syncedVersion
		}

		for _, tx := range transactions {
			if tx.IsActive() {
				return min(version-1, syncedVersion)
			}
		}

		m.transactions.PopMin()
	}
}

// Approve sends a success response to each transaction in the batch.
func (m *TransactionManager) Approve(transactions []TransactionCommit) {
	for _, tx := range transactions {
		tx.Response <- TransactionCommitResponse{Success: true}
	}
}

// Reject sends an error response to each transaction in the batch.
func (m *TransactionManager) Reject(transactions []TransactionCommit, err error) {
	for _, tx := range transactions {
		tx.Response <- TransactionCommitResponse{Error: err, Success: false}
	}
}

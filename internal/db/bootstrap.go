package db

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	"encoding/binary"
)

func setupStorage(config *DatabaseConfig) (walStorage, dbStorage store.Storage, err error) {
	if walStorage, err = store.NewFileStorage(config.Directory + "/wal.log"); err != nil {
		return
	}

	if config.InMemory {
		dbStorage = store.NewMemoryStorage()
	} else {
		dbStorage, err = store.NewFileStorage(config.Directory + "/data.db")
	}

	return
}

func parseHeader(header []byte) (rootPage pager.PagePointer, nextTransactionID TransactionID, pagesCount uint64) {
	rootPage = pager.PagePointer(binary.LittleEndian.Uint64(header[0:8]))
	nextTransactionID = TransactionID(binary.LittleEndian.Uint64(header[8:16]))
	pagesCount = binary.LittleEndian.Uint64(header[16:24])

	return
}

func buildHeader(rootPage pager.PagePointer, nextTransactionID TransactionID, pagesCount uint64) []byte {
	header := make([]byte, HEADER_SIZE)

	binary.LittleEndian.PutUint64(header[0:8], uint64(rootPage))
	binary.LittleEndian.PutUint64(header[8:16], uint64(nextTransactionID))
	binary.LittleEndian.PutUint64(header[16:24], uint64(pagesCount))
	return header
}

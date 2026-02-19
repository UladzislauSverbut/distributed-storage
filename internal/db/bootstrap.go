package db

import (
	"distributed-storage/internal/store"
)

func setupStorage(config DatabaseConfig) (dbStorage store.Storage, err error) {
	initialSize := config.PageSize * 10

	if config.InMemory {
		dbStorage = store.NewMemoryStorage(initialSize)
	} else {
		dbStorage, err = store.NewFileStorage(config.Directory+"/data.db", initialSize)
	}

	return
}

const DEFAULT_DIRECTORY = "/var/lib/kv"
const DEFAULT_PAGE_SIZE = 16 * 1024               // 16KB
const DEFAULT_WAL_SEGMENT_SIZE = 10 * 1024 * 1024 // 10MB

func applyDefaults(config DatabaseConfig) DatabaseConfig {
	if config.Directory == "" {
		config.Directory = DEFAULT_DIRECTORY
	}

	if config.PageSize == 0 {
		config.PageSize = DEFAULT_PAGE_SIZE
	}

	if config.WALSegmentSize == 0 {
		config.WALSegmentSize = DEFAULT_WAL_SEGMENT_SIZE
	}

	return config
}

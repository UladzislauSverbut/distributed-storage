package db

import (
	"distributed-storage/internal/store"
)

func setupStorage(config DatabaseConfig) (walStorage, dbStorage store.Storage, err error) {
	initialSize := config.PageSize * 10

	if walStorage, err = store.NewFileStorage(config.Directory+"/wal.log", initialSize); err != nil {
		return
	}

	if config.InMemory {
		dbStorage = store.NewMemoryStorage(initialSize)
	} else {
		dbStorage, err = store.NewFileStorage(config.Directory+"/data.db", initialSize)
	}

	return
}

const DEFAULT_DIRECTORY = "/var/lib/kv"
const DEFAULT_PAGE_SIZE = 16 * 1024 // 16KB

func applyDefaults(config DatabaseConfig) DatabaseConfig {
	if config.Directory == "" {
		config.Directory = DEFAULT_DIRECTORY
	}

	if config.PageSize == 0 {
		config.PageSize = DEFAULT_PAGE_SIZE
	}

	return config
}

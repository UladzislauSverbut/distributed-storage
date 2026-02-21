package db

import (
	"distributed-storage/internal/store"
	"fmt"
	"os"
)

func setupStorage(config DatabaseConfig) (dbStorage store.Storage, err error) {
	if err := os.Mkdir(config.Directory, 0755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("Bootstrap: failed to create storage directory: %w", err)
	}

	if err = os.Mkdir(config.WALDirectory, 0755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("Bootstrap: failed to create WAL segment directory: %w", err)
	}

	if err = os.Mkdir(config.WALArchiveDirectory, 0755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("Bootstrap: failed to create WAL archive subdirectory: %w", err)
	}

	initialSize := config.PageSize * 10

	if config.InMemory {
		dbStorage = store.NewMemoryStorage(initialSize)
		return
	}

	if dbStorage, err = store.NewFileStorage(config.Directory+"/data.db", initialSize); err != nil {
		return nil, fmt.Errorf("Bootstrap: failed to create file storage: %w", err)
	}

	return
}

const DEFAULT_DIRECTORY = "/var/lib/kv"
const DEFAULT_WAL_DIRECTORY = "wal"
const DEFAULT_WAL_ARCHIVE_DIRECTORY = "archive"

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

	if config.WALDirectory == "" {
		config.WALDirectory = DEFAULT_WAL_DIRECTORY
	}

	if config.WALArchiveDirectory == "" {
		config.WALArchiveDirectory = DEFAULT_WAL_ARCHIVE_DIRECTORY
	}

	return config
}

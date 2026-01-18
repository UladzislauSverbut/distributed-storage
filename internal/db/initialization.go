package db

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
)

const DEFAULT_PAGE_SIZE = 16 * 1024 // 16KB

func initializeStorage(config *DatabaseConfig) (store.Storage, error) {
	if config.InMemory {
		return store.NewMemoryStorage(), nil
	}

	directory := config.Directory

	if directory == "" {
		directory = DEFAULT_DIRECTORY
	}

	return store.NewFileStorage(directory + "/data")
}

func initializePageManager(config *DatabaseConfig) (*pager.PageManager, error) {
	storage, err := initializeStorage(config)

	if err != nil {
		return nil, err
	}

	pageSize := config.PageSize

	if pageSize == 0 {
		pageSize = DEFAULT_PAGE_SIZE
	}

	return pager.NewPageManager(storage, pageSize)
}

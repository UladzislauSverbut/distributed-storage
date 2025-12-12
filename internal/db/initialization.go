package db

import (
	"distributed-storage/internal/store"
)

func initializeStorage(config *DatabaseConfig) (store.Storage, error) {
	if config.InMemory {
		return store.NewMemoryStorage(), nil
	}

	dir := config.Directory
	if dir == "" {
		dir = DEFAULT_DIRECTORY
	}

	return store.NewFileStorage(dir + "/data")
}

package db

import (
	"distributed-storage/internal/store"
)

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

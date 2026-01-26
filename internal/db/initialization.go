package db

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/store"
	"distributed-storage/internal/vals"
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

func initializeSchemaTable(root pager.PagePointer, pager *pager.PageManager) *Table {
	schema := &TableSchema{
		Name:         "@schemas",
		ColumnNames:  []string{"name", "definition", "root", "size"},
		PrimaryIndex: []string{"name"},
		ColumnTypes:  map[string]vals.ValueType{"name": vals.TYPE_STRING, "definition": vals.TYPE_STRING, "root": vals.TYPE_UINT64, "size": vals.TYPE_UINT64},
	}

	schemaTable, _ := NewTable(root, pager, schema, 0)

	return schemaTable
}

package db

import "distributed-storage/internal/kv"

const DEFAULT_DIRECTORY string = "/var/lib/kv/data"

type Database struct {
	keyValue *kv.KeyValue
	tables   map[string]*Table
}

type DatabaseConfig struct {
	Directory string
}

func New(config *DatabaseConfig) (*Database, error) {
	storageDirectory := config.Directory

	if storageDirectory == "" {
		storageDirectory = DEFAULT_DIRECTORY
	}

	storageDirectory += "/data"

	keyValue, err := kv.New(storageDirectory)

	if err != nil {
		return nil, err
	}

	return &Database{
		keyValue: keyValue,
		tables:   make(map[string]*Table),
	}, nil

}

// func (database *Database) Get(tableName string) *Table {
// 	table, exist := db.tables[tableName]

// 	if !exist {
// 		query := new(Object)
// 		query.addString("name", []byte(tableName))

// 		kek, err := SCHEMAS_TABLE.Get(query)
// 	}
// }

// var META_TABLE = &Table{
// 	Prefix:       1,
// 	Name:         "@meta",
// 	ColumnTypes:  []ValueType{VALUE_TYPE_BYTES, VALUE_TYPE_INT64},
// 	ColumnNames:  []string{"key", "value"},
// 	IndexColumns: []string{"key"},
// }

// var SCHEMAS_TABLE = &Table{
// 	Prefix:       1,
// 	Name:         "@schema",
// 	ColumnTypes:  []ValueType{VALUE_TYPE_BYTES, VALUE_TYPE_BYTES},
// 	ColumnNames:  []string{"name", "definition"},
// 	IndexColumns: []string{"name"},
// }

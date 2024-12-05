package db

import (
	"distributed-storage/internal/kv"
	"encoding/json"
	"fmt"
)

const DEFAULT_DIRECTORY = "/var/lib/kv/data"
const META_TABLE_ID = "meta"
const SCHEMA_TABLE_ID = "schemas"

type Database struct {
	kv     *kv.KeyValue
	tables map[string]*Table
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

	database := &Database{
		kv:     keyValue,
		tables: make(map[string]*Table),
	}

	database.initSystemTables()

	return database, nil
}

func (database *Database) Get(tableName string) *Table {
	table, exist := database.tables[tableName]

	if !exist {
		schema := (&Record{}).Set("name", &StringValue{[]byte(tableName)})

		exist, err := database.tables[SCHEMA_TABLE_ID].Get(schema)

		if err != nil {
			panic(fmt.Sprintf("Database can`t read schema table %v", table))
		}

		if !exist {
			return nil
		}

		table = database.parseTableSchema(schema)
		database.tables[tableName] = table
	}

	return table
}

func (database *Database) initSystemTables() {
	database.tables[META_TABLE_ID] = &Table{
		Name:         "@meta",
		ColumnTypes:  []ValueType{VALUE_TYPE_STRING, VALUE_TYPE_INT64},
		ColumnNames:  []string{"key", "value"},
		IndexColumns: []string{"key"},
		Prefix:       1,
		kv:           database.kv,
	}

	database.tables[SCHEMA_TABLE_ID] = &Table{
		Name:         "@schema",
		ColumnTypes:  []ValueType{VALUE_TYPE_STRING, VALUE_TYPE_STRING},
		ColumnNames:  []string{"name", "definition"},
		IndexColumns: []string{"name"},
		Prefix:       1,
		kv:           database.kv,
	}
}

func (database *Database) parseTableSchema(record *Record) *Table {
	tableSchema := record.Get("definition").(*StringValue).Val

	table := &Table{
		kv: database.kv,
	}

	if err := json.Unmarshal(tableSchema, table); err != nil {
		panic(fmt.Sprintf("Database can`t parse scheme %v", err))
	}

	return table
}

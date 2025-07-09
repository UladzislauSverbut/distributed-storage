package db

import (
	"distributed-storage/internal/kv"
	"encoding/json"
	"fmt"
)

const DEFAULT_DIRECTORY = "/var/lib/kv"
const META_TABLE_NAME = "@meta"
const SCHEMA_TABLE_NAME = "@schemas"

type Database struct {
	kv     *kv.Namespace
	tables map[string]*Table
}

type DatabaseConfig struct {
	Directory string
}

func NewDatabase(config *DatabaseConfig) (*Database, error) {
	storageDirectory := config.Directory

	if storageDirectory == "" {
		storageDirectory = DEFAULT_DIRECTORY
	}

	storageDirectory += "/data"

	database := &Database{
		kv:     kv.NewRootNamespace(storageDirectory),
		tables: make(map[string]*Table),
	}

	database.initSystemTables()

	return database, nil
}

func (database *Database) Get(tableName string) *Table {
	_, exist := database.tables[tableName]

	if !exist {
		schema := database.getTableSchema(tableName)

		if schema != nil {
			database.tables[tableName] = &Table{schema: schema, kv: kv.NewChildNamespace(database.kv, schema.Name)}
		}
	}

	return database.tables[tableName]
}

func (database *Database) Create(schema *TableSchema) (*Table, error) {
	if err := database.validateTableSchema(schema); err != nil {
		return nil, err
	}

	table := database.Get(schema.Name)

	if table != nil {
		return nil, fmt.Errorf("Database can`t create table %s because it`s already exist", schema.Name)
	}

	database.saveTableSchema(schema)

	table = &Table{
		schema: schema,
		kv:     kv.NewChildNamespace(database.kv, schema.Name),
	}

	return table, nil
}

func (database *Database) List() []*Object {
	schemaTable := database.Get(SCHEMA_TABLE_NAME)

	return schemaTable.GetAll()
}

func (database *Database) getTableSchema(tableName string) *TableSchema {
	schemaTable := database.Get(SCHEMA_TABLE_NAME)

	query := NewObject().
		Set("name", NewStringValue(tableName))

	record, err := schemaTable.Get(query)

	if err != nil {
		panic(fmt.Errorf("Database can`t read schema table %w", err))
	}

	if record == nil {
		return nil
	}

	tableSchema := &TableSchema{}

	if err := json.Unmarshal([]byte(record.Get("definition").(*StringValue).Get()), tableSchema); err != nil {
		panic(fmt.Errorf("Database can`t parse scheme %w", err))
	}

	return tableSchema
}

func (database *Database) saveTableSchema(schema *TableSchema) error {
	schemaTable := database.Get(SCHEMA_TABLE_NAME)
	stringifiedSchema, _ := json.Marshal(schema)

	query := NewObject().
		Set("name", NewStringValue(schema.Name)).
		Set("definition", NewStringValue(string(stringifiedSchema)))

	return schemaTable.Insert(query)
}

func (database *Database) validateTableSchema(schema *TableSchema) error {
	return nil
}

func (database *Database) initSystemTables() {
	database.tables[META_TABLE_NAME] = &Table{
		schema: &TableSchema{
			Name:         META_TABLE_NAME,
			ColumnTypes:  []ValueType{VALUE_TYPE_STRING, VALUE_TYPE_UINT32},
			ColumnNames:  []string{"key", "value"},
			PrimaryIndex: []string{"key"},
		},
		kv: kv.NewChildNamespace(database.kv, META_TABLE_NAME),
	}

	database.tables[SCHEMA_TABLE_NAME] = &Table{
		schema: &TableSchema{
			Name:         SCHEMA_TABLE_NAME,
			ColumnTypes:  []ValueType{VALUE_TYPE_STRING, VALUE_TYPE_STRING},
			ColumnNames:  []string{"name", "definition"},
			PrimaryIndex: []string{"name"},
		},
		kv: kv.NewChildNamespace(database.kv, SCHEMA_TABLE_NAME),
	}
}

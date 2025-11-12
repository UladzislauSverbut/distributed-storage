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
	kv     *kv.KeyValue
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
		kv:     kv.NewKeyValue(storageDirectory),
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
			database.tables[tableName] = &Table{schema: schema, kv: kv.WithPrefix(database.kv, schema.Name)}
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
		kv:     kv.WithPrefix(database.kv, schema.Name),
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
		panic(fmt.Errorf("Database: can`t read schema table %w", err))
	}

	if record == nil {
		return nil
	}

	tableSchema := &TableSchema{}

	if err := json.Unmarshal([]byte(record.Get("definition").(*StringValue).Value()), tableSchema); err != nil {
		panic(fmt.Errorf("Database: can`t parse schema %w", err))
	}

	return tableSchema
}

func (database *Database) saveTableSchema(schema *TableSchema) error {
	transaction := NewTransaction(*database, SCHEMA_TABLE_NAME)

	schemaTable := database.Get(SCHEMA_TABLE_NAME)
	stringifiedSchema, _ := json.Marshal(schema)

	query := NewObject().
		Set("name", NewStringValue(schema.Name)).
		Set("definition", NewStringValue(string(stringifiedSchema)))

	if err := schemaTable.Insert(query); err != nil {
		transaction.Abort()

		return fmt.Errorf("Database: can't save schema %w", err)
	}

	return transaction.Commit()
}

func (database *Database) validateTableSchema(schema *TableSchema) error {
	return nil
}

func (database *Database) initSystemTables() {
	database.tables[META_TABLE_NAME] = &Table{
		schema: &TableSchema{
			Name:         META_TABLE_NAME,
			ColumnNames:  []string{"key", "value"},
			PrimaryIndex: []string{"key"},
			ColumnTypes:  map[string]ValueType{"key": VALUE_TYPE_STRING, "value": VALUE_TYPE_UINT32},
		},
		kv: kv.WithPrefix(database.kv, META_TABLE_NAME),
	}

	database.tables[SCHEMA_TABLE_NAME] = &Table{
		schema: &TableSchema{
			Name:         SCHEMA_TABLE_NAME,
			ColumnNames:  []string{"name", "definition"},
			PrimaryIndex: []string{"name"},
			ColumnTypes:  map[string]ValueType{"name": VALUE_TYPE_STRING, "definition": VALUE_TYPE_STRING},
		},
		kv: kv.WithPrefix(database.kv, SCHEMA_TABLE_NAME),
	}
}

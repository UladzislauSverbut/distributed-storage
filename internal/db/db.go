package db

import (
	"distributed-storage/internal/kv"
	"encoding/json"
	"fmt"
)

const DEFAULT_DIRECTORY = "/var/lib/kv"
const META_TABLE_NAME = "@meta"
const SCHEMA_TABLE_NAME = "@schemas"

const MIN_UNRESERVED_TABLE_ID = uint32(3)

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

	keyValue, err := kv.NewKeyValue(storageDirectory)

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
		schema := database.getTableSchema(tableName)

		if schema == nil {
			return nil
		}

		table = &Table{schema: schema, kv: database.kv}
		database.tables[tableName] = table
	}

	return table
}

func (database *Database) Create(schema *TableSchema) (*Table, error) {
	if err := database.validateTableSchema(schema); err != nil {
		return nil, err
	}

	table := database.Get(schema.Name)

	if table != nil {
		return nil, fmt.Errorf("Database can`t create table %s because it`s already exist", schema.Name)
	}

	tableId, err := database.getNextTableId()

	if err != nil {
		return nil, err
	}

	schema.Prefix = tableId

	database.saveTableSchema(schema)

	table = &Table{
		schema: schema,
		kv:     database.kv,
	}

	return table, nil
}

func (database *Database) getTableSchema(tableName string) *TableSchema {
	schemaTable := database.Get(SCHEMA_TABLE_NAME)

	query := NewObject().
		Set("name", NewStringValue(tableName))

	record, err := schemaTable.Get(query)

	if err != nil {
		panic(fmt.Sprintf("Database can`t read schema table %v", schemaTable))
	}

	if record == nil {
		return nil
	}

	tableSchema := &TableSchema{}

	if err := json.Unmarshal([]byte(record.Get("definition").(*StringValue).Get()), tableSchema); err != nil {
		panic(fmt.Sprintf("Database can`t parse scheme %v", err))
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

func (database *Database) getNextTableId() (uint32, error) {
	metaTable := database.Get(META_TABLE_NAME)

	query := NewObject().
		Set("key", NewStringValue("next_table_id"))

	record, err := metaTable.Get(query)

	if err != nil {
		return 0, err
	}

	availableTableId := MIN_UNRESERVED_TABLE_ID

	if record != nil {
		availableTableId = record.Get("value").(*IntValue[uint32]).Get()
	}

	query.Set("value", NewIntValue(availableTableId+1))

	if err = metaTable.Upsert(query); err != nil {
		return 0, err
	}

	return availableTableId, nil
}

func (database *Database) initSystemTables() {
	database.tables[META_TABLE_NAME] = &Table{
		schema: &TableSchema{
			Name:         META_TABLE_NAME,
			ColumnTypes:  []ValueType{VALUE_TYPE_STRING, VALUE_TYPE_UINT32},
			ColumnNames:  []string{"key", "value"},
			PrimaryIndex: []string{"key"},
			Prefix:       1,
		},
		kv: database.kv,
	}

	database.tables[SCHEMA_TABLE_NAME] = &Table{
		schema: &TableSchema{
			Name:         SCHEMA_TABLE_NAME,
			ColumnTypes:  []ValueType{VALUE_TYPE_STRING, VALUE_TYPE_STRING},
			ColumnNames:  []string{"name", "definition"},
			PrimaryIndex: []string{"name"},
			Prefix:       2,
		},
		kv: database.kv,
	}
}

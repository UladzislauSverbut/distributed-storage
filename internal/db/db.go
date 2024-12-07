package db

import (
	"distributed-storage/internal/kv"
	"encoding/json"
	"fmt"
	"strconv"
)

const DEFAULT_DIRECTORY = "/var/lib/kv/data"
const META_TABLE_NAME = "@meta"
const SCHEMA_TABLE_NAME = "@schemas"

const MIN_UNRESERVED_TABLE_ID = 3

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

func (database *Database) Create(schema TableSchema) (*Table, error) {
	if err := database.validateTableSchema(&schema); err != nil {
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

	database.saveTableSchema(&schema)

	table = &Table{
		schema: &schema,
		kv:     database.kv,
	}

	return table, nil
}

func (database *Database) getTableSchema(tableName string) *TableSchema {
	schemaTable := database.Get(SCHEMA_TABLE_NAME)

	query := NewRecord().Set("name", NewStringValue(tableName))

	exist, err := schemaTable.Get(query)

	if err != nil {
		panic(fmt.Sprintf("Database can`t read schema table %v", schemaTable))
	}

	if !exist {
		return nil
	}

	tableSchema := &TableSchema{}

	if err := json.Unmarshal([]byte(query.Get("definition").(*StringValue).Get()), tableSchema); err != nil {
		panic(fmt.Sprintf("Database can`t parse scheme %v", err))
	}

	return tableSchema
}

func (database *Database) saveTableSchema(schema *TableSchema) error {
	schemaTable := database.Get(SCHEMA_TABLE_NAME)
	stringifiedSchema, _ := json.Marshal(schema)

	query := NewRecord().Set("name", NewStringValue(schema.Name)).Set("definition", NewStringValue(string(stringifiedSchema)))

	return schemaTable.Insert(query)
}

func (database *Database) validateTableSchema(schema *TableSchema) error {
	return nil
}

func (database *Database) getNextTableId() (uint32, error) {
	metaTable := database.Get(META_TABLE_NAME)
	query := NewRecord().Set("key", NewStringValue("next_table_id"))

	exist, err := metaTable.Get(query)

	if !exist {
		query.Set("value", NewStringValue(strconv.Itoa(MIN_UNRESERVED_TABLE_ID)))
	}

	if err != nil {
		return 0, err
	}

	availableTableId := query.Get("value").(*StringValue).Get()
	parsedTableId, err := strconv.Atoi(availableTableId)

	if err != nil {
		return 0, err
	}

	query.Set("value", NewStringValue(strconv.Itoa(parsedTableId+1)))

	if err = metaTable.Upsert(query); err != nil {
		return 0, err
	}

	return uint32(parsedTableId), nil
}

func (database *Database) initSystemTables() {

	database.tables[META_TABLE_NAME] = &Table{
		schema: &TableSchema{
			Name:         META_TABLE_NAME,
			ColumnTypes:  []ValueType{VALUE_TYPE_STRING, VALUE_TYPE_INT64},
			ColumnNames:  []string{"key", "value"},
			IndexColumns: []string{"key"},
			Prefix:       1,
		},
		kv: database.kv,
	}

	database.tables[SCHEMA_TABLE_NAME] = &Table{
		schema: &TableSchema{
			Name:         SCHEMA_TABLE_NAME,
			ColumnTypes:  []ValueType{VALUE_TYPE_STRING, VALUE_TYPE_STRING},
			ColumnNames:  []string{"name", "definition"},
			IndexColumns: []string{"name"},
			Prefix:       2,
		},
		kv: database.kv,
	}
}

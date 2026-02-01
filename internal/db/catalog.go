package db

import (
	"distributed-storage/internal/pager"
	"distributed-storage/internal/vals"
	"encoding/json"
	"fmt"
)

var catalogSchema = TableSchema{
	Name:         "@catalog",
	ColumnNames:  []string{"name", "definition", "root", "size"},
	PrimaryIndex: []string{"name"},
	ColumnTypes:  map[string]vals.ValueType{"name": vals.TYPE_STRING, "definition": vals.TYPE_STRING, "root": vals.TYPE_UINT64, "size": vals.TYPE_UINT64},
}

type TableDescriptor struct {
	root   pager.PagePointer
	name   string
	size   uint64
	schema *TableSchema
}

type Catalog struct {
	table  *Table
	tables map[string]*TableDescriptor
}

func NewCatalog(root pager.PagePointer, pageManager *pager.PageManager) (*Catalog, error) {
	table, err := NewTable(root, pageManager, &catalogSchema, 0)

	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't initialize catalog table: %w", err)
	}

	return &Catalog{
		table:  table,
		tables: make(map[string]*TableDescriptor),
	}, nil
}

func (catalog *Catalog) getTable(name string) (*TableDescriptor, error) {
	if table, ok := catalog.tables[name]; ok {
		return table, nil
	}

	query := vals.NewObject().Set("name", vals.NewString(name))

	record, err := catalog.table.Get(query)

	if err != nil {
		return nil, fmt.Errorf("Catalog: can't read table schema: %w", err)
	}

	definition := record.Get("definition").(*vals.StringValue).Value()
	root := record.Get("root").(*vals.IntValue[uint64]).Value()
	size := record.Get("size").(*vals.IntValue[uint64]).Value()
	schema := &TableSchema{}

	if err := json.Unmarshal([]byte(definition), schema); err != nil {
		return nil, fmt.Errorf("Catalog: can't parse table schema: %w", err)
	}

	catalog.tables[name] = &TableDescriptor{
		root:   root,
		name:   name,
		size:   size,
		schema: schema,
	}

	return catalog.tables[name], nil
}

func (catalog *Catalog) updateTable(name string, table *TableDescriptor) error {
	stringifiedSchema, _ := json.Marshal(table.schema)

	schemaRecord := vals.NewObject().
		Set("name", vals.NewString(table.name)).
		Set("definition", vals.NewString(string(stringifiedSchema))).
		Set("root", vals.NewInt(table.root)).
		Set("size", vals.NewInt(table.size))

	if err := catalog.table.Upsert(schemaRecord); err != nil {
		return fmt.Errorf("Transaction: couldn't save table %s: %w", name, err)
	}

	return nil
}

func (catalog *Catalog) Root() pager.PagePointer {
	return catalog.table.Root()
}

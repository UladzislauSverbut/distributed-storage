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
	table       *Table
	tables      map[string]*Table
	pageManager *pager.PageManager
}

func NewCatalog(root pager.PagePointer, pageManager *pager.PageManager) (*Catalog, error) {
	table, err := NewTable(root, pageManager, &catalogSchema, 0)

	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't initialize catalog table: %w", err)
	}

	return &Catalog{
		table:       table,
		tables:      make(map[string]*Table),
		pageManager: pageManager,
	}, nil
}

func (catalog *Catalog) GetTable(name string) (*Table, error) {
	if table, ok := catalog.tables[name]; ok {
		return table, nil
	}

	query := vals.NewObject().Set("name", vals.NewString(name))

	record, err := catalog.table.Get(query)

	if err != nil {
		return nil, fmt.Errorf("Catalog: can't read table schema: %w", err)
	}

	if record == nil {
		return nil, nil
	}

	definition := record.Get("definition").(*vals.StringValue).Value()
	root := record.Get("root").(*vals.IntValue[uint64]).Value()
	size := record.Get("size").(*vals.IntValue[uint64]).Value()
	schema := &TableSchema{}

	if err := json.Unmarshal([]byte(definition), schema); err != nil {
		return nil, fmt.Errorf("Catalog: can't parse table schema: %w", err)
	}

	catalog.tables[name], err = NewTable(root, catalog.pageManager, schema, size)
	if err != nil {
		return nil, fmt.Errorf("Catalog: can't initialize table %s: %w", name, err)
	}

	return catalog.tables[name], nil
}

func (catalog *Catalog) UpdateTable(name string, table *Table) error {
	stringifiedSchema, _ := json.Marshal(table.schema)

	schemaRecord := vals.NewObject().
		Set("name", vals.NewString(table.schema.Name)).
		Set("definition", vals.NewString(string(stringifiedSchema))).
		Set("root", vals.NewInt(table.Root())).
		Set("size", vals.NewInt(table.Size()))

	if err := catalog.table.Upsert(schemaRecord); err != nil {
		return fmt.Errorf("Transaction: couldn't save table %s: %w", name, err)
	}

	return nil
}

func (catalog *Catalog) CreateTable(schema *TableSchema) (*Table, error) {
	table, err := NewTable(pager.NULL_PAGE, catalog.pageManager, schema, 0)
	if err != nil {
		return nil, fmt.Errorf("Catalog: couldn't create table %s: %w", schema.Name, err)
	}

	if err := catalog.UpdateTable(schema.Name, table); err != nil {
		return nil, fmt.Errorf("Catalog: couldn't register table %s in catalog: %w", schema.Name, err)
	}

	return table, nil
}

func (catalog *Catalog) DeleteTable(name string) error {
	query := vals.NewObject().Set("name", vals.NewString(name))

	if err := catalog.table.Delete(query); err != nil {
		return fmt.Errorf("Catalog: couldn't delete table %s from catalog: %w", name, err)
	}

	delete(catalog.tables, name)
	return nil
}

func (catalog *Catalog) Root() pager.PagePointer {
	return catalog.table.Root()
}

func (catalog *Catalog) Save() error {
	for name, table := range catalog.tables {
		if err := catalog.UpdateTable(name, table); err != nil {
			return fmt.Errorf("Catalog: couldn't save table %s: %w", name, err)
		}
	}

	return nil
}

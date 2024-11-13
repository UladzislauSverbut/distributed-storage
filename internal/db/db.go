package db

import "distributed-storage/internal/kv"

type DB struct {
	Path   string
	kv     kv.DistributedKV
	tables map[string]*TableDef
}

type TableDef struct {
	Name        string
	ColumnTypes []ValueType
	ColumnNames []string
	PKeys       int
	Prefix      uint32
}

var META_TABLE = &TableDef{
	Prefix:      1,
	Name:        "@meta",
	ColumnTypes: []ValueType{VALUE_TYPE_BYTES, VALUE_TYPE_INT64},
	ColumnNames: []string{"key", "value"},
	PKeys:       1,
}

var SCHEMAS_TABLE = &TableDef{
	Prefix:      1,
	Name:        "@schema",
	ColumnTypes: []ValueType{VALUE_TYPE_BYTES, VALUE_TYPE_BYTES},
	ColumnNames: []string{"name", "definition"},
	PKeys:       1,
}

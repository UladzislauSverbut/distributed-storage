package events

// TxID is an identifier of transaction stored in events.
type TxID uint64

// Event represents a WAL event independent from db package.
// It intentionally does NOT know about db.Catalog to avoid cyclic imports.
type Event interface {
	Name() string
	Serialize() []byte
	Parse(data []byte) error
}

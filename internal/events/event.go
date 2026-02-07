package events

// Event represents a WAL event independent from db package.
// It intentionally does NOT know about db internals to avoid cyclic imports.
type Event interface {
	Name() string
	Serialize() []byte
	Parse(data []byte) error
}

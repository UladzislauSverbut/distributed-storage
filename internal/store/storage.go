package store

type MemorySegmentUpdate struct {
	Offset int
	Data   []byte
}

type Storage interface {
	Flush() error
	Size() int
	MemorySegment(offset int, size int) []byte
	AppendMemorySegment(data []byte) error
	UpdateMemorySegments(updates []MemorySegmentUpdate) error
}

package store

type Storage interface {
	Flush() error
	Size() int
	MemorySegment(offset int, size int) []byte
	AppendMemorySegment(data []byte) error
	UpdateMemorySegment(offset int, data []byte) error
}

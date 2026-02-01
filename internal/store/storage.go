package store

type Storage interface {
	Flush() error
	Size() int
	IncreaseSize(size int) error
	MemorySegment(offset int, size int) []byte
	AppendMemorySegment(data []byte) (int, error)
	UpdateMemorySegment(offset int, data []byte)
}

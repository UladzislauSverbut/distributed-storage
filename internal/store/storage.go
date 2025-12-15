package store

type Storage interface {
	Flush() error
	Size() int
	IncreaseSize(size int) error
	MemorySegment(size int, offset int) []byte
	UpdateMemorySegment(data []byte, offset int)
	SaveMemorySegment(data []byte, offset int) error
}

package storage

type Storage interface {
	Flush() error
	Size() int
	IncreaseSize(size int) error
	MemoryBlock(size int, offset int) []byte
	UpdateMemoryBlock(data []byte, offset int) error
	FlushMemoryBlock(data []byte, offset int) error
}

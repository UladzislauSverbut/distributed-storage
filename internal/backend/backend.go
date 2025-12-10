package backend

type Backend interface {
	Flush() error
	Size() int
	IncreaseSize(size int) error
	MemoryBlock(size int, offset int) []byte
	UpdateMemoryBlock(data []byte, offset int)
	FlushMemoryBlock(data []byte, offset int) error
}

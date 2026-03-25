package store

type SegmentUpdate struct {
	Offset int
	Data   []byte
}

type Storage interface {
	Segment(offset int, size int) []byte
	UpdateSegments(updates []SegmentUpdate) error
	Flush() error
	Size() int
}

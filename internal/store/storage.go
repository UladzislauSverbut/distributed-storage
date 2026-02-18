package store

type SegmentUpdate struct {
	Offset int
	Data   []byte
}

type Storage interface {
	Segment(offset int, size int) []byte
	UpdateSegments(updates []SegmentUpdate) error
	UpdateSegmentsAndFlush(updates []SegmentUpdate) error // Atomic write + fsync under
	AppendSegmentAndFlush(data []byte) error              // Atomic append + fsync
	Flush() error
	Size() int
}

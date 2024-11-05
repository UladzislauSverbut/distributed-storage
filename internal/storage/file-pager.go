package storage

type PagePointer = uint64

type Page struct {
	data []byte
}

type FilePager struct {
	root PagePointer
}

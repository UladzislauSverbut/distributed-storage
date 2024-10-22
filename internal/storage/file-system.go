package storage

import (
	"os"
)

type File struct {
	pointer *os.File
	path    string
	size    int
}

type Chunks struct {
	totalSize int
	data      [][]byte
}

type Pages struct {
	all [][]byte
}

type FileSystemStorage struct {
	file           File
	pageSize       int
	allocatedPages [][]byte
	memorySize     int
	virtualMemory  [][]byte
}

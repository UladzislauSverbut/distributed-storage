package helpers

import (
	"bufio"
	"io"
	"iter"
	"os"
)

func ReadFileByChunk(file *os.File, chunkSize int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		reader := bufio.NewReaderSize(file, chunkSize)

		chunk := make([]byte, chunkSize)

		for {
			readBytes, err := reader.Read(chunk)

			if err != nil && err != io.EOF {
				yield(nil, err)
				return
			}

			if !yield(chunk[:readBytes], nil) {
				return
			}

			if err == io.EOF {
				return
			}
		}
	}
}

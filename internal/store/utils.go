package store

func findMemorySegment(segments [][]byte, size int, offset int) []byte {
	block := make([]byte, size)
	blockStart := 0

	for _, segment := range segments {
		if offset < 0 {
			break
		}

		if offset >= len(segment) {
			offset -= len(segment)
			continue
		}

		blockEnd := min(size-blockStart, len(segment)-offset)

		copy(block[blockStart:blockStart+blockEnd], segment[offset:offset+blockEnd])

		blockStart += blockEnd
		offset -= blockStart
	}

	return block
}

func writeMemorySegment(segments [][]byte, data []byte, offset int) {
	for _, segment := range segments {
		if offset >= 0 && offset < len(segment) {
			blockEnd := min(len(data), len(segment)-offset)

			copy(segment[offset:offset+blockEnd], data[:blockEnd])

			if blockEnd == len(data) {
				return
			}

			data = data[blockEnd:]
			offset = 0
		} else {
			offset -= len(segment)
			continue
		}
	}
}

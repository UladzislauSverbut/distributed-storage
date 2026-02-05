package helpers

import "unsafe"

func CopySlice[T comparable](src []T) []T {

	if len(src) == 0 {
		return []T{}
	}

	sliceLength := len(src)

	dst := make([]T, sliceLength)

	copy(unsafe.Slice(&dst[0], sliceLength), unsafe.Slice(&src[0], sliceLength))
	return dst
}

func StringifySlice[T comparable](src []T, stringifier func(T) string, separator string) string {
	str := ""

	for idx, elem := range src {
		str += stringifier(elem)

		if idx < len(src)-1 {
			str += separator
		}
	}

	return str
}

func ReadSegments[T comparable](segments [][]T, offset int, size int) []T {
	block := make([]T, size)
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

func FillSegments[T comparable](segments [][]T, offset int, data []T) {
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

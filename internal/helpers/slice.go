package helpers

import "unsafe"

func CopySlice[T comparable](src []T) []T {
	sliceLength := len(src)

	dst := make([]T, sliceLength)

	copy(unsafe.Slice(&dst[0], sliceLength), unsafe.Slice(&src[0], sliceLength))
	return dst
}

func StringifySlice[T comparable](src []T, separator string) string {
	str := ""

	for idx, elem := range src {
		str += string(rune(unsafe.Sizeof(elem)))

		if idx < len(src)-1 {
			str += separator
		}
	}

	return str
}

package helpers

type Set[T comparable] struct {
	elements map[T]struct{}
}

func NewSet[T comparable]() Set[T] {
	return Set[T]{
		elements: make(map[T]struct{}),
	}
}

func (set Set[T]) Add(element T) {
	set.elements[element] = struct{}{}
}

func (set Set[T]) Pop() (T, bool) {
	var value T
	var ok bool = false

	if len(set.elements) > 0 {
		for key := range set.elements {
			value = key
			ok = true
			set.Remove(value)

			break
		}
	}

	return value, ok
}

func (set Set[T]) Remove(element T) {
	delete(set.elements, element)
}

func (set Set[T]) Has(element T) bool {
	_, exists := set.elements[element]
	return exists
}

func (set Set[T]) Values() []T {
	if len(set.elements) == 0 {
		return []T{}
	}

	values := make([]T, 0, len(set.elements))

	for key := range set.elements {
		values = append(values, key)
	}

	return values
}

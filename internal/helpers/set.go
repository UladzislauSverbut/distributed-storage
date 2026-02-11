package helpers

type Set[T comparable] struct {
	elements map[T]struct{}
}

func NewSet[T comparable](initialElements ...T) Set[T] {
	set := Set[T]{
		elements: make(map[T]struct{}),
	}

	for _, element := range initialElements {
		set.Add(element)
	}

	return set
}

func (s *Set[T]) Add(element T) {
	if s.elements == nil {
		s.elements = make(map[T]struct{})
	}
	s.elements[element] = struct{}{}
}

func (s *Set[T]) Pop() (T, bool) {
	var value T
	var ok bool = false

	if len(s.elements) > 0 {
		for key := range s.elements {
			value = key
			ok = true
			s.Remove(value)

			break
		}
	}

	return value, ok
}

func (s *Set[T]) Remove(element T) {
	if s.elements == nil {
		return
	}
	delete(s.elements, element)
}

func (s *Set[T]) Has(element T) bool {
	if s.elements == nil {
		return false
	}

	_, exists := s.elements[element]
	return exists
}

func (s *Set[T]) Values() []T {
	if len(s.elements) == 0 {
		return []T{}
	}

	values := make([]T, 0, len(s.elements))
	for key := range s.elements {
		values = append(values, key)
	}

	return values
}

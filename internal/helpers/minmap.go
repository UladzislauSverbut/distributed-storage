package helpers

import (
	"container/heap"
)

type MinMap[Key comparable, Value any] struct {
	items      map[Key][]Value
	heap       keyHeap[Key]
	index      map[Key]int
	comparator func(i, j Key) bool
}

func NewMinMap[Key comparable, Value any](comparator func(i, j Key) bool) *MinMap[Key, Value] {
	m := &MinMap[Key, Value]{
		items:      make(map[Key][]Value),
		index:      make(map[Key]int),
		comparator: comparator,
	}
	m.heap.comparator = comparator
	m.heap.index = m.index
	return m
}

func (m *MinMap[Key, Value]) Add(key Key, value Value) {
	if _, exists := m.items[key]; !exists {
		heap.Push(&m.heap, key)
	}

	m.items[key] = append(m.items[key], value)
}

func (m *MinMap[Key, Value]) Get(key Key) ([]Value, bool) {
	values, ok := m.items[key]
	return values, ok
}

func (m *MinMap[Key, Value]) PeekMin() (Key, []Value, bool) {
	if m.heap.Len() == 0 {
		var empty Key
		return empty, nil, false
	}
	key := m.heap.keys[0]
	return key, m.items[key], true
}

func (m *MinMap[Key, Value]) PopMin() (Key, []Value, bool) {
	if m.heap.Len() == 0 {
		var zero Key
		return zero, nil, false
	}
	key := heap.Pop(&m.heap).(Key)
	values := m.items[key]
	delete(m.items, key)
	delete(m.index, key)
	return key, values, true
}

func (m *MinMap[Key, Value]) RemoveKey(key Key) bool {
	idx, ok := m.index[key]
	if !ok {
		return false
	}
	heap.Remove(&m.heap, idx)
	delete(m.items, key)
	delete(m.index, key)
	return true
}

func (m *MinMap[Key, Value]) Len() int {
	return len(m.items)
}

type keyHeap[Key comparable] struct {
	keys       []Key
	comparator func(a, b Key) bool
	index      map[Key]int
}

func (h keyHeap[Key]) Len() int {
	return len(h.keys)
}

func (h keyHeap[Key]) Less(i, j int) bool {
	return h.comparator(h.keys[i], h.keys[j])
}

func (h keyHeap[Key]) Swap(i, j int) {
	h.keys[i], h.keys[j] = h.keys[j], h.keys[i]

	h.index[h.keys[i]] = i
	h.index[h.keys[j]] = j
}

func (h *keyHeap[Key]) Push(key any) {
	h.keys = append(h.keys, key.(Key))
	h.index[key.(Key)] = len(h.keys) - 1
}

func (h *keyHeap[Key]) Pop() any {
	keysCount := len(h.keys)
	key := h.keys[keysCount-1]
	h.keys = h.keys[:keysCount-1]

	delete(h.index, key)

	return key
}

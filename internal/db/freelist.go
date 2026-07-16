package db

import (
	"distributed-storage/internal/pager"
	"encoding/binary"
)

/*
	Node Format

	| pointer to next node | next available page offset | total pages count | page pointer - released db version pairs  |
	|          8B          |            4B              |        4B         |           numbed of pages * 16B           |

*/

const (
	HEADER_SIZE = 16
)

type freeList struct {
	head  pager.PagePointer
	tail  pager.PagePointer
	pager *pager.Pager
}

func newFreeList(head, tail pager.PagePointer, pg *pager.Pager) *freeList {
	return &freeList{head: head, tail: tail, pager: pg}
}

func (list *freeList) push(version uint64, pages []pager.PagePointer) error {
	if len(pages) == 0 {
		return nil
	}

	if list.isEarlierVersion(list.head, version) {
		return list.insertAtHead(version, pages)
	}

	return list.appendToTail(version, pages)
}

func (list *freeList) collect(version uint64) ([]pager.PagePointer, error) {
	var result []pager.PagePointer

	for list.head != pager.NULL_PAGE {
		data := list.pager.Page(list.head)
		next := pager.PagePointer(binary.LittleEndian.Uint64(data[:8]))
		readPos := int(binary.LittleEndian.Uint32(data[8:12]))
		count := int(binary.LittleEndian.Uint32(data[12:16]))

		newReadPos := readPos
		for newReadPos < count {
			offset := HEADER_SIZE + newReadPos*16
			if binary.LittleEndian.Uint64(data[offset:]) > maxVersion {
				break
			}
			result = append(result, pager.PagePointer(binary.LittleEndian.Uint64(data[offset+8:])))
			newReadPos++
		}

		if newReadPos == count {
			// Node fully consumed: reclaim the node page itself and advance head
			result = append(result, list.head)
			list.head = next
			if list.head == pager.NULL_PAGE {
				list.tail = pager.NULL_PAGE
			}
		} else {
			// Partially consumed: persist the new read position and stop
			if newReadPos > readPos {
				updated := make([]byte, list.pager.PageSize())
				copy(updated, data)
				binary.LittleEndian.PutUint32(updated[8:12], uint32(newReadPos))
				if err := list.pager.UpdatePage(list.head, updated); err != nil {
					return nil, err
				}
			}
			break
		}
	}

	return result, nil
}

func (list *freeList) isEarlierVersion(node pager.PagePointer, version uint64) bool {
	if node == pager.NULL_PAGE {
		return false
	}
	data := list.pager.Page(node)
	readPos := binary.LittleEndian.Uint32(data[8:12])
	count := binary.LittleEndian.Uint32(data[12:16])
	if readPos >= count {
		return false
	}
	nodeVersion := binary.LittleEndian.Uint64(data[HEADER_SIZE+readPos*16:])
	return nodeVersion > version
}

func (list *freeList) insertAtHead(version uint64, pages []pager.PagePointer) error {
	wasEmpty := list.head == pager.NULL_PAGE
	oldHead := list.head
	segment := wrapToFreeList(list.pager, version, pages)

	if !wasEmpty {
		tailData := list.pager.Page(segment.tail)
		updated := make([]byte, list.pager.PageSize())
		copy(updated, tailData)
		binary.LittleEndian.PutUint64(updated[:8], uint64(oldHead))
		if err := list.pager.UpdatePage(segment.tail, updated); err != nil {
			return err
		}
	}

	list.head = segment.head
	if wasEmpty {
		list.tail = segment.tail
	}
	return nil
}

func (list *freeList) appendToTail(version uint64, pages []pager.PagePointer) error {
	if len(pages) == 0 {
		return nil
	}

	// Pack into existing tail node if it has space
	if list.tail != pager.NULL_PAGE {
		oldTail := list.tail
		capacity := list.nodeCapacity()
		tailData := list.pager.Page(oldTail)
		count := int(binary.LittleEndian.Uint32(tailData[12:16]))

		if count < capacity {
			fill := min(capacity-count, len(pages))

			// Build overflow chain first so we know the next pointer upfront
			var overflowHead pager.PagePointer = pager.NULL_PAGE
			if fill < len(pages) {
				overflow := wrapToFreeList(list.pager, version, pages[fill:])
				overflowHead = overflow.head
				list.tail = overflow.tail
			}

			updated := make([]byte, list.pager.PageSize())
			copy(updated, tailData)
			binary.LittleEndian.PutUint64(updated[:8], uint64(overflowHead))
			for i, p := range pages[:fill] {
				offset := HEADER_SIZE + (count+i)*16
				binary.LittleEndian.PutUint64(updated[offset:], version)
				binary.LittleEndian.PutUint64(updated[offset+8:], uint64(p))
			}
			binary.LittleEndian.PutUint32(updated[12:16], uint32(count+fill))
			return list.pager.UpdatePage(oldTail, updated)
		}
	}

	// Tail is NULL or full: create a new chain and link it
	segment := wrapToFreeList(list.pager, version, pages)

	if list.tail != pager.NULL_PAGE {
		tailData := list.pager.Page(list.tail)
		updated := make([]byte, list.pager.PageSize())
		copy(updated, tailData)
		binary.LittleEndian.PutUint64(updated[:8], uint64(segment.head))
		if err := list.pager.UpdatePage(list.tail, updated); err != nil {
			return err
		}
	} else {
		list.head = segment.head
	}

	list.tail = segment.tail
	return nil
}

func (list *freeList) nodeCapacity() int {
	return (list.pager.PageSize() - HEADER_SIZE) / 16
}

func wrapToFreeList(pg *pager.Pager, version uint64, pages []pager.PagePointer) *freeList {
	capacity := (pg.PageSize() - HEADER_SIZE) / 16
	next := pager.NULL_PAGE
	tail := pager.NULL_PAGE

	for i := len(pages); i > 0; i -= capacity {
		start := max(0, i-capacity)
		data := make([]byte, pg.PageSize())
		binary.LittleEndian.PutUint64(data[:8], uint64(next))
		count := 0
		for _, p := range pages[start:i] {
			offset := HEADER_SIZE + count*16
			binary.LittleEndian.PutUint64(data[offset:], version)
			binary.LittleEndian.PutUint64(data[offset+8:], uint64(p))
			count++
		}
		binary.LittleEndian.PutUint32(data[12:16], uint32(count))
		node := pg.CreatePage(data)
		if tail == pager.NULL_PAGE {
			tail = node
		}
		next = node
	}

	return &freeList{head: next, tail: tail, pager: pg}
}

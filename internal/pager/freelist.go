package pager

import "encoding/binary"

/*
	FreeList node format

	| next node | read offset | page count | reuse version | page pointers   |
	|    8B     |     4B      |     4B     |      8B       | page count * 8B |

	The list is a queue of freed-page batches. Each Push appends one batch —
	all sharing a single reuse version — as a chain of nodes at the tail. pop
	consumes pages from the head oldest-first; once a node's pages are drained,
	the node's own page is handed out as reusable too.
*/

const (
	freeListNext    = 0  // next node pointer
	freeListRead    = 8  // index of the next page to hand out
	freeListCount   = 12 // number of page pointers stored
	freeListVersion = 16 // reuse version shared by every page in the node
	freeListHeader  = 24
)

type FreeList struct {
	Head  PagePointer
	Tail  PagePointer
	arena *PageArena
}

func NewFreeList(head, tail PagePointer, arena *PageArena) *FreeList {
	return &FreeList{Head: head, Tail: tail, arena: arena}
}

// Push appends a batch of freed pages, stamped with the version at or after
// which they may be reused, as new nodes at the tail of the list.
func (list *FreeList) Push(version uint64, pages []PagePointer) error {
	if len(pages) == 0 {
		return nil
	}

	head, tail := list.buildChain(version, pages)

	if list.Tail == NULL_PAGE {
		list.Head = head
	} else if err := list.setNext(list.Tail, head); err != nil {
		return err
	}

	list.Tail = tail
	return nil
}

// pop returns a single page safe to reuse at maxVersion, advancing the head
// cursor. ok=false means the head batch is not reusable yet (or the list is
// empty), so the caller should grow the file instead. A fully consumed node
// yields its own page as the next reusable page.
func (list *FreeList) pop(maxVersion uint64) (PagePointer, bool) {
	for list.Head != NULL_PAGE {
		node := list.arena.Page(list.Head)

		if binary.LittleEndian.Uint64(node[freeListVersion:]) > maxVersion {
			return NULL_PAGE, false
		}

		readPos := binary.LittleEndian.Uint32(node[freeListRead:])
		count := binary.LittleEndian.Uint32(node[freeListCount:])

		if readPos < count {
			page := PagePointer(binary.LittleEndian.Uint64(node[freeListHeader+int(readPos)*8:]))
			list.setReadPos(list.Head, readPos+1)
			return page, true
		}

		// Node drained: reclaim its page and advance to the next node.
		reclaimed := list.Head
		list.Head = PagePointer(binary.LittleEndian.Uint64(node[freeListNext:]))
		if list.Head == NULL_PAGE {
			list.Tail = NULL_PAGE
		}
		return reclaimed, true
	}

	return NULL_PAGE, false
}

// buildChain wraps pages into a linked chain of nodes and returns its head
// and tail. It builds from the end so each node knows its successor at
// creation time, keeping page order head-to-tail.
func (list *FreeList) buildChain(version uint64, pages []PagePointer) (head, tail PagePointer) {
	capacity := list.nodeCapacity()
	next := NULL_PAGE

	for end := len(pages); end > 0; end -= capacity {
		start := max(0, end-capacity)
		node := list.newNode(version, next, pages[start:end])
		if tail == NULL_PAGE {
			tail = node
		}
		next = node
	}

	return next, tail
}

func (list *FreeList) newNode(version uint64, next PagePointer, pages []PagePointer) PagePointer {
	data := make([]byte, list.arena.pageSize)
	binary.LittleEndian.PutUint64(data[freeListNext:], uint64(next))
	binary.LittleEndian.PutUint32(data[freeListCount:], uint32(len(pages)))
	binary.LittleEndian.PutUint64(data[freeListVersion:], version)
	for i, page := range pages {
		binary.LittleEndian.PutUint64(data[freeListHeader+i*8:], uint64(page))
	}
	return list.arena.CreatePage(data)
}

func (list *FreeList) setNext(node, next PagePointer) error {
	data := make([]byte, list.arena.pageSize)
	copy(data, list.arena.Page(node))
	binary.LittleEndian.PutUint64(data[freeListNext:], uint64(next))
	return list.arena.UpdatePage(node, data)
}

// setReadPos advances the head cursor copy-on-write: a Pager Snapshot
// shallow-copies the write buffer, so mutating a page in place would corrupt
// a snapshot taken before this pop.
func (list *FreeList) setReadPos(node PagePointer, readPos uint32) {
	data := make([]byte, list.arena.pageSize)
	copy(data, list.arena.Page(node))
	binary.LittleEndian.PutUint32(data[freeListRead:], readPos)
	list.arena.pageUpdates[node] = data
}

func (list *FreeList) nodeCapacity() int {
	return (list.arena.pageSize - freeListHeader) / 8
}

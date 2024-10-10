package storage

type BTree struct {
	root   BNodePointer
	get    func(BNodePointer) *BNode
	create func(*BNode) BNodePointer
	delete func(BNodePointer)
}

package storage

type BTree struct {
	root   BNode
	get    func(BNodePointer) *BNode
	create func(*BNode) BNodePointer
	delete func(BNodePointer)
}

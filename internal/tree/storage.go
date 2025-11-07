package tree

type Storage interface {
	GetRoot() BTreeRootPointer       // get root node pointer
	SaveRoot(BTreeRootPointer) error // save root node pointer

	Get(BNodePointer) *BNode    // dereference a pointer
	Create(*BNode) BNodePointer // allocate a new node
	Delete(BNodePointer)        // deallocate a node
}

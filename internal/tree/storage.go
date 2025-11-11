package tree

type SnapshotID uint64

type Storage interface {
	GetRoot() BTreeRootPointer       // get root node pointer
	SaveRoot(BTreeRootPointer) error // save root node pointer to the storage

	Get(BNodePointer) *BNode    // dereference a pointer
	Create(*BNode) BNodePointer // allocate a new node
	Delete(BNodePointer)        // deallocate a node
	Flush() error               // flush all changes to the storage

	Snapshot() SnapshotID
	Restore(id SnapshotID)
}

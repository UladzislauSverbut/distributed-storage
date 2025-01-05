package kv

type GetRequest struct {
	Key []byte
}

type GetResponse struct {
	Value []byte
}

type SetRequest struct {
	Key   []byte
	Value []byte
}

type SetResponse struct {
	Added    bool
	Updated  bool
	OldValue []byte
}

type DeleteRequest struct {
	Key []byte
}

type DeleteResponse struct {
	OldValue []byte
}

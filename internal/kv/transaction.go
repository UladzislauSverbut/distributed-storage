package kv

type Transaction struct {
}

func (transaction *Transaction) Begin()  {}
func (transaction *Transaction) Commit() {}
func (transaction *Transaction) Abort()  {}

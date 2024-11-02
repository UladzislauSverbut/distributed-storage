package main

import (
	"distributed-storage/internal/kv"
	"log"
)

func main() {
	db, err := kv.New()

	if err != nil {
		log.Fatal(err)
	}

	db.Set([]byte("abc"), []byte("value"))

	// value, _ := db.Get([]byte("abc"))

	// fmt.Print(value)

}

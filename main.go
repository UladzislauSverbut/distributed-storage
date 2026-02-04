package main

import (
	"distributed-storage/internal/db"
	"distributed-storage/internal/vals"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

func main() {
	dbConfig := db.DatabaseConfig{
		Directory: "./",
	}

	database, err := db.NewDatabase(&dbConfig)

	processError(err)

	tableConfig := db.TableSchema{
		Name:             "users1",
		ColumnNames:      []string{"userId", "username", "email"},
		PrimaryIndex:     []string{"userId"},
		SecondaryIndexes: [][]string{{"username", "email"}},
		ColumnTypes:      map[string]vals.ValueType{"userId": vals.TYPE_INT64, "username": vals.TYPE_STRING, "email": vals.TYPE_STRING},
	}

	wg := sync.WaitGroup{}

	database.StartTransaction(func(tx *db.Transaction) {
		_, err := tx.CreateTable(&tableConfig)
		processError(err)
	})

	for idx := range 1000 {
		wg.Add(1)
		retries := 0

		go func() {
			for {
				tx, err := db.NewTransaction(database)
				processError(err)

				table, err := tx.Table(tableConfig.Name)
				processError(err)

				startTime := time.Now()
				err = table.Insert(
					vals.NewObject().
						Set("userId", vals.NewInt(int64(idx))).
						Set("username", vals.NewString("test username"+strconv.Itoa(idx))).
						Set("email", vals.NewString("test emailtest"+strconv.Itoa(idx))),
				)

				fmt.Printf("Took time %s to finish insert %d\n", time.Since(startTime), idx)

				if err == nil {
					err = tx.Commit()
				}

				fmt.Printf("Took time %s to finish commit %d\n", time.Since(startTime), idx)

				if err == nil {
					wg.Done()
					fmt.Printf("Took time %s to finish transaction %d\n", time.Since(startTime), idx)
					return
				}

				if err != db.ErrTransactionConflict {
					tx.Rollback()
					wg.Done()
					// log.Printf("Failed to commit transaction %d: %v\n", idx, err)
					return
				}

				retries++
				time.Sleep(time.Duration(time.Duration(rand.Intn(10)).Milliseconds()))
			}
		}()
	}

	wg.Wait()

	// database.StartTransaction(func(tx *db.Transaction) {
	// 	table, err := tx.Table(tableConfig.Name)

	// 	processError(err)

	// 	for idx := range 300000 {
	// 		res, err := table.Get(
	// 			vals.NewObject().
	// 				Set("userId", vals.NewInt(int64(idx))),
	// 		)

	// 		processError(err)

	// 		if res != nil {
	// 			username := res.Get("username").(*vals.StringValue)

	// 			log.Printf("Get result: %s\n", username.Value())
	// 		} else {
	// 			log.Printf("Failed to get username from result %d", idx)
	// 		}

	// 	}
	// })

	// table := database.Get("users2")

	processError(err)

	// for idx := range 1 {
	// 	err := table.Delete(
	// 		db.NewObject().
	// 			Set("userId", db.NewIntValue(int64(5000+idx))),
	// 	)

	// 	processError(err)
	// }

	// idx := 4

	// res, err := table.Find(
	// 	db.NewObject().
	// 		Set("username", db.NewStringValue("test username"+strconv.Itoa(idx))),
	// 	// Set("email", db.NewStringValue("test emailtest")),
	// )

	// processError(err)

	// if res != nil {

	// 	for _, record := range res {
	// 		username := record.Get("username").(*db.StringValue)
	// 		email := record.Get("email").(*db.StringValue)

	// 		log.Printf("Get result: %s and %s\n", username.Value(), email.Value())
	// 	}
	// } else {

	// 	log.Printf("Failed to get username from result %d", idx)
	// }

}

func processError(err error) {
	if err != nil {
		log.Printf("Error: %v\n", err)
	}
}

package main

import (
	"distributed-storage/internal/db"
	"log"
	"strconv"
)

func main() {
	dbConfig := db.DatabaseConfig{
		Directory: "./",
	}

	database, err := db.NewDatabase(&dbConfig)

	processError(err)

	tableConfig := db.TableSchema{
		Name:             "users2",
		ColumnNames:      []string{"userId", "username", "email"},
		PrimaryIndex:     []string{"userId"},
		SecondaryIndexes: [][]string{{"username", "email"}},
		ColumnTypes:      map[string]db.ValueType{"userId": db.VALUE_TYPE_INT64, "username": db.VALUE_TYPE_STRING, "email": db.VALUE_TYPE_STRING},
	}

	table, err := database.Create(&tableConfig)
	// table := database.Get("users2")

	processError(err)

	for idx := range 10000 {
		err = table.Insert(
			db.NewObject().
				Set("userId", db.NewIntValue(int64(idx))).
				Set("username", db.NewStringValue("test username"+strconv.Itoa(idx))).
				Set("email", db.NewStringValue("test emailtest"+strconv.Itoa(idx))),
		)

		if err != nil {
			log.Print(idx)
			processError(err)
		}
	}

	for idx := range 10000 {
		res, err := table.Get(
			db.NewObject().
				Set("userId", db.NewIntValue(int64(idx))).
				Set("username", db.NewStringValue("test username"+strconv.Itoa(idx%10))).
				Set("email", db.NewStringValue("test emailtest")),
		)

		processError(err)

		if res != nil {
			username := res.Get("username").(*db.StringValue)

			log.Printf("Get result: %s\n", username.Value())
		} else {

			log.Printf("Failed to get username from result %d", idx)
		}
	}

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

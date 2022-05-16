package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"sync"
	"sync/atomic"
)

var DB *sqlx.DB
var TotalCommit int32

func updateRow() {
	query := "UPDATE tests SET value = ? WHERE id = 1"
	query1 := "SELECT value FROM tests WHERE id = 1"
	var value int
	err := DB.Get(&value, query1)
	if err != nil {
		fmt.Println(err)
		return
	}

	value += 1
	_, err = DB.Exec(query, &value)
	if err != nil {
		fmt.Println(err)
	}
	atomic.AddInt32(&TotalCommit, 1)
}

func updateRowNoLock() {
	tx, err := DB.Begin()
	defer tx.Rollback()

	if err != nil {
		fmt.Printf("error begining txn %s", err.Error())
		return
	}

	query := "SELECT value FROM tests WHERE id = 1"
	var value int
	err = tx.QueryRow(query).Scan(&value)
	if err != nil {
		fmt.Printf("error getting value %s", err.Error())
		return
	}

	value += 1
	query = "UPDATE tests SET value = ? WHERE id = 1"
	_, err = tx.Exec(query, value)
	if err != nil {
		fmt.Printf("error updating value %s", err.Error())
		return
	}

	err = tx.Commit()
	if err != nil {
		fmt.Printf("error committing record %s", err.Error())
	}

	atomic.AddInt32(&TotalCommit, 1)
	return
}

func updateRowPessimisticLock() {
	tx, err := DB.Begin()
	defer tx.Rollback()

	if err != nil {
		fmt.Printf("error begining txn %s", err.Error())
		return
	}

	query := "SELECT value FROM tests WHERE id = 1 FOR UPDATE"
	var value int
	err = tx.QueryRow(query).Scan(&value)
	if err != nil {
		fmt.Printf("error getting value %s", err.Error())
		return
	}

	value += 1
	query = "UPDATE tests SET value = ? WHERE id = 1"
	_, err = tx.Exec(query, value)
	if err != nil {
		fmt.Printf("error updating value %s", err.Error())
		return
	}

	err = tx.Commit()
	if err != nil {
		fmt.Printf("error committing record %s", err.Error())
	}

	atomic.AddInt32(&TotalCommit, 1)
	return
}

func updateRowOptimisticLock() {
	tx, err := DB.Begin()
	defer tx.Rollback()

	if err != nil {
		fmt.Printf("error begining txn %s", err.Error())
		return
	}

	query := "SELECT value FROM tests WHERE id = 1 LOCK IN SHARE MODE"
	var value int
	err = tx.QueryRow(query).Scan(&value)
	if err != nil {
		fmt.Printf("error getting value %s", err.Error())
		return
	}

	value += 1
	query = "UPDATE tests SET value = ? WHERE id = 1"
	_, err = tx.Exec(query, value)
	if err != nil {
		fmt.Printf("error updating value %s", err.Error())
		return
	}

	err = tx.Commit()
	if err != nil {
		fmt.Printf("error committing record %s", err.Error())
	}
	atomic.AddInt32(&TotalCommit, 1)
	return
}

func main() {
	DB, _ = sqlx.Connect("mysql", "root:1@(localhost:3306)/test_locking")
	DB.SetMaxOpenConns(100)
	DB.SetMaxIdleConns(50)
	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			//updateRow() Total commit 10000 but result only 3 => race condition
			//updateRowNoLock() total commit 10000 but result only 101  => race condition
			//updateRowPessimisticLock() total commit 10000, result 10000 => no race conditions
			//updateRowOptimisticLock() total commit 102, result 102 => no race conditions but some transactions not done
		}()
	}
	wg.Wait()
	fmt.Println("Total commits", TotalCommit)
}

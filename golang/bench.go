package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	host         = "10.2.8.2"
	port         = "4501"
	user         = "root"
	password     = ""
	database     = "test"
	numTables    = 10
	sizeTable    = 100000
	numThreads   = 11000
	numIdle      = 10000
	numQueries   = 10
	queryPeriod  = 5 * time.Second
	reportPeriod = 10 * time.Second
)

var queries = []func(*sql.DB, *sync.WaitGroup){
	pointGet,
}

var connectionCount int32

func pointGet(db *sql.DB, wg *sync.WaitGroup) {
	randomID := rand.Intn(sizeTable)
	randomTable := rand.Intn(numTables) + 1
	query := fmt.Sprintf("SELECT * FROM sbtest%d WHERE id = ?", randomTable)
	db.Query(query, randomID)
}

func executeQueries(db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		queryFunc := queries[rand.Intn(len(queries))]
		queryFunc(db, wg)
	}
}

func idle(db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		time.Sleep(1 * time.Second)
		x := rand.Intn(4839280)
		db.Exec(fmt.Sprintf("select %d", x))
	}
}

func withCount(ctx context.Context, db *sql.DB, wg *sync.WaitGroup, task func(db *sql.DB, wg *sync.WaitGroup)) {
	atomic.AddInt32(&connectionCount, 1)
	defer atomic.AddInt32(&connectionCount, -1)
	task(db, wg)
}

func withDB(ctx context.Context, wg *sync.WaitGroup, task func(db *sql.DB, wg *sync.WaitGroup)) {
	atomic.AddInt32(&connectionCount, 1)
	defer atomic.AddInt32(&connectionCount, -1)
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, database))
	db.SetMaxIdleConns(math.MaxInt32)
	db.SetMaxOpenConns(math.MaxInt32)
	db.SetConnMaxIdleTime(-1)
	db.SetConnMaxLifetime(-1)
	if err != nil {
		fmt.Println("Error connecting to database:", err)
		return
	}
	defer db.Close()
	task(db, wg)
}

func reportConnections() {
	for {
		time.Sleep(reportPeriod)
		count := atomic.LoadInt32(&connectionCount)
		fmt.Printf("Number of running connections: %d\n", count)
	}
}

func main() {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, database))
	if err != nil {
		fmt.Println("Error connecting to database:", err)
		return
	}
	defer db.Close()
	db.SetMaxIdleConns(math.MaxInt32)
	db.SetMaxOpenConns(math.MaxInt32)

	var wg sync.WaitGroup

	// Start idle connections
	for i := 0; i < numIdle; i++ {
		wg.Add(1)
		go withCount(context.Background(), db, &wg, idle)
	}

	// Start threads for executing queries
	for i := 0; i < numThreads-numIdle; i++ {
		wg.Add(1)
		go withCount(context.Background(), db, &wg, executeQueries)
	}

	// Start goroutine to report connections
	go reportConnections()

	// Wait for all threads to finish
	wg.Wait()
}

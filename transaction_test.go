package sqlpro

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
)

func FailsWithREADMutexTestConcurrency(t *testing.T) {
	var err error

	db1, err := db.Begin()
	if err != nil {
		t.Error(err)
		return
	}

	wg := sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			db2, err := db.Begin()
			if err != nil {
				t.Error(errors.Wrap(err, "BEGIN failed"))
				return
			}

			err = saveRow(db2, 10)
			if err != nil {
				t.Error(errors.Wrap(err, "INSERT failed"))
				return
			}

			db2.Commit()

			time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
			// time.Sleep(150 * time.Millisecond)
		}()
	}

	time.Sleep(50 * time.Millisecond)

	err = saveRow(db1, 11)
	if err != nil {
		t.Error(err)
		return
	}

	wg.Wait()

	db1.Commit()

}

func readRow(db *DB) error {
	rows := []*testRow{}
	return db.Query(&rows, "SELECT * FROM test LIMIT 1")
}

func saveRow(db *DB, i int) error {
	data := []*testRow{
		{
			B: "concurrency",
			C: fmt.Sprintf("concurrency %d", i),
			F: jsonStore{"Yo", "Mama"},
		},
	}
	return db.Insert("test", data)
}

func TestConcurrency(t *testing.T) {
	wg := sync.WaitGroup{}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			db2, err := db.Begin()
			if err != nil {
				t.Error(err)
				return
			}

			err = readRow(db2)
			if err != nil {
				t.Error(err)
				db2.Rollback()
				return
			}

			err = saveRow(db2, i)
			if err != nil {
				t.Error(err)
				db2.Rollback()
				return
			}

			// time.Sleep(2 * time.Second)
			db2.Commit()
		}(i)
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			db2, err := db.BeginRead()
			if err != nil {
				t.Error(err)
				return
			}

			err = readRow(db2)
			if err != nil {
				t.Error(err)
				db2.Rollback()
				return
			}

			// time.Sleep(2 * time.Second)
			db2.Commit()
		}(i)
	}

	wg.Wait()
}

func TestReadOnlyMode(t *testing.T) {

	db2, err := db.BeginRead()
	if err != nil {
		t.Error(err)
		return
	}

	err = readRow(db2)
	if err != nil {
		t.Error(err)
		db2.Rollback()
		return
	}

	err = db2.Insert("test", []*testRow{
		{
			B: "readonly",
			F: jsonStore{"no", "writes"},
		},
	})
	if err == nil {
		t.Error("Expected error trying to write when a transaction is not in write mode")
		db2.Rollback()
		return
	}

	db2.Commit()
}

func TestTwoConnections(t *testing.T) {

	db2, err := db.BeginRead()
	if err != nil {
		t.Error(err)
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		println("Getting TX")
		db3, err := db.Begin()
		if err != nil {
			db3.Rollback()
		} else {
			db3.Commit()
		}
		println("Got TX")
		wg.Done()

	}()

	wg.Wait()
	db2.Commit()

}

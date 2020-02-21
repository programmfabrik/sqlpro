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
		&testRow{
			B: "concurrency",
			C: fmt.Sprintf("concurrency %d", i),
			F: jsonStore{"Yo", "Mama"},
		},
	}
	return db.Insert("test", data)
}

func TestConcurrency(t *testing.T) {
	wg := sync.WaitGroup{}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			db2, err := db.Begin()
			if err != nil {
				t.Error(err)
				return
			}

			err = saveRow(db2, i)
			if err != nil {
				t.Error(err)
				return
			}

			time.Sleep(2 * time.Second)
			db2.Commit()
		}(i)
	}

	wg.Wait()

}

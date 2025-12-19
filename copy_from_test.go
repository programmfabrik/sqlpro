package sqlpro

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type row struct {
	Id    int    `db:"id,pk"`
	Name  string `db:"name"`
	Value int    `db:"value"`
}

func TestCopyFrom(t *testing.T) {
	// Replace with your PostgreSQL connection string

	db, err := Open(POSTGRES, "host=localhost port=5432 dbname=apitest password=egal sslmode=disable")
	if !assert.NoError(t, err) {
		return
	}
	defer db.Close()

	ctx := context.Background()

	// Sample data to copy
	rows := []row{
		{0, "Alice", 100},
		{0, "Bob", 200},
		{0, "Bob", 200},
		{0, "Charlie", 300},
	}

	err = db.ExecTX(ctx, func(ctx context.Context) error {

		tx := CtxTX(ctx)

		err = tx.ExecContext(ctx, `
		CREATE TEMP TABLE temp_example (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			value INTEGER
		)
	`)
		if err != nil {
			return err
		}

		return tx.InsertBulk("temp_example", rows)
	}, nil)
	if !assert.NoError(t, err) {
		return
	}
}

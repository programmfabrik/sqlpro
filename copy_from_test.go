package sqlpro

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type row struct {
	Id    int    `db:"id,pk,omitempty"`
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
		{0, "Charlie", 300},
		{0, "Dora", 400},
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

		// plain InsertBulk uses COPY FROM here (inside ExecTX, no read-back)
		err = tx.InsertBulk("temp_example", rows)
		if err != nil {
			return err
		}
		for i := range rows {
			assert.Equal(t, 0, rows[i].Id, "InsertBulk must not read ids back")
		}

		// InsertBulkReadbackIdsContext runs INSERT ... RETURNING and writes
		// the generated keys back in row order
		more := []*row{
			{0, "Emil", 500},
			{0, "Frida", 600},
		}
		err = tx.InsertBulkReadbackIdsContext(ctx, "temp_example", more)
		if err != nil {
			return err
		}
		for i := range more {
			if !assert.Greater(t, more[i].Id, 0, "row %d has no id", i) {
				return nil
			}
			if i > 0 {
				assert.Greater(t, more[i].Id, more[i-1].Id)
			}
		}

		var total int64
		err = tx.Query(&total, `SELECT count(*) FROM temp_example`)
		if err != nil {
			return err
		}
		assert.Equal(t, int64(len(rows)+len(more)), total)
		return nil
	}, nil)
	if !assert.NoError(t, err) {
		return
	}
}

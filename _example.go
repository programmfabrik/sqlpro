package export

import (
	"database/sql"

	"github.com/programmfabrik/sqlpro"
)

// Query[Context]
// Update[Context]
// Insert[Context]
// InsertBulk
// Save[Context]
// Exec[Context]

type User struct {
	Id      int64    `db:"id,pk"`
	Name    string   `db:"name"`
	Emails  []string `db:"emails,json"`
	Country string   `db:"country,readonly"`
}

func ExampleStruct() {

	u := User{
		Name:   "Max Mustermann",
		Emails: []string{"max@max.de", "muser.de"},
	}

	db, _ := sqlpro.Open("sqlite3", "test.db")
	_ = db.Save("user", u) // checks the "pk" for 0

	tx, _ := db.BeginRead()

	users := []User{}

	err := tx.Query(&users, "SELECT * FROM user")
	if err == sqlpro.ErrQueryReturnedZeroRows {

	}

	err = tx.Log().Query(&users, "SELECT name FROM user WHERE @ IN ? AND id = ?", "id", []int{1, 2, 4}, 5)
	if err == sqlpro.ErrQueryReturnedZeroRows {

	}

	err = tx.Log().Query(&u, "SELECT name FROM user WHERE @ IN ? AND id = ? LIMIT 1", "id", []int{1, 2, 4}, 5)

	var i int
	err = tx.Log().Query(&i, "SELECT COUNT(name) FROM user WHERE @ IN ? AND id = ? LIMIT 1", "id", []int{1, 2, 4}, 5)

	_ = tx.InsertBulk("user", users)

	tx.Exec("SHOW variable")

	rows := sql.Rows
	err = tx.Log().Query(&rows, "SELECT name FROM user WHERE @ IN ? AND id = ? LIMIT 1", "id", []int{1, 2, 4}, 5)

}

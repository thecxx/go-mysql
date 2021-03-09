package mysql

import (
	"testing"
)

// New database.
func newDatabase() (*Database, error) {
	return NewDatabase("127.0.0.1:3306", "test", "root", "123456")
}

func TestDatabase_Query(t *testing.T) {
	d, err := newDatabase()
	if err != nil {
		t.Errorf("New database failed, err=%s\n", err.Error())
		return
	}
	result, err := d.Query("SELECT * FROM `users` LIMIT 1")
	if err != nil {
		t.Errorf("Query failed, err=%s\n", err.Error())
		return
	}
	rows, err := result.Rows()
	if err != nil {
		t.Errorf("Get rows failed, err=%s\n", err.Error())
		return
	}
	if rows == nil {
		t.Errorf("Invalid rows\n")
		return
	}
}

func TestStatement_Query(t *testing.T) {
	d, err := newDatabase()
	if err != nil {
		t.Errorf("New database failed, err=%s\n", err.Error())
		return
	}

	stmt, err := d.Prepare("SELECT * FROM `users` WHERE `uid` = ?")
	if err != nil {
		t.Errorf("Prepare failed, err=%s\n", err.Error())
		return
	}

	result, err := stmt.Query(5)
	if err != nil {
		t.Errorf("Query failed, err=%s\n", err.Error())
		return
	}

	defer stmt.Close()

	rows, err := result.Rows()
	if err != nil {
		t.Errorf("Get rows failed, err=%s\n", err.Error())
		return
	}
	if rows == nil {
		t.Errorf("Invalid rows\n")
		return
	}

	t.Logf("%#v\n", rows)
}

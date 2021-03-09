package mysql

import (
	"testing"
)

func TestClient_Query(t *testing.T) {

	conf := NewDefaultConfig("127.0.0.1:3306", "test", "root", "123456", true)

	c, err := NewClient(conf)
	if err != nil {
		t.Errorf("NewClient failed, err = %s\n", err.Error())
		return
	}
	err = c.SetReplica(conf)
	if err != nil {
		t.Errorf("SetReplica failed, err = %s\n", err.Error())
		return
	}
	result, err := c.Query("SELECT * FROM users LIMIT 1")
	if err != nil {
		t.Errorf("Query failed, err = %s\n", err.Error())
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

	t.Logf("%#v\n", rows)

}

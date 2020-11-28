package logdb

import "testing"

func TestDB(t *testing.T) {
	db, err := NewDatabase("tmp/logs")
	if err != nil {
		t.Error(err)
	}
	err = db.CreateQueue("first")
	if err != nil {
		t.Error(err)
	}
	db.Append("first", "alpha", map[string]string{"name": "john"})
	db2, err := NewDatabase("tmp/logs")
	if err != nil {
		t.Error(err)
	}
	_ = db2
}

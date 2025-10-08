package ha_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/litesql/go-ha"
)

func TestConnector(t *testing.T) {
	pub := new(fakePublisher)
	connector, err := ha.NewConnector("file:/test.db?vfs=memdb", ha.WithCDCPublisher(pub))
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	query := "CREATE TABLE users(ID INTEGER PRIMARY KEY, name TEXT)"
	_, err = db.ExecContext(context.TODO(), query)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	if len(pub.changes) != 1 {
		t.Errorf("want 1 changes, but got %d", len(pub.changes))
	}
	if pub.changes[0].Operation != "SQL" {
		t.Errorf("expect SQL operation, but got %q", pub.changes[0].Operation)
	}
	if pub.changes[0].SQL != query {
		t.Errorf("want %q, got %q", query, pub.changes[0].SQL)
	}
	_, err = db.ExecContext(context.TODO(), "INSERT INTO users(name) VALUES(?)", "test")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}
	if len(pub.changes) != 1 {
		t.Errorf("want 1 changes, but got %d", len(pub.changes))
	}
	if pub.changes[0].Operation != "INSERT" {
		t.Errorf("expect INSERT operation, but got %q", pub.changes[0].Operation)
	}
}

type fakePublisher struct {
	err     error
	changes []ha.Change
}

func (f *fakePublisher) Publish(cs *ha.ChangeSet) error {
	f.changes = cs.Changes
	return f.err
}

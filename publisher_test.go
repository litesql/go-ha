package ha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
)

type relayTestRecord struct {
	id        int64
	subject   string
	timestamp int64
	changeset []byte
}

type relayTestDB struct {
	mu          sync.Mutex
	records     []relayTestRecord
	queryCount  int
	deleteCount int
}

type relayTestConnector struct {
	db *relayTestDB
}

func (c relayTestConnector) Connect(context.Context) (driver.Conn, error) {
	return relayTestConn{db: c.db}, nil
}

func (relayTestConnector) Driver() driver.Driver {
	return relayTestDriver{}
}

type relayTestDriver struct{}

func (relayTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("relay test driver must be opened through its connector")
}

type relayTestConn struct {
	db *relayTestDB
}

func (relayTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepared statements are not supported")
}

func (relayTestConn) Close() error { return nil }

func (relayTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (c relayTestConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if !strings.HasPrefix(query, "SELECT rowid, changeset FROM `"+OutboxTable+"`") ||
		!strings.HasSuffix(query, fmt.Sprintf("LIMIT %d", outboxRelayBatchSize)) {
		return nil, fmt.Errorf("unexpected relay query %q", query)
	}
	if len(args) != 1 {
		return nil, fmt.Errorf("unexpected relay query argument count: %d", len(args))
	}
	subject, ok := args[0].Value.(string)
	if !ok {
		return nil, errors.New("invalid subject argument")
	}

	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	c.db.queryCount++

	records := make([]relayTestRecord, 0)
	for _, record := range c.db.records {
		if record.subject == subject {
			records = append(records, record)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].timestamp == records[j].timestamp {
			return records[i].id < records[j].id
		}
		return records[i].timestamp < records[j].timestamp
	})
	if len(records) > outboxRelayBatchSize {
		records = records[:outboxRelayBatchSize]
	}

	rows := make([][]driver.Value, len(records))
	for i, record := range records {
		rows[i] = []driver.Value{record.id, record.changeset}
	}
	return &relayTestRows{rows: rows}, nil
}

func (c relayTestConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if !strings.HasPrefix(query, "DELETE FROM "+OutboxTable+" WHERE rowid IN (") {
		return nil, errors.New("unexpected relay delete")
	}

	ids := make(map[int64]struct{}, len(args))
	for _, arg := range args {
		id, ok := arg.Value.(int64)
		if !ok {
			return nil, errors.New("invalid rowid argument")
		}
		ids[id] = struct{}{}
	}

	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	c.db.deleteCount++
	remaining := c.db.records[:0]
	var deleted int64
	for _, record := range c.db.records {
		if _, ok := ids[record.id]; ok {
			deleted++
			continue
		}
		remaining = append(remaining, record)
	}
	c.db.records = remaining
	return relayTestResult(deleted), nil
}

type relayTestRows struct {
	rows [][]driver.Value
	next int
}

func (*relayTestRows) Columns() []string { return []string{"rowid", "changeset"} }

func (r *relayTestRows) Close() error { return nil }

func (r *relayTestRows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

type relayTestResult int64

func (r relayTestResult) LastInsertId() (int64, error) { return 0, nil }

func (r relayTestResult) RowsAffected() (int64, error) { return int64(r), nil }

type relayTestPublisher struct {
	nodes []string
}

func (p *relayTestPublisher) Publish(_ context.Context, cs *ChangeSet) error {
	p.nodes = append(p.nodes, cs.Node)
	return nil
}

func (*relayTestPublisher) Sequence() uint64 { return 0 }

func relayTestChangeSet(t *testing.T, node string) []byte {
	t.Helper()
	data, err := json.Marshal(&ChangeSet{Node: node})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestOutboxRelayPublishesBatchInOrder(t *testing.T) {
	state := &relayTestDB{records: []relayTestRecord{
		{id: 3, subject: "replication", timestamp: 3, changeset: relayTestChangeSet(t, "third")},
		{id: 1, subject: "replication", timestamp: 1, changeset: relayTestChangeSet(t, "first")},
		{id: 2, subject: "replication", timestamp: 2, changeset: relayTestChangeSet(t, "second")},
		{id: 4, subject: "other", timestamp: 4, changeset: relayTestChangeSet(t, "other")},
	}}
	db := sql.OpenDB(relayTestConnector{db: state})
	defer db.Close()

	publisher := &relayTestPublisher{}
	outbox := &OutboxPublisher{publisher: publisher, subject: "replication", localDB: db}
	outbox.relay()

	if want := []string{"first", "second", "third"}; !reflect.DeepEqual(publisher.nodes, want) {
		t.Fatalf("published nodes = %v, want %v", publisher.nodes, want)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.queryCount != 1 || state.deleteCount != 1 {
		t.Fatalf("database operations = query:%d delete:%d, want one of each", state.queryCount, state.deleteCount)
	}
	if len(state.records) != 1 || state.records[0].subject != "other" {
		t.Fatalf("remaining outbox records = %+v, want only the other subject", state.records)
	}
}

func TestOutboxRelayLimitsBatchSize(t *testing.T) {
	state := &relayTestDB{}
	for i := 1; i <= outboxRelayBatchSize+1; i++ {
		state.records = append(state.records, relayTestRecord{
			id:        int64(i),
			subject:   "replication",
			timestamp: int64(i),
			changeset: relayTestChangeSet(t, fmt.Sprintf("node-%d", i)),
		})
	}
	db := sql.OpenDB(relayTestConnector{db: state})
	defer db.Close()

	publisher := &relayTestPublisher{}
	outbox := &OutboxPublisher{publisher: publisher, subject: "replication", localDB: db}
	outbox.relay()

	if got := len(publisher.nodes); got != outboxRelayBatchSize {
		t.Fatalf("published %d records, want batch size %d", got, outboxRelayBatchSize)
	}
	state.mu.Lock()
	if len(state.records) != 1 || state.records[0].id != int64(outboxRelayBatchSize+1) {
		state.mu.Unlock()
		t.Fatalf("remaining records = %+v, want the record after the first batch", state.records)
	}
	state.mu.Unlock()

	outbox.relay()
	if got := len(publisher.nodes); got != outboxRelayBatchSize+1 {
		t.Fatalf("published %d records after second relay, want %d", got, outboxRelayBatchSize+1)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.records) != 0 || state.queryCount != 2 || state.deleteCount != 2 {
		t.Fatalf("records = %d, queries = %d, deletes = %d; want all records drained in two batches", len(state.records), state.queryCount, state.deleteCount)
	}
}

func TestOutboxRelayDeletesOnlyPublishedPrefix(t *testing.T) {
	state := &relayTestDB{records: []relayTestRecord{
		{id: 1, subject: "replication", timestamp: 1, changeset: relayTestChangeSet(t, "first")},
		{id: 2, subject: "replication", timestamp: 2, changeset: []byte("{")},
		{id: 3, subject: "replication", timestamp: 3, changeset: relayTestChangeSet(t, "third")},
	}}
	db := sql.OpenDB(relayTestConnector{db: state})
	defer db.Close()

	publisher := &relayTestPublisher{}
	outbox := &OutboxPublisher{publisher: publisher, subject: "replication", localDB: db}
	outbox.relay()

	if want := []string{"first"}; !reflect.DeepEqual(publisher.nodes, want) {
		t.Fatalf("published nodes = %v, want %v", publisher.nodes, want)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleteCount != 1 || len(state.records) != 2 || state.records[0].id != 2 || state.records[1].id != 3 {
		t.Fatalf("remaining records = %+v after %d deletes, want failed record and later records intact", state.records, state.deleteCount)
	}
}

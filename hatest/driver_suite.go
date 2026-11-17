package hatest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/litesql/go-ha"
)

type ConnectorFactory func(name string, opts ...ha.Option) (*ha.Connector, error)

func RunDriverSuite(t *testing.T, newConnector ConnectorFactory, memDSN func(t *testing.T, name string) string) {
	if memDSN == nil {
		memDSN = func(_ *testing.T, name string) string {
			return fmt.Sprintf("file:/%s?vfs=memdb", name)
		}
	}

	t.Run("Connector", func(t *testing.T) {
		pub := new(fakePublisher)
		connector, err := newConnector(memDSN(t, "test.db"), ha.WithReplicationPublisher(pub))
		if err != nil {
			t.Fatal(err)
		}
		defer connector.Close()

		db := sql.OpenDB(connector)
		defer db.Close()

		_, err = db.ExecContext(context.TODO(), "CREATE TABLE users(ID INTEGER PRIMARY KEY, name TEXT); CREATE TABLE users2(ID INTEGER PRIMARY KEY, name TEXT)")
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}
		if len(pub.changes) != 1 {
			t.Errorf("want 1 changes, but got %d", len(pub.changes))
		}
		if pub.changes[0].Operation != "SQL" {
			t.Errorf("expect SQL operation, but got %q", pub.changes[0].Operation)
		}
		want := "CREATE TABLE IF NOT EXISTS users (ID INTEGER PRIMARY KEY, name TEXT);CREATE TABLE IF NOT EXISTS users2 (ID INTEGER PRIMARY KEY, name TEXT)"
		got := strings.ReplaceAll(pub.changes[0].Command, "\"", "")
		if strings.EqualFold(got, want) {
			t.Errorf("want %q, got %q", want, got)
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
	})

	t.Run("TwoPhaseCommitWrapsLocalWrites", func(t *testing.T) {
		pub := new(fakeTwoPhasePublisher)
		connector, err := newConnector(memDSN(t, "two-phase-commit.db"), ha.WithReplicationPublisher(pub))
		if err != nil {
			t.Fatal(err)
		}
		defer connector.Close()
		db := sql.OpenDB(connector)
		defer db.Close()

		if _, err := db.ExecContext(context.Background(), "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(context.Background(), "INSERT INTO users(name) VALUES(?)", "autocommit"); err != nil {
			t.Fatal(err)
		}

		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(context.Background(), "INSERT INTO users(name) VALUES(?)", "explicit"); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		rows, err := db.QueryContext(context.Background(), "INSERT INTO users(name) VALUES(?) RETURNING name", "returning")
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			t.Fatalf("expected RETURNING row, got error %v", rows.Err())
		}
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		if name != "returning" {
			t.Fatalf("unexpected RETURNING value %q", name)
		}
		if rows.Next() {
			t.Fatal("expected one RETURNING row")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		rows, err = db.QueryContext(context.Background(), "INSERT INTO users(name) VALUES(?) RETURNING name", "closed early")
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			t.Fatalf("expected RETURNING row before early close, got error %v", rows.Err())
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}

		var count int
		if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM users").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 4 {
			t.Fatalf("want 4 committed rows, got %d", count)
		}
		if pub.prepares != 5 || pub.commits != 5 {
			t.Fatalf("want 5 prepare/commit cycles, got %d/%d", pub.prepares, pub.commits)
		}
		if pub.legacyPublishes != 0 {
			t.Fatalf("commit hook used legacy Publish %d times", pub.legacyPublishes)
		}
		if pub.recoveries != 5 {
			t.Fatalf("want recovery before each of 5 local transactions, got %d", pub.recoveries)
		}
		if _, err := db.ExecContext(context.Background(), "BEGIN"); err == nil {
			t.Fatal("expected raw BEGIN to be rejected when 2PC is enabled")
		}
		if _, err := db.ExecContext(context.Background(), "SAVEPOINT unmanaged"); err == nil {
			t.Fatal("expected raw SAVEPOINT to be rejected when 2PC is enabled")
		}
	})

	t.Run("TwoPhaseCommitKeepsLocalDecisionOnRemoteFailure", func(t *testing.T) {
		pub := &fakeTwoPhasePublisher{failAt: 2}
		connector, err := newConnector(memDSN(t, "two-phase-pending.db"), ha.WithReplicationPublisher(pub))
		if err != nil {
			t.Fatal(err)
		}
		defer connector.Close()
		db := sql.OpenDB(connector)
		defer db.Close()

		if _, err := db.ExecContext(context.Background(), "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
			t.Fatal(err)
		}
		_, err = db.ExecContext(context.Background(), "INSERT INTO users(name) VALUES(?)", "committed locally")
		if !errors.Is(err, ha.ErrTwoPhaseCommitPending) {
			t.Fatalf("want pending remote commit error, got %v", err)
		}

		var count int
		if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM users").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("want local transaction committed once, got %d rows", count)
		}
		if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM ha_2pc_decisions WHERE transaction_id = ?", "fake-tx-2").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("want durable local decision row, got %d", count)
		}
	})

	t.Run("TwoPhaseCommitPublisherLifecycle", func(t *testing.T) {
		pub, err := ha.NewTwoPhaseCommitPublisher(nil, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		connector, err := newConnector(memDSN(t, "two-phase-publisher.db"), ha.WithReplicationPublisher(pub))
		if err != nil {
			t.Fatal(err)
		}
		defer connector.Close()
		db := sql.OpenDB(connector)
		defer db.Close()

		if _, err := db.ExecContext(context.Background(), "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(context.Background(), "INSERT INTO users(name) VALUES(?)", "actual publisher"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(context.Background(), `INSERT INTO ha_2pc_decisions(transaction_id, changeset, workers) VALUES(?, ?, ?)`, "pending", []byte(`{"transactionId":"pending"}`), []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(context.Background(), "INSERT INTO users(name) VALUES(?)", "after recovery"); err != nil {
			t.Fatal(err)
		}
		var pending int
		if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM ha_2pc_decisions").Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending != 0 {
			t.Fatalf("want successful transaction decisions cleared, got %d", pending)
		}
	})

	t.Run("TwoPhaseCommitRecoveryIsCalledBeforeNewWrites", func(t *testing.T) {
		pub := &recoveringPublisher{}
		connector, err := newConnector(memDSN(t, "two-phase-recovery-calls.db"), ha.WithReplicationPublisher(pub))
		if err != nil {
			t.Fatal(err)
		}
		defer connector.Close()
		db := sql.OpenDB(connector)
		defer db.Close()

		if _, err := db.ExecContext(context.Background(), "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
			t.Fatal(err)
		}
		pub.recoveries = 0
		if _, err := db.ExecContext(context.Background(), "INSERT INTO users(name) VALUES(?)", "after recovery"); err != nil {
			t.Fatal(err)
		}
		if pub.recoveries != 1 {
			t.Fatalf("want one recovery before the write, got %d", pub.recoveries)
		}
	})

	t.Run("TwoPhaseCommitRecoveryFailureBlocksNewWrites", func(t *testing.T) {
		pub := &recoveryFailurePublisher{err: errors.New("recovery unavailable")}
		connector, err := newConnector(memDSN(t, "two-phase-recovery-failure.db"), ha.WithReplicationPublisher(pub))
		if err != nil {
			t.Fatal(err)
		}
		defer connector.Close()
		db := sql.OpenDB(connector)
		defer db.Close()

		_, err = db.ExecContext(context.Background(), "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)")
		if !errors.Is(err, pub.err) {
			t.Fatalf("want recovery error, got %v", err)
		}
		if pub.recoveries != 1 {
			t.Fatalf("want recover to run once before blocking the write, got %d", pub.recoveries)
		}
	})

	t.Run("ChangeSetInterceptorLastWriterWins", func(t *testing.T) {
		lww := ha.NewLastWriterWinsInterceptor()
		pub := new(fakePublisher)
		connector, err := newConnector(memDSN(t, "interceptor-lww.db"),
			ha.WithName("node-b"),
			ha.WithReplicationPublisher(pub),
			ha.WithChangeSetInterceptor(lww),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer connector.Close()
		db := sql.OpenDB(connector)
		defer db.Close()

		if _, err := db.ExecContext(context.Background(), "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
			t.Fatal(err)
		}
		// Local write on node-b commits with current wall-clock timestamp (~now)
		if _, err := db.ExecContext(context.Background(), "INSERT INTO users(id, name) VALUES(1, 'LocalNewer')"); err != nil {
			t.Fatal(err)
		}

		// Older remote ChangeSet (ts=100) from node-a must be skipped by LWW!
		staleCS := ha.NewChangeSet("node-a", connector.ReplicationID())
		staleCS.Timestamp = 100
		staleCS.SetInterceptor(lww)
		staleCS.SetConnProvider(connectorHooksProvider{connector: connector})
		staleCS.Changes = []ha.Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "UPDATE",
			OldValues: []any{int64(1), "Old"},
			NewValues: []any{int64(1), "RemoteStale"},
			TsNs:      100,
		}}
		if err := staleCS.Apply(context.Background(), db); err != nil {
			t.Fatalf("apply stale ChangeSet: %v", err)
		}

		var name string
		if err := db.QueryRowContext(context.Background(), "SELECT name FROM users WHERE id = 1").Scan(&name); err != nil {
			t.Fatal(err)
		}
		if name != "LocalNewer" {
			t.Fatalf("expected newer local write to win, got %q", name)
		}

		// Newer remote ChangeSet (far future timestamp) must win and update row!
		futureTs := time.Now().Add(time.Hour).UnixNano()
		newerCS := ha.NewChangeSet("node-c", connector.ReplicationID())
		newerCS.Timestamp = futureTs
		newerCS.SetInterceptor(lww)
		newerCS.SetConnProvider(connectorHooksProvider{connector: connector})
		newerCS.Changes = []ha.Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "UPDATE",
			OldValues: []any{int64(1), "LocalNewer"},
			NewValues: []any{int64(1), "RemoteNewer"},
			TsNs:      futureTs,
		}}
		if err := newerCS.Apply(context.Background(), db); err != nil {
			t.Fatalf("apply newer ChangeSet: %v", err)
		}
		if err := db.QueryRowContext(context.Background(), "SELECT name FROM users WHERE id = 1").Scan(&name); err != nil {
			t.Fatal(err)
		}
		if name != "RemoteNewer" {
			t.Fatalf("expected newer remote write to win, got %q", name)
		}
	})
}

type recoveringPublisher struct {
	fakeTwoPhasePublisher
	recoveries int
}

func (p *recoveringPublisher) RecoverTwoPhaseCommits(context.Context) error {
	p.recoveries++
	return nil
}

type recoveryFailurePublisher struct {
	fakeTwoPhasePublisher
	err        error
	recoveries int
}

func (p *recoveryFailurePublisher) RecoverTwoPhaseCommits(context.Context) error {
	p.recoveries++
	return p.err
}

type fakePublisher struct {
	err      error
	changes  []ha.Change
	sequence uint64
}

func (f *fakePublisher) Publish(ctx context.Context, cs *ha.ChangeSet) error {
	f.changes = cs.Changes
	return f.err
}

func (f *fakePublisher) Sequence() uint64 {
	return f.sequence
}

type fakeTwoPhasePublisher struct {
	prepares        int
	commits         int
	aborts          int
	legacyPublishes int
	failAt          int
	localDB         *sql.DB
	recoveries      int
}

func (f *fakeTwoPhasePublisher) Publish(context.Context, *ha.ChangeSet) error {
	f.legacyPublishes++
	return nil
}

func (f *fakeTwoPhasePublisher) Sequence() uint64 {
	return 0
}

func (f *fakeTwoPhasePublisher) BindLocalDB(db *sql.DB) error {
	f.localDB = db
	_, err := db.ExecContext(context.Background(), `CREATE TABLE IF NOT EXISTS ha_2pc_decisions (
		transaction_id TEXT PRIMARY KEY,
		changeset BLOB NOT NULL,
		workers BLOB NOT NULL
	)`)
	return err
}

func (f *fakeTwoPhasePublisher) RecoverTwoPhaseCommits(context.Context) error {
	f.recoveries++
	return nil
}

func (f *fakeTwoPhasePublisher) PrepareTwoPhaseCommit(context.Context, *ha.ChangeSet) (ha.PreparedTwoPhaseCommit, error) {
	f.prepares++
	return &fakePreparedTwoPhaseCommit{publisher: f, transactionID: fmt.Sprintf("fake-tx-%d", f.prepares)}, nil
}

type fakePreparedTwoPhaseCommit struct {
	publisher     *fakeTwoPhasePublisher
	transactionID string
}

func (f *fakePreparedTwoPhaseCommit) RecordCommitDecision(ctx context.Context, write ha.TwoPhaseCommitDecisionWriter) error {
	return write(ctx, `INSERT INTO ha_2pc_decisions(transaction_id, changeset, workers) VALUES(?, ?, ?)`, f.transactionID, []byte("{}"), []byte("{}"))
}

func (f *fakePreparedTwoPhaseCommit) Commit(ctx context.Context) error {
	f.publisher.commits++
	if f.publisher.failAt == f.publisher.commits {
		return errors.New("worker commit unavailable")
	}
	_, err := f.publisher.localDB.ExecContext(ctx, "DELETE FROM ha_2pc_decisions WHERE transaction_id = ?", f.transactionID)
	if err != nil {
		return err
	}
	return nil
}

func (f *fakePreparedTwoPhaseCommit) Abort() error {
	f.publisher.aborts++
	return nil
}

func (f *fakePreparedTwoPhaseCommit) ResolveCommit(ctx context.Context) (bool, error) {
	return false, nil
}

func (f *fakePreparedTwoPhaseCommit) Close() {}

type connectorHooksProvider struct {
	connector *ha.Connector
}

func (p connectorHooksProvider) RegisterHooks(conn driver.Conn, _ *ha.Connector) (driver.Conn, error) {
	return conn, nil
}

func (p connectorHooksProvider) DisableHooks(conn *sql.Conn) error {
	return p.connector.DisableHooks(conn)
}

func (p connectorHooksProvider) EnableHooks(conn *sql.Conn) error {
	return p.connector.EnableHooks(conn)
}

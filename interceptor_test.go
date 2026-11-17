package ha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	rqlitesql "github.com/rqlite/sql"
)

func TestNoopAndFuncsInterceptors(t *testing.T) {
	db := openMemTestDB(t)
	execMemSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)

	noop := NewNoopInterceptor()
	cs := &ChangeSet{
		Node:      "node-a",
		Timestamp: 100,
		strategy:  defaultStrategy,
		Changes: []Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "INSERT",
			NewValues: []any{int64(1), "Alice"},
			TsNs:      100,
		}},
	}
	cs.SetInterceptor(noop)
	if err := cs.Apply(context.Background(), db); err != nil {
		t.Fatalf("Apply with NoopInterceptor failed: %v", err)
	}
	assertUserRow(t, db, 1, "Alice")

	var beforeCalled, afterCalled bool
	funcs := ChangeSetInterceptorFuncs{
		BeforeApplyFn: func(cs *ChangeSet, conn *sql.Conn) (bool, error) {
			beforeCalled = conn != nil && len(cs.Changes) == 1
			return true, nil // skip
		},
		AfterApplyFn: func(cs *ChangeSet, conn *sql.Conn, applyErr error) error {
			afterCalled = conn != nil && applyErr == nil
			return nil
		},
	}
	csSkipped := &ChangeSet{
		Node:      "node-b",
		Timestamp: 200,
		strategy:  defaultStrategy,
		Changes: []Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "UPDATE",
			OldValues: []any{int64(1), "Alice"},
			NewValues: []any{int64(1), "Skipped"},
			TsNs:      200,
		}},
	}
	csSkipped.SetInterceptor(funcs)
	if err := csSkipped.Apply(context.Background(), db); err != nil {
		t.Fatalf("Apply with ChangeSetInterceptorFuncs failed: %v", err)
	}
	if !beforeCalled || !afterCalled {
		t.Fatalf("expected BeforeApply and AfterApply to run, got before=%v after=%v", beforeCalled, afterCalled)
	}
	assertUserRow(t, db, 1, "Alice")
}

func TestCompositeInterceptorOrderingAndShortCircuit(t *testing.T) {
	db := openMemTestDB(t)
	execMemSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)

	var order []string
	first := ChangeSetInterceptorFuncs{
		BeforeApplyFn: func(*ChangeSet, *sql.Conn) (bool, error) {
			order = append(order, "before-1")
			return false, nil
		},
		AfterApplyFn: func(*ChangeSet, *sql.Conn, error) error {
			order = append(order, "after-1")
			return nil
		},
	}
	second := ChangeSetInterceptorFuncs{
		BeforeApplyFn: func(*ChangeSet, *sql.Conn) (bool, error) {
			order = append(order, "before-2")
			return false, nil
		},
		AfterApplyFn: func(*ChangeSet, *sql.Conn, error) error {
			order = append(order, "after-2")
			return nil
		},
	}

	chain := NewCompositeInterceptor(nil, first, second)
	cs := &ChangeSet{
		Node:     "node-a",
		strategy: defaultStrategy,
		Changes: []Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "INSERT",
			NewValues: []any{int64(1), "Alice"},
		}},
	}
	cs.SetInterceptor(chain)
	if err := cs.Apply(context.Background(), db); err != nil {
		t.Fatalf("CompositeInterceptor Apply failed: %v", err)
	}
	wantOrder := []string{"before-1", "before-2", "after-2", "after-1"}
	if !slices.Equal(order, wantOrder) {
		t.Fatalf("order = %v, want %v", order, wantOrder)
	}

	// Verify short-circuit when first interceptor returns skip=true: second's BeforeApply and AfterApply do not run.
	order = nil
	skipFirst := ChangeSetInterceptorFuncs{
		BeforeApplyFn: func(*ChangeSet, *sql.Conn) (bool, error) {
			order = append(order, "before-skip")
			return true, nil
		},
		AfterApplyFn: func(*ChangeSet, *sql.Conn, error) error {
			order = append(order, "after-skip")
			return nil
		},
	}
	chainShort := NewCompositeInterceptor(skipFirst, second)
	cs2 := &ChangeSet{
		Node:     "node-a",
		strategy: defaultStrategy,
		Changes: []Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "UPDATE",
			OldValues: []any{int64(1), "Alice"},
			NewValues: []any{int64(1), "Bob"},
		}},
	}
	cs2.SetInterceptor(chainShort)
	if err := cs2.Apply(context.Background(), db); err != nil {
		t.Fatalf("CompositeInterceptor short-circuit Apply failed: %v", err)
	}
	wantShort := []string{"before-skip", "after-skip"}
	if !slices.Equal(order, wantShort) {
		t.Fatalf("short-circuit order = %v, want %v", order, wantShort)
	}
	assertUserRow(t, db, 1, "Alice")
}

func TestFilterInterceptors(t *testing.T) {
	db := openMemTestDB(t)
	execMemSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	execMemSQL(t, db, `CREATE TABLE sessions (id INTEGER PRIMARY KEY, token TEXT)`)

	t.Run("IgnoreTablesAndDDL", func(t *testing.T) {
		interceptor := NewIgnoreTablesInterceptor("sessions")
		cs := &ChangeSet{
			Node:     "node-a",
			strategy: defaultStrategy,
			Changes: []Change{
				{
					Operation: "SQL",
					Command:   "CREATE TABLE IF NOT EXISTS sessions (id INTEGER PRIMARY KEY, token TEXT)",
				},
				{
					Database:  "main",
					Table:     "sessions",
					Columns:   []string{"id", "token"},
					PKColumns: []string{"id"},
					Operation: "INSERT",
					NewValues: []any{int64(1), "secret"},
				},
				{
					Database:  "main",
					Table:     "users",
					Columns:   []string{"id", "name"},
					PKColumns: []string{"id"},
					Operation: "INSERT",
					NewValues: []any{int64(1), "Alice"},
				},
			},
		}
		cs.SetInterceptor(interceptor)
		if err := cs.Apply(context.Background(), db); err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		assertUserRow(t, db, 1, "Alice")
		assertRowCount(t, db, "sessions", 0)
	})

	t.Run("AllowTablesSkipsEntireChangeSetWhenNoneMatch", func(t *testing.T) {
		interceptor := NewAllowTablesInterceptor("users")
		cs := &ChangeSet{
			Node:      "node-a",
			Subject:   "ha.users",
			StreamSeq: 7,
			strategy:  defaultStrategy,
			Changes: []Change{{
				Database:  "main",
				Table:     "sessions",
				Columns:   []string{"id", "token"},
				PKColumns: []string{"id"},
				Operation: "INSERT",
				NewValues: []any{int64(2), "tok"},
			}},
		}
		execMemSQL(t, db, `CREATE TABLE IF NOT EXISTS ha_stats (subject TEXT PRIMARY KEY, received_seq INTEGER, updated_at TEXT)`)
		cs.SetInterceptor(interceptor)
		if err := cs.Apply(context.Background(), db); err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		assertRowCount(t, db, "sessions", 0)
		// Verify ha_stats still advances received_seq when ChangeSet is skipped by interceptor!
		var seq uint64
		if err := db.QueryRow(`SELECT received_seq FROM ha_stats WHERE subject = ?`, "ha.users").Scan(&seq); err != nil {
			t.Fatalf("query ha_stats: %v", err)
		}
		if seq != 7 {
			t.Fatalf("ha_stats received_seq = %d, want 7", seq)
		}
	})

	t.Run("IgnoreDeleteOperations", func(t *testing.T) {
		interceptor := NewIgnoreOperationsInterceptor("DELETE")
		cs := &ChangeSet{
			Node:     "node-a",
			strategy: defaultStrategy,
			Changes: []Change{{
				Database:  "main",
				Table:     "users",
				Columns:   []string{"id", "name"},
				PKColumns: []string{"id"},
				Operation: "DELETE",
				OldValues: []any{int64(1), "Alice"},
			}},
		}
		cs.SetInterceptor(interceptor)
		if err := cs.Apply(context.Background(), db); err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		assertUserRow(t, db, 1, "Alice")
	})

	t.Run("AllowOperations", func(t *testing.T) {
		interceptor := NewAllowOperationsInterceptor("INSERT")
		cs := &ChangeSet{
			Node:     "node-a",
			strategy: defaultStrategy,
			Changes: []Change{
				{
					Database:  "main",
					Table:     "users",
					Columns:   []string{"id", "name"},
					PKColumns: []string{"id"},
					Operation: "UPDATE",
					OldValues: []any{int64(1), "Alice"},
					NewValues: []any{int64(1), "Updated"},
				},
				{
					Database:  "main",
					Table:     "users",
					Columns:   []string{"id", "name"},
					PKColumns: []string{"id"},
					Operation: "INSERT",
					NewValues: []any{int64(2), "Bob"},
				},
			},
		}
		cs.SetInterceptor(interceptor)
		if err := cs.Apply(context.Background(), db); err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		assertUserRow(t, db, 1, "Alice")
		assertUserRow(t, db, 2, "Bob")
	})
}

func TestLastWriterWinsMultiMasterConvergence(t *testing.T) {
	// Simulate two active-active leaderless nodes (node-a and node-b) writing concurrently
	// to the same row, including JSON serialization/deserialization (float64 PKs) as done by NATS.
	dbA := openMemTestDB(t)
	dbB := openMemTestDB(t)
	execMemSQL(t, dbA, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	execMemSQL(t, dbB, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)

	var conflictsOnB int
	lwwA := NewLastWriterWinsInterceptor()
	lwwB := NewLastWriterWinsInterceptor(WithLWWOnConflict(func(_ *ChangeSet, _ Change, localTsNs int64, localNode string) {
		conflictsOnB++
		if localTsNs != 200 || localNode != "node-b" {
			t.Errorf("unexpected conflict metadata: ts=%d node=%s", localTsNs, localNode)
		}
	}))

	// Step 1: Node A writes locally at t=100 ("Alice")
	execMemSQL(t, dbA, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	csFromA := &ChangeSet{
		Node:      "node-a",
		Timestamp: 100,
		Changes: []Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "INSERT",
			NewValues: []any{int64(1), "Alice"},
			TsNs:      100,
		}},
	}
	lwwA.OnLocalChangeSet(csFromA)

	// Step 2: Concurrently, Node B writes locally at t=200 ("Bob") BEFORE receiving csFromA!
	execMemSQL(t, dbB, `INSERT INTO users (id, name) VALUES (1, 'Bob')`)
	csFromB := &ChangeSet{
		Node:      "node-b",
		Timestamp: 200,
		Changes: []Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "INSERT",
			NewValues: []any{int64(1), "Bob"},
			TsNs:      200,
		}},
	}
	lwwB.OnLocalChangeSet(csFromB)

	// Step 3: Deliver csFromB -> Node A, and csFromA -> Node B (simulating JSON round-trip over NATS)
	remoteBOnA := roundTripJSONChangeSet(t, csFromB, lwwA)
	if err := remoteBOnA.Apply(context.Background(), dbA); err != nil {
		t.Fatalf("apply csFromB on Node A: %v", err)
	}

	remoteAOnB := roundTripJSONChangeSet(t, csFromA, lwwB)
	if err := remoteAOnB.Apply(context.Background(), dbB); err != nil {
		t.Fatalf("apply csFromA on Node B: %v", err)
	}

	// Both nodes MUST converge to "Bob" (t=200 > t=100)!
	assertUserRow(t, dbA, 1, "Bob")
	assertUserRow(t, dbB, 1, "Bob")
	if conflictsOnB != 1 {
		t.Fatalf("conflictsOnB = %d, want 1", conflictsOnB)
	}
}

func TestLastWriterWinsEqualTimestampTieBreaking(t *testing.T) {
	dbA := openMemTestDB(t)
	dbB := openMemTestDB(t)
	execMemSQL(t, dbA, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	execMemSQL(t, dbB, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)

	lwwA := NewLastWriterWinsInterceptor()
	lwwB := NewLastWriterWinsInterceptor()

	// Identical timestamp ts=500 on both nodes; "node-b" > "node-a" wins deterministically.
	execMemSQL(t, dbA, `INSERT INTO users (id, name) VALUES (1, 'FromA')`)
	csA := &ChangeSet{
		Node:      "node-a",
		Timestamp: 500,
		Changes: []Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "INSERT",
			NewValues: []any{int64(1), "FromA"},
			TsNs:      500,
		}},
	}
	lwwA.OnLocalChangeSet(csA)

	execMemSQL(t, dbB, `INSERT INTO users (id, name) VALUES (1, 'FromB')`)
	csB := &ChangeSet{
		Node:      "node-b",
		Timestamp: 500,
		Changes: []Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "INSERT",
			NewValues: []any{int64(1), "FromB"},
			TsNs:      500,
		}},
	}
	lwwB.OnLocalChangeSet(csB)

	if err := roundTripJSONChangeSet(t, csB, lwwA).Apply(context.Background(), dbA); err != nil {
		t.Fatalf("apply csB on A: %v", err)
	}
	if err := roundTripJSONChangeSet(t, csA, lwwB).Apply(context.Background(), dbB); err != nil {
		t.Fatalf("apply csA on B: %v", err)
	}

	assertUserRow(t, dbA, 1, "FromB")
	assertUserRow(t, dbB, 1, "FromB")
}

func TestLastWriterWinsDeleteTombstoneAndResurrection(t *testing.T) {
	db := openMemTestDB(t)
	execMemSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	lww := NewLastWriterWinsInterceptor()

	// 1. Insert at t=100
	insertCS := &ChangeSet{
		Node:     "node-a",
		strategy: defaultStrategy,
		Changes: []Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "INSERT",
			NewValues: []any{int64(1), "Alice"},
			TsNs:      100,
		}},
	}
	insertCS.SetInterceptor(lww)
	if err := insertCS.Apply(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	assertUserRow(t, db, 1, "Alice")

	// 2. Delete at t=300 records tombstone ts=300
	deleteCS := &ChangeSet{
		Node:     "node-b",
		strategy: defaultStrategy,
		Changes: []Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "DELETE",
			OldValues: []any{int64(1), "Alice"},
			TsNs:      300,
		}},
	}
	deleteCS.SetInterceptor(lww)
	if err := deleteCS.Apply(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	assertRowCount(t, db, "users", 0)

	// 3. Delayed UPDATE (t=200 < 300) arrives late: MUST NOT resurrect deleted row!
	staleUpdate := &ChangeSet{
		Node:     "node-a",
		strategy: defaultStrategy,
		Changes: []Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "UPDATE",
			OldValues: []any{int64(1), "Alice"},
			NewValues: []any{int64(1), "Zombie"},
			TsNs:      200,
		}},
	}
	staleUpdate.SetInterceptor(lww)
	if err := staleUpdate.Apply(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	assertRowCount(t, db, "users", 0)

	// 4. Newer UPDATE (t=400 > 300) arrives: wins over t=300 delete and upserts row!
	newerUpdate := &ChangeSet{
		Node:     "node-c",
		strategy: defaultStrategy,
		Changes: []Change{{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			PKColumns: []string{"id"},
			Operation: "UPDATE",
			OldValues: []any{int64(1), "Alice"},
			NewValues: []any{int64(1), "Reborn"},
			TsNs:      400,
		}},
	}
	newerUpdate.SetInterceptor(lww)
	if err := newerUpdate.Apply(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	assertUserRow(t, db, 1, "Reborn")
}

func TestLastWriterWinsPartialVsAtomicAndRollbackSafety(t *testing.T) {
	t.Run("PartialRowFilteringByDefault", func(t *testing.T) {
		db := openMemTestDB(t)
		execMemSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
		lww := NewLastWriterWinsInterceptor()

		// Seed row 1 at t=200
		seed := &ChangeSet{
			Node:     "node-b",
			strategy: defaultStrategy,
			Changes: []Change{{
				Database:  "main",
				Table:     "users",
				Columns:   []string{"id", "name"},
				PKColumns: []string{"id"},
				Operation: "INSERT",
				NewValues: []any{int64(1), "NewerRow1"},
				TsNs:      200,
			}},
		}
		seed.SetInterceptor(lww)
		if err := seed.Apply(context.Background(), db); err != nil {
			t.Fatal(err)
		}

		// Multi-row tx at t=150 touching row 1 (stale) and row 2 (fresh): row 1 skipped, row 2 applied!
		multi := &ChangeSet{
			Node:     "node-a",
			strategy: defaultStrategy,
			Changes: []Change{
				{
					Database:  "main",
					Table:     "users",
					Columns:   []string{"id", "name"},
					PKColumns: []string{"id"},
					Operation: "UPDATE",
					OldValues: []any{int64(1), "Old"},
					NewValues: []any{int64(1), "StaleRow1"},
					TsNs:      150,
				},
				{
					Database:  "main",
					Table:     "users",
					Columns:   []string{"id", "name"},
					PKColumns: []string{"id"},
					Operation: "INSERT",
					NewValues: []any{int64(2), "FreshRow2"},
					TsNs:      150,
				},
			},
		}
		multi.SetInterceptor(lww)
		if err := multi.Apply(context.Background(), db); err != nil {
			t.Fatal(err)
		}
		assertUserRow(t, db, 1, "NewerRow1")
		assertUserRow(t, db, 2, "FreshRow2")
	})

	t.Run("AtomicTransactionSkipsEntireChangeSetOnConflict", func(t *testing.T) {
		db := openMemTestDB(t)
		execMemSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
		lww := NewLastWriterWinsInterceptor(WithLWWAtomicTransaction(true))

		seed := &ChangeSet{
			Node:     "node-b",
			strategy: defaultStrategy,
			Changes: []Change{{
				Database:  "main",
				Table:     "users",
				Columns:   []string{"id", "name"},
				PKColumns: []string{"id"},
				Operation: "INSERT",
				NewValues: []any{int64(1), "NewerRow1"},
				TsNs:      200,
			}},
		}
		seed.SetInterceptor(lww)
		if err := seed.Apply(context.Background(), db); err != nil {
			t.Fatal(err)
		}

		// Row 2 comes BEFORE Row 1 in the transaction, yet because Row 1 conflicts,
		// neither Row 2 nor its metadata version may be committed!
		multi := &ChangeSet{
			Node:     "node-a",
			strategy: defaultStrategy,
			Changes: []Change{
				{
					Database:  "main",
					Table:     "users",
					Columns:   []string{"id", "name"},
					PKColumns: []string{"id"},
					Operation: "INSERT",
					NewValues: []any{int64(2), "AbortedRow2"},
					TsNs:      150,
				},
				{
					Database:  "main",
					Table:     "users",
					Columns:   []string{"id", "name"},
					PKColumns: []string{"id"},
					Operation: "UPDATE",
					OldValues: []any{int64(1), "Old"},
					NewValues: []any{int64(1), "StaleRow1"},
					TsNs:      150,
				},
			},
		}
		multi.SetInterceptor(lww)
		if err := multi.Apply(context.Background(), db); err != nil {
			t.Fatal(err)
		}
		assertRowCount(t, db, "users", 1)

		// And a subsequent write to Row 2 at t=120 (< 150) must still succeed because t=150 was never recorded!
		retryRow2 := &ChangeSet{
			Node:     "node-a",
			strategy: defaultStrategy,
			Changes: []Change{{
				Database:  "main",
				Table:     "users",
				Columns:   []string{"id", "name"},
				PKColumns: []string{"id"},
				Operation: "INSERT",
				NewValues: []any{int64(2), "Row2At120"},
				TsNs:      120,
			}},
		}
		retryRow2.SetInterceptor(lww)
		if err := retryRow2.Apply(context.Background(), db); err != nil {
			t.Fatal(err)
		}
		assertUserRow(t, db, 2, "Row2At120")
	})

	t.Run("FailedApplyDoesNotAdvanceStoredVersion", func(t *testing.T) {
		db := openMemTestDB(t)
		// Deliberately do NOT create table "missing_table" so Apply fails in tx.ExecContext.
		lww := NewLastWriterWinsInterceptor()

		failing := &ChangeSet{
			Node:     "node-b",
			strategy: defaultStrategy,
			Changes: []Change{{
				Database:  "main",
				Table:     "missing_table",
				Columns:   []string{"id", "name"},
				PKColumns: []string{"id"},
				Operation: "INSERT",
				NewValues: []any{int64(1), "Fail"},
				TsNs:      500,
			}},
		}
		failing.SetInterceptor(lww)
		if err := failing.Apply(context.Background(), db); err == nil {
			t.Fatal("expected Apply on missing table to fail")
		}

		// Now create table and apply an older timestamp (t=100); must succeed because t=500 rolled back!
		execMemSQL(t, db, `CREATE TABLE missing_table (id INTEGER PRIMARY KEY, name TEXT)`)
		succeeding := &ChangeSet{
			Node:     "node-a",
			strategy: defaultStrategy,
			Changes: []Change{{
				Database:  "main",
				Table:     "missing_table",
				Columns:   []string{"id", "name"},
				PKColumns: []string{"id"},
				Operation: "INSERT",
				NewValues: []any{int64(1), "Recovered"},
				TsNs:      100,
			}},
		}
		succeeding.SetInterceptor(lww)
		if err := succeeding.Apply(context.Background(), db); err != nil {
			t.Fatalf("expected Apply after failed tx to succeed, got %v", err)
		}
		assertRowCount(t, db, "missing_table", 1)
	})
}

func TestColumnLWWInterceptor(t *testing.T) {
	t.Run("NumericVersionAndSymmetricTieBreaker", func(t *testing.T) {
		dbA := openMemTestDB(t)
		dbB := openMemTestDB(t)
		execMemSQL(t, dbA, `CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, version INTEGER)`)
		execMemSQL(t, dbB, `CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, version INTEGER)`)

		interceptor := NewColumnLWWInterceptor("version")

		// Node A has local row (1, "Alpha", version=2); Node B has local row (1, "Beta", version=2) (exact version tie!)
		execMemSQL(t, dbA, `INSERT INTO docs (id, title, version) VALUES (1, 'Alpha', 2)`)
		execMemSQL(t, dbB, `INSERT INTO docs (id, title, version) VALUES (1, 'Beta', 2)`)

		csA := &ChangeSet{
			Node: "node-a",
			Changes: []Change{{
				Database:  "main",
				Table:     "docs",
				Columns:   []string{"id", "title", "version"},
				PKColumns: []string{"id"},
				Operation: "INSERT",
				NewValues: []any{int64(1), "Alpha", int64(2)},
			}},
		}
		csB := &ChangeSet{
			Node: "node-b",
			Changes: []Change{{
				Database:  "main",
				Table:     "docs",
				Columns:   []string{"id", "title", "version"},
				PKColumns: []string{"id"},
				Operation: "INSERT",
				NewValues: []any{int64(1), "Beta", int64(2)},
			}},
		}

		if err := roundTripJSONChangeSet(t, csB, interceptor).Apply(context.Background(), dbA); err != nil {
			t.Fatal(err)
		}
		if err := roundTripJSONChangeSet(t, csA, interceptor).Apply(context.Background(), dbB); err != nil {
			t.Fatal(err)
		}

		// "Beta" > "Alpha", so both nodes deterministically converge to "Beta"!
		var titleA, titleB string
		if err := dbA.QueryRow(`SELECT title FROM docs WHERE id = 1`).Scan(&titleA); err != nil {
			t.Fatal(err)
		}
		if err := dbB.QueryRow(`SELECT title FROM docs WHERE id = 1`).Scan(&titleB); err != nil {
			t.Fatal(err)
		}
		if titleA != "Beta" || titleB != "Beta" {
			t.Fatalf("diverged on version tie: A=%q B=%q, want both Beta", titleA, titleB)
		}

		// Stale update with version=1 is rejected
		stale := &ChangeSet{
			Node:     "node-c",
			strategy: defaultStrategy,
			Changes: []Change{{
				Database:  "main",
				Table:     "docs",
				Columns:   []string{"id", "title", "version"},
				PKColumns: []string{"id"},
				Operation: "UPDATE",
				OldValues: []any{int64(1), "Old", int64(0)},
				NewValues: []any{int64(1), "Stale", int64(1)},
			}},
		}
		stale.SetInterceptor(interceptor)
		if err := stale.Apply(context.Background(), dbA); err != nil {
			t.Fatal(err)
		}
		if err := dbA.QueryRow(`SELECT title FROM docs WHERE id = 1`).Scan(&titleA); err != nil {
			t.Fatal(err)
		}
		if titleA != "Beta" {
			t.Fatalf("stale version overwrote row: got %q, want Beta", titleA)
		}
	})

	t.Run("RFC3339TimestampAndDeleteVsConcurrentUpdate", func(t *testing.T) {
		dbA := openMemTestDB(t)
		dbB := openMemTestDB(t)
		execMemSQL(t, dbA, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT)`)
		execMemSQL(t, dbB, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT)`)

		interceptor := NewColumnLWWInterceptor("updated_at")

		// Both nodes start at version 2026-11-17T10:00:00Z
		execMemSQL(t, dbA, `INSERT INTO items (id, name, updated_at) VALUES (1, 'Initial', '2026-11-17T10:00:00Z')`)
		execMemSQL(t, dbB, `INSERT INTO items (id, name, updated_at) VALUES (1, 'Initial', '2026-11-17T10:00:00Z')`)

		// Node A deletes row observing 10:00:00Z; concurrently Node B updates row to 10:05:00Z!
		execMemSQL(t, dbA, `DELETE FROM items WHERE id = 1`)
		deleteFromA := &ChangeSet{
			Node: "node-a",
			Changes: []Change{{
				Database:  "main",
				Table:     "items",
				Columns:   []string{"id", "name", "updated_at"},
				PKColumns: []string{"id"},
				Operation: "DELETE",
				OldValues: []any{int64(1), "Initial", "2026-11-17T10:00:00Z"},
			}},
		}

		execMemSQL(t, dbB, `UPDATE items SET name = 'UpdatedOnB', updated_at = '2026-11-17T10:05:00Z' WHERE id = 1`)
		updateFromB := &ChangeSet{
			Node: "node-b",
			Changes: []Change{{
				Database:  "main",
				Table:     "items",
				Columns:   []string{"id", "name", "updated_at"},
				PKColumns: []string{"id"},
				Operation: "UPDATE",
				OldValues: []any{int64(1), "Initial", "2026-11-17T10:00:00Z"},
				NewValues: []any{int64(1), "UpdatedOnB", "2026-11-17T10:05:00Z"},
			}},
		}

		// Apply deleteFromA on B (skipped because B is at 10:05 > 10:00), and updateFromB on A (upserts!)
		if err := roundTripJSONChangeSet(t, deleteFromA, interceptor).Apply(context.Background(), dbB); err != nil {
			t.Fatal(err)
		}
		if err := roundTripJSONChangeSet(t, updateFromB, interceptor).Apply(context.Background(), dbA); err != nil {
			t.Fatal(err)
		}

		// Both nodes converge to UpdatedOnB!
		var nameA, nameB string
		if err := dbA.QueryRow(`SELECT name FROM items WHERE id = 1`).Scan(&nameA); err != nil {
			t.Fatalf("expected item on A to be restored by newer concurrent update: %v", err)
		}
		if err := dbB.QueryRow(`SELECT name FROM items WHERE id = 1`).Scan(&nameB); err != nil {
			t.Fatalf("expected item on B to survive stale concurrent delete: %v", err)
		}
		if nameA != "UpdatedOnB" || nameB != "UpdatedOnB" {
			t.Fatalf("diverged: A=%q B=%q", nameA, nameB)
		}

		// Causal delete observing 10:05:00Z deletes the row cleanly!
		causalDelete := &ChangeSet{
			Node:     "node-b",
			strategy: defaultStrategy,
			Changes: []Change{{
				Database:  "main",
				Table:     "items",
				Columns:   []string{"id", "name", "updated_at"},
				PKColumns: []string{"id"},
				Operation: "DELETE",
				OldValues: []any{int64(1), "UpdatedOnB", "2026-11-17T10:05:00Z"},
			}},
		}
		causalDelete.SetInterceptor(interceptor)
		if err := causalDelete.Apply(context.Background(), dbA); err != nil {
			t.Fatal(err)
		}
		assertRowCount(t, dbA, "items", 0)
	})

	t.Run("DDLAndInsertInSameChangeSetAndStrictMissingColumn", func(t *testing.T) {
		db := openMemTestDB(t)
		interceptor := NewColumnLWWInterceptor("updated_at",
			WithTableVersionColumn("custom_tbl", "rev"),
			WithNodeTieBreakerColumn("origin"),
		)

		// Table custom_tbl does not exist yet when BeforeApply runs because Change 0 creates it!
		cs := &ChangeSet{
			Node:     "node-a",
			strategy: defaultStrategy,
			Changes: []Change{
				{
					Operation: "SQL",
					Command:   `CREATE TABLE IF NOT EXISTS custom_tbl (id INTEGER PRIMARY KEY, val TEXT, rev INTEGER, origin TEXT)`,
				},
				{
					Database:  "main",
					Table:     "custom_tbl",
					Columns:   []string{"id", "val", "rev", "origin"},
					PKColumns: []string{"id"},
					Operation: "INSERT",
					NewValues: []any{int64(1), "v1", int64(10), "node-a"},
				},
			},
		}
		cs.SetInterceptor(interceptor)
		if err := cs.Apply(context.Background(), db); err != nil {
			t.Fatalf("DDL + INSERT in same ChangeSet failed: %v", err)
		}
		assertRowCount(t, db, "custom_tbl", 1)

		// Strict missing column returns an error when column is absent
		strict := NewColumnLWWInterceptor("nonexistent_col", WithColumnLWWStrictMissingColumn(true))
		badCS := &ChangeSet{
			Node:     "node-a",
			strategy: defaultStrategy,
			Changes: []Change{{
				Database:  "main",
				Table:     "custom_tbl",
				Columns:   []string{"id", "val", "rev", "origin"},
				PKColumns: []string{"id"},
				Operation: "INSERT",
				NewValues: []any{int64(2), "v2", int64(11), "node-a"},
			}},
		}
		badCS.SetInterceptor(strict)
		if err := badCS.Apply(context.Background(), db); err == nil {
			t.Fatal("expected error with strict missing column, got nil")
		}
	})

	t.Run("IntraBatchMultipleChangesToSameRow", func(t *testing.T) {
		db := openMemTestDB(t)
		execMemSQL(t, db, `CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, version INTEGER)`)
		interceptor := NewColumnLWWInterceptor("version")

		cs := &ChangeSet{
			Node:     "node-a",
			strategy: defaultStrategy,
			Changes: []Change{
				{
					Database:  "main",
					Table:     "docs",
					Columns:   []string{"id", "title", "version"},
					PKColumns: []string{"id"},
					Operation: "INSERT",
					NewValues: []any{int64(1), "v5", int64(5)},
				},
				{
					Database:  "main",
					Table:     "docs",
					Columns:   []string{"id", "title", "version"},
					PKColumns: []string{"id"},
					Operation: "UPDATE",
					OldValues: []any{int64(1), "v5", int64(5)},
					NewValues: []any{int64(1), "v3-stale", int64(3)},
				},
			},
		}
		cs.SetInterceptor(interceptor)
		if err := cs.Apply(context.Background(), db); err != nil {
			t.Fatal(err)
		}
		var title string
		if err := db.QueryRow(`SELECT title FROM docs WHERE id = 1`).Scan(&title); err != nil {
			t.Fatal(err)
		}
		if title != "v5" {
			t.Fatalf("title = %q, want v5", title)
		}
	})
}

func TestNameToOptionsConflictResolution(t *testing.T) {
	tests := []struct {
		name      string
		dsn       string
		wantType  string
		wantError bool
	}{
		{
			name:     "default metadata lww",
			dsn:      "file:test.db?conflictResolution=lww&_journal=WAL",
			wantType: "*ha.LastWriterWinsInterceptor",
		},
		{
			name:     "lww with column",
			dsn:      "file:test.db?conflictResolution=lww:updated_at",
			wantType: "*ha.ColumnLWWInterceptor",
		},
		{
			name:     "column prefix",
			dsn:      "file:test.db?conflictResolution=column:version",
			wantType: "*ha.ColumnLWWInterceptor",
		},
		{
			name:     "noop",
			dsn:      "file:test.db?conflictResolution=noop",
			wantType: "*ha.NoopInterceptor",
		},
		{
			name:      "empty column",
			dsn:       "file:test.db?conflictResolution=column:",
			wantError: true,
		},
		{
			name:      "invalid strategy",
			dsn:       "file:test.db?conflictResolution=unknown",
			wantError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cleanDSN, opts, err := NameToOptions(tc.dsn, "")
			if tc.wantError {
				if err == nil {
					t.Fatalf("expected error for %q, got nil", tc.dsn)
				}
				return
			}
			if err != nil {
				t.Fatalf("NameToOptions(%q) unexpected error: %v", tc.dsn, err)
			}
			if strings.Contains(cleanDSN, "conflictResolution") {
				t.Fatalf("cleanDSN %q still contains conflictResolution", cleanDSN)
			}
			var c Connector
			for _, opt := range opts {
				opt(&c)
			}
			gotType := fmt.Sprintf("%T", c.interceptor)
			if gotType != tc.wantType {
				t.Fatalf("interceptor type = %q, want %q", gotType, tc.wantType)
			}
		})
	}
}

func roundTripJSONChangeSet(t *testing.T, src *ChangeSet, interceptor ChangeSetInterceptor) *ChangeSet {
	t.Helper()
	raw, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshal ChangeSet: %v", err)
	}
	var dst ChangeSet
	dst.SetStrategy(defaultStrategy)
	dst.SetInterceptor(interceptor)
	if err := json.Unmarshal(raw, &dst); err != nil {
		t.Fatalf("unmarshal ChangeSet: %v", err)
	}
	return &dst
}

func execMemSQL(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func assertUserRow(t *testing.T, db *sql.DB, id int64, wantName string) {
	t.Helper()
	var got string
	if err := db.QueryRow(`SELECT name FROM users WHERE id = ?`, id).Scan(&got); err != nil {
		t.Fatalf("SELECT name FROM users WHERE id = %d: %v", id, err)
	}
	if got != wantName {
		t.Fatalf("users[%d].name = %q, want %q", id, got, wantName)
	}
}

func assertRowCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteSQLiteIdent(table))).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if got != want {
		t.Fatalf("row count of %s = %d, want %d", table, got, want)
	}
}

// ---------------------------------------------------------------------------
// Minimal in-memory SQL driver backed by github.com/rqlite/sql AST parser
// for self-contained testing of ChangeSet.Apply and ChangeSetInterceptors.
// ---------------------------------------------------------------------------

var (
	memDriverOnce sync.Once
	memStoresMu   sync.Mutex
	memStores     = make(map[string]*memDatabase)
	memStoreSeq   int
)

type memTable struct {
	columns   []string
	pkColumns []string
	rows      [][]any
}

func (t *memTable) clone() *memTable {
	cp := &memTable{
		columns:   slices.Clone(t.columns),
		pkColumns: slices.Clone(t.pkColumns),
		rows:      make([][]any, len(t.rows)),
	}
	for i, r := range t.rows {
		cp.rows[i] = slices.Clone(r)
	}
	return cp
}

type memDatabase struct {
	mu     sync.Mutex
	tables map[string]*memTable
}

type memDriver struct{}

func (memDriver) Open(name string) (driver.Conn, error) {
	memStoresMu.Lock()
	store, ok := memStores[name]
	if !ok {
		store = &memDatabase{tables: make(map[string]*memTable)}
		memStores[name] = store
	}
	memStoresMu.Unlock()
	return &memConn{db: store}, nil
}

func openMemTestDB(t *testing.T) *sql.DB {
	t.Helper()
	memDriverOnce.Do(func() {
		sql.Register("go-ha-memtest", memDriver{})
	})
	memStoresMu.Lock()
	memStoreSeq++
	dsn := fmt.Sprintf("mem-%d", memStoreSeq)
	memStoresMu.Unlock()

	db, err := sql.Open("go-ha-memtest", dsn)
	if err != nil {
		t.Fatalf("open memtest db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		memStoresMu.Lock()
		delete(memStores, dsn)
		memStoresMu.Unlock()
	})
	return db
}

type memConn struct {
	db       *memDatabase
	txTables map[string]*memTable
}

func (c *memConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("use ExecContext/QueryContext")
}

func (c *memConn) Close() error { return nil }

func (c *memConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *memConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	if c.txTables != nil {
		return nil, errors.New("nested transaction")
	}
	c.txTables = make(map[string]*memTable, len(c.db.tables))
	for k, tbl := range c.db.tables {
		c.txTables[k] = tbl.clone()
	}
	return &memTx{conn: c}, nil
}

type memTx struct {
	conn *memConn
}

func (tx *memTx) Commit() error {
	tx.conn.db.mu.Lock()
	defer tx.conn.db.mu.Unlock()
	if tx.conn.txTables == nil {
		return sql.ErrTxDone
	}
	tx.conn.db.tables = tx.conn.txTables
	tx.conn.txTables = nil
	return nil
}

func (tx *memTx) Rollback() error {
	tx.conn.db.mu.Lock()
	defer tx.conn.db.mu.Unlock()
	if tx.conn.txTables == nil {
		return sql.ErrTxDone
	}
	tx.conn.txTables = nil
	return nil
}

func (c *memConn) activeTables() map[string]*memTable {
	if c.txTables != nil {
		return c.txTables
	}
	return c.db.tables
}

func (c *memConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()

	stmts, err := rqlitesql.NewParser(strings.NewReader(query)).ParseStatements()
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", query, err)
	}
	tables := c.activeTables()
	var totalAffected int64

	for _, stmt := range stmts {
		switch s := stmt.(type) {
		case *rqlitesql.CreateTableStatement:
			name := strings.ToLower(s.Name.Name)
			if _, exists := tables[name]; exists && s.IfNotExists.IsValid() {
				continue
			}
			tbl := &memTable{}
			for _, col := range s.Columns {
				colName := col.Name.Name
				tbl.columns = append(tbl.columns, colName)
				for _, cons := range col.Constraints {
					if _, ok := cons.(*rqlitesql.PrimaryKeyConstraint); ok {
						tbl.pkColumns = append(tbl.pkColumns, colName)
					}
				}
			}
			for _, cons := range s.Constraints {
				if pk, ok := cons.(*rqlitesql.PrimaryKeyConstraint); ok {
					for _, expr := range pk.Columns {
						tbl.pkColumns = append(tbl.pkColumns, strings.Trim(expr.String(), `"`))
					}
				}
			}
			tables[name] = tbl

		case *rqlitesql.InsertStatement:
			name := strings.ToLower(s.Table.Name)
			tbl, ok := tables[name]
			if !ok {
				return nil, fmt.Errorf("no such table: %s", s.Table.Name)
			}
			cols := tbl.columns
			if len(s.Columns) > 0 {
				cols = make([]string, len(s.Columns))
				for i, id := range s.Columns {
					cols[i] = id.Name
				}
			}
			argPos := 0
			for _, tuple := range s.ValueLists {
				row := make([]any, len(tbl.columns))
				for i, expr := range tuple.Exprs {
					val, err := evalMemExpr(expr, args, &argPos)
					if err != nil {
						return nil, err
					}
					colIdx := findColumnIndex(tbl.columns, cols[i])
					if colIdx < 0 {
						return nil, fmt.Errorf("no such column: %s", cols[i])
					}
					row[colIdx] = val
				}
				replaced := false
				if len(tbl.pkColumns) > 0 {
					for rIdx, existing := range tbl.rows {
						if samePK(tbl, existing, row) {
							if s.Replace.IsValid() || s.InsertOrReplace.IsValid() || len(s.UpsertClauses) > 0 {
								tbl.rows[rIdx] = row
								replaced = true
								break
							}
							return nil, fmt.Errorf("UNIQUE constraint failed: %s", name)
						}
					}
				}
				if !replaced {
					tbl.rows = append(tbl.rows, row)
				}
				totalAffected++
			}

		case *rqlitesql.UpdateStatement:
			name := strings.ToLower(s.Table.Name.Name)
			tbl, ok := tables[name]
			if !ok {
				return nil, fmt.Errorf("no such table: %s", s.Table.Name.Name)
			}
			argPos := 0
			setIndices := make([]int, len(s.Assignments))
			setValues := make([]any, len(s.Assignments))
			for i, assign := range s.Assignments {
				colName := assign.Columns[0].Name
				colIdx := findColumnIndex(tbl.columns, colName)
				if colIdx < 0 {
					return nil, fmt.Errorf("no such column: %s", colName)
				}
				val, err := evalMemExpr(assign.Expr, args, &argPos)
				if err != nil {
					return nil, err
				}
				setIndices[i] = colIdx
				setValues[i] = val
			}
			whereStart := argPos
			for rIdx, row := range tbl.rows {
				pos := whereStart
				matched, err := evalMemWhere(s.WhereExpr, tbl, row, args, &pos)
				if err != nil {
					return nil, err
				}
				if matched {
					updated := slices.Clone(row)
					for i, colIdx := range setIndices {
						updated[colIdx] = setValues[i]
					}
					tbl.rows[rIdx] = updated
					totalAffected++
				}
			}

		case *rqlitesql.DeleteStatement:
			name := strings.ToLower(s.Table.Name.Name)
			tbl, ok := tables[name]
			if !ok {
				return nil, fmt.Errorf("no such table: %s", s.Table.Name.Name)
			}
			kept := make([][]any, 0, len(tbl.rows))
			for _, row := range tbl.rows {
				pos := 0
				matched, err := evalMemWhere(s.WhereExpr, tbl, row, args, &pos)
				if err != nil {
					return nil, err
				}
				if matched {
					totalAffected++
				} else {
					kept = append(kept, row)
				}
			}
			tbl.rows = kept

		default:
			return nil, fmt.Errorf("unsupported statement in memDriver: %T", stmt)
		}
	}

	return sqliteResult{rowsAffected: totalAffected}, nil
}

func (c *memConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()

	stmts, err := rqlitesql.NewParser(strings.NewReader(query)).ParseStatements()
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", query, err)
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected 1 select statement, got %d", len(stmts))
	}
	sel, ok := stmts[0].(*rqlitesql.SelectStatement)
	if !ok {
		return nil, fmt.Errorf("expected SelectStatement, got %T", stmts[0])
	}
	src, ok := sel.Source.(*rqlitesql.QualifiedTableName)
	if !ok {
		return nil, fmt.Errorf("unsupported select source %T", sel.Source)
	}
	tbl, exists := c.activeTables()[strings.ToLower(src.Name.Name)]
	if !exists {
		return nil, fmt.Errorf("no such table: %s", src.Name.Name)
	}

	var matchedRows [][]any
	for _, row := range tbl.rows {
		pos := 0
		matched, err := evalMemWhere(sel.WhereExpr, tbl, row, args, &pos)
		if err != nil {
			return nil, err
		}
		if matched {
			matchedRows = append(matchedRows, row)
		}
	}

	if len(sel.Columns) == 1 {
		if call, ok := sel.Columns[0].Expr.(*rqlitesql.Call); ok && strings.EqualFold(call.Name.Name, "count") {
			return &memRows{
				cols: []string{"count"},
				rows: [][]driver.Value{{int64(len(matchedRows))}},
			}, nil
		}
	}

	colNames := make([]string, len(sel.Columns))
	colIndices := make([]int, len(sel.Columns))
	for i, rc := range sel.Columns {
		ident, ok := rc.Expr.(*rqlitesql.Ident)
		if !ok {
			return nil, fmt.Errorf("unsupported select expr %T", rc.Expr)
		}
		idx := findColumnIndex(tbl.columns, ident.Name)
		if idx < 0 {
			return nil, fmt.Errorf("no such column: %s", ident.Name)
		}
		colNames[i] = ident.Name
		colIndices[i] = idx
	}

	if sel.LimitExpr != nil {
		if n, err := strconv.Atoi(sel.LimitExpr.String()); err == nil && n < len(matchedRows) {
			matchedRows = matchedRows[:n]
		}
	}

	out := make([][]driver.Value, len(matchedRows))
	for rIdx, row := range matchedRows {
		vals := make([]driver.Value, len(colIndices))
		for cIdx, tblIdx := range colIndices {
			vals[cIdx] = toDriverValue(row[tblIdx])
		}
		out[rIdx] = vals
	}
	return &memRows{cols: colNames, rows: out}, nil
}

func samePK(tbl *memTable, a, b []any) bool {
	if len(tbl.pkColumns) == 0 {
		return false
	}
	for _, pk := range tbl.pkColumns {
		idx := findColumnIndex(tbl.columns, pk)
		if idx < 0 || canonicalScalar(a[idx]) != canonicalScalar(b[idx]) {
			return false
		}
	}
	return true
}

func evalMemWhere(expr rqlitesql.Expr, tbl *memTable, row []any, args []driver.NamedValue, argPos *int) (bool, error) {
	if expr == nil {
		return true, nil
	}
	switch e := expr.(type) {
	case *rqlitesql.ParenExpr:
		return evalMemWhere(e.X, tbl, row, args, argPos)
	case *rqlitesql.BinaryExpr:
		switch e.Op {
		case rqlitesql.AND:
			left, err := evalMemWhere(e.X, tbl, row, args, argPos)
			if err != nil || !left {
				// Still advance bind positions on right branch if needed
				if err == nil {
					_, _ = evalMemWhere(e.Y, tbl, row, args, argPos)
				}
				return false, err
			}
			return evalMemWhere(e.Y, tbl, row, args, argPos)
		case rqlitesql.EQ:
			ident, ok := e.X.(*rqlitesql.Ident)
			if !ok {
				return false, fmt.Errorf("unsupported lhs %T", e.X)
			}
			idx := findColumnIndex(tbl.columns, ident.Name)
			if idx < 0 {
				return false, fmt.Errorf("no such column: %s", ident.Name)
			}
			rhs, err := evalMemExpr(e.Y, args, argPos)
			if err != nil {
				return false, err
			}
			return canonicalScalar(row[idx]) == canonicalScalar(rhs), nil
		case rqlitesql.IS:
			ident, ok := e.X.(*rqlitesql.Ident)
			if !ok {
				return false, fmt.Errorf("unsupported IS lhs %T", e.X)
			}
			idx := findColumnIndex(tbl.columns, ident.Name)
			if idx < 0 {
				return false, fmt.Errorf("no such column: %s", ident.Name)
			}
			if _, isNull := e.Y.(*rqlitesql.NullLit); isNull {
				return row[idx] == nil, nil
			}
		}
	}
	return false, fmt.Errorf("unsupported where expr %T: %s", expr, expr.String())
}

func evalMemExpr(expr rqlitesql.Expr, args []driver.NamedValue, argPos *int) (any, error) {
	switch e := expr.(type) {
	case *rqlitesql.BindExpr:
		if strings.HasPrefix(e.Name, "?") && len(e.Name) > 1 {
			ord, err := strconv.Atoi(e.Name[1:])
			if err != nil {
				return nil, err
			}
			if ord < 1 || ord > len(args) {
				return nil, fmt.Errorf("bind index %d out of range", ord)
			}
			return args[ord-1].Value, nil
		}
		if *argPos >= len(args) {
			return nil, fmt.Errorf("bind pos %d out of range (%d)", *argPos, len(args))
		}
		val := args[*argPos].Value
		*argPos++
		return val, nil
	case *rqlitesql.StringLit:
		return e.Value, nil
	case *rqlitesql.NumberLit:
		if n, err := strconv.ParseInt(e.Value, 10, 64); err == nil {
			return n, nil
		}
		return strconv.ParseFloat(e.Value, 64)
	case *rqlitesql.NullLit:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported expr %T", expr)
	}
}

func toDriverValue(v any) driver.Value {
	switch val := v.(type) {
	case int:
		return int64(val)
	default:
		return val
	}
}

type memRows struct {
	cols []string
	rows [][]driver.Value
	pos  int
}

func (r *memRows) Columns() []string { return slices.Clone(r.cols) }
func (r *memRows) Close() error      { return nil }

func (r *memRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.pos])
	r.pos++
	return nil
}

package ha

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestRowidIdentifyStrategyToSQL(t *testing.T) {
	strategy := rowidIdentifyStrategy{}

	t.Run("INSERT", func(t *testing.T) {
		change := Change{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			NewValues: []any{int64(1), "alice"},
			NewRowID:  42,
			Operation: "INSERT",
		}
		sqlQuery, args := strategy.ToSQL(change)
		wantSQL := "INSERT INTO main.users (id, name, `rowid`) VALUES (?1,?2,?3) ON CONFLICT DO UPDATE SET id = ?1, name = ?2, `rowid` = ?3"
		wantArgs := []any{int64(1), "alice", int64(42)}
		if sqlQuery != wantSQL {
			t.Fatalf("ToSQL(INSERT) sql = %q, want %q", sqlQuery, wantSQL)
		}
		if !reflect.DeepEqual(args, wantArgs) {
			t.Fatalf("ToSQL(INSERT) args = %#v, want %#v", args, wantArgs)
		}
	})

	t.Run("UPDATE", func(t *testing.T) {
		change := Change{
			Database:  "main",
			Table:     "users",
			Columns:   []string{"id", "name"},
			NewValues: []any{int64(1), "bob"},
			OldRowID:  42,
			NewRowID:  42,
			Operation: "UPDATE",
		}
		sqlQuery, args := strategy.ToSQL(change)
		wantSQL := "UPDATE main.users SET id = ?, name = ? WHERE `rowid` = ?"
		wantArgs := []any{int64(1), "bob", int64(42)}
		if sqlQuery != wantSQL {
			t.Fatalf("ToSQL(UPDATE) sql = %q, want %q", sqlQuery, wantSQL)
		}
		if !reflect.DeepEqual(args, wantArgs) {
			t.Fatalf("ToSQL(UPDATE) args = %#v, want %#v", args, wantArgs)
		}
	})

	t.Run("DELETE", func(t *testing.T) {
		change := Change{
			Database:  "main",
			Table:     "users",
			OldRowID:  42,
			Operation: "DELETE",
		}
		sqlQuery, args := strategy.ToSQL(change)
		wantSQL := "DELETE FROM main.users WHERE `rowid` = ?"
		wantArgs := []any{int64(42)}
		if sqlQuery != wantSQL {
			t.Fatalf("ToSQL(DELETE) sql = %q, want %q", sqlQuery, wantSQL)
		}
		if !reflect.DeepEqual(args, wantArgs) {
			t.Fatalf("ToSQL(DELETE) args = %#v, want %#v", args, wantArgs)
		}
	})
}

type stubSubscriber struct {
	NoopSubscriber
	seq atomic.Uint64
}

func (s *stubSubscriber) LatestSeq() uint64 {
	return s.seq.Load()
}

type stubPublisher struct {
	NoopPublisher
	seq atomic.Uint64
}

func (p *stubPublisher) Sequence() uint64 {
	return p.seq.Load()
}

func TestConsistentReaderWaitsOnFailoverLeaderCatchup(t *testing.T) {
	sub := &stubSubscriber{}
	pub := &stubPublisher{}
	sub.seq.Store(5)
	pub.seq.Store(0) // Newly elected leader did not originate write 6

	c := &Connector{
		leaderProvider: &StaticLeader{Target: ""}, // IsLeader() == true
		subscriber:     sub,
		publisher:      pub,
	}

	go func() {
		time.Sleep(15 * time.Millisecond)
		sub.seq.Store(6)
	}()

	var observedSeq uint64
	handler := c.ConsistentReaderFunc(func(w http.ResponseWriter, r *http.Request) {
		observedSeq = c.LatestSeq()
		w.WriteHeader(http.StatusOK)
	}, 200*time.Millisecond, http.MethodGet)

	req := httptest.NewRequest(http.MethodGet, "/items", nil)
	req.AddCookie(&http.Cookie{Name: TXCookieName, Value: "6"})
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if observedSeq < 6 {
		t.Fatalf("ConsistentReader served stale read on new leader: observedSeq = %d, want >= 6", observedSeq)
	}
}

package ha

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
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

func TestConsistentReaderWaitsWhenNewLeaderHasGapBeforePubSeq(t *testing.T) {
	sub := &stubSubscriber{}
	pub := &stubPublisher{}
	// Subscriber has only applied up to 4 (missing 5 from previous leader),
	// while the new leader already published sequence 6.
	sub.seq.Store(4)
	pub.seq.Store(6)

	c := &Connector{
		leaderProvider: &StaticLeader{Target: ""},
		subscriber:     sub,
		publisher:      pub,
	}

	go func() {
		time.Sleep(15 * time.Millisecond)
		sub.seq.Store(5)
	}()

	var observedSubSeq uint64
	handler := c.ConsistentReaderFunc(func(w http.ResponseWriter, r *http.Request) {
		observedSubSeq = c.LatestSeq()
		w.WriteHeader(http.StatusOK)
	}, 200*time.Millisecond, http.MethodGet)

	req := httptest.NewRequest(http.MethodGet, "/items", nil)
	req.AddCookie(&http.Cookie{Name: TXCookieName, Value: "5"})
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if observedSubSeq < 5 {
		t.Fatalf("ConsistentReader served stale read before gap closed: observedSubSeq = %d, want >= 5", observedSubSeq)
	}
}

func TestConsistentReaderLeaderTimeoutReturnsGatewayTimeout(t *testing.T) {
	sub := &stubSubscriber{}
	pub := &stubPublisher{}
	sub.seq.Store(2)
	pub.seq.Store(0)

	c := &Connector{
		leaderProvider: &StaticLeader{Target: ""}, // IsLeader() == true
		subscriber:     sub,
		publisher:      pub,
	}

	called := false
	handler := c.ConsistentReaderFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}, 15*time.Millisecond, http.MethodGet)

	req := httptest.NewRequest(http.MethodGet, "/items", nil)
	req.AddCookie(&http.Cookie{Name: TXCookieName, Value: "5"})
	rec := httptest.NewRecorder()

	handler(rec, req)

	if called {
		t.Fatalf("expected handler not to be called when leader times out waiting for txSeq")
	}
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusGatewayTimeout)
	}
	if !strings.Contains(rec.Body.String(), "consistent reader timeout") {
		t.Fatalf("body = %q, want to contain %q", rec.Body.String(), "consistent reader timeout")
	}
}

func TestConsistentReaderForwardsBodyWithoutPrematureCancel(t *testing.T) {
	leaderSub := &stubSubscriber{}
	leaderPub := &stubPublisher{}
	leaderSub.seq.Store(10)

	leaderConn := &Connector{
		leaderProvider: &StaticLeader{Target: ""},
		subscriber:     leaderSub,
		publisher:      leaderPub,
	}

	expectedPayload := strings.Repeat("consistent-chunk-", 2048)
	var observedRawQuery string
	leaderSrv := httptest.NewServer(leaderConn.ConsistentReaderFunc(func(w http.ResponseWriter, r *http.Request) {
		observedRawQuery = r.URL.RawQuery
		flusher, _ := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		// Stream in multiple chunks with a pause to verify context is not canceled before body finishes
		half := len(expectedPayload) / 2
		_, _ = w.Write([]byte(expectedPayload[:half]))
		if flusher != nil {
			flusher.Flush()
		}
		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write([]byte(expectedPayload[half:]))
	}, 200*time.Millisecond, http.MethodGet))
	defer leaderSrv.Close()

	followerSub := &stubSubscriber{}
	followerPub := &stubPublisher{}
	followerSub.seq.Store(5)

	followerConn := &Connector{
		// Include trailing slash to verify URL joining does not produce "//"
		leaderProvider: &StaticLeader{Target: leaderSrv.URL + "/"},
		subscriber:     followerSub,
		publisher:      followerPub,
	}

	handler := followerConn.ConsistentReaderFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("follower local handler should not have been called")
	}, 25*time.Millisecond, http.MethodGet)

	req := httptest.NewRequest(http.MethodGet, "/items", nil)
	req.AddCookie(&http.Cookie{Name: TXCookieName, Value: "10"})
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %q", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Body.String() != expectedPayload {
		t.Fatalf("forwarded body length = %d, want %d", rec.Body.Len(), len(expectedPayload))
	}
	if observedRawQuery != "" {
		t.Fatalf("observedRawQuery = %q, want empty", observedRawQuery)
	}
}

func TestConsistentReaderPreventsForwardingLoop(t *testing.T) {
	sub := &stubSubscriber{}
	pub := &stubPublisher{}
	sub.seq.Store(1)

	var srv *httptest.Server
	c := &Connector{
		subscriber: sub,
		publisher:  pub,
	}
	var hits atomic.Int32
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		c.ConsistentReaderFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}, 15*time.Millisecond, http.MethodGet)(w, r)
	}))
	defer srv.Close()

	// Misconfigured/stale follower target pointing to itself
	c.leaderProvider = &StaticLeader{Target: srv.URL}

	req := httptest.NewRequest(http.MethodGet, "/items", nil)
	req.AddCookie(&http.Cookie{Name: TXCookieName, Value: "10"})
	rec := httptest.NewRecorder()

	c.ConsistentReaderFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}, 20*time.Millisecond, http.MethodGet)(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusGatewayTimeout)
	}
	if got := hits.Load(); got > 1 {
		t.Fatalf("expected at most 1 forwarded hop, got %d", got)
	}
}

func TestConsistentReaderClientContextCancellation(t *testing.T) {
	sub := &stubSubscriber{}
	pub := &stubPublisher{}
	sub.seq.Store(1)

	c := &Connector{
		leaderProvider: &StaticLeader{Target: ""},
		subscriber:     sub,
		publisher:      pub,
	}

	called := false
	handler := c.ConsistentReaderFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}, 200*time.Millisecond, http.MethodGet)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	req := httptest.NewRequest(http.MethodGet, "/items", nil).WithContext(ctx)
	req.AddCookie(&http.Cookie{Name: TXCookieName, Value: "5"})
	rec := httptest.NewRecorder()

	handler(rec, req)

	if called {
		t.Fatalf("expected handler not to be called when client cancels request context")
	}
}

func TestConsistentReaderMultipleCookiesAndResponseWriterCookiePath(t *testing.T) {
	sub := &stubSubscriber{}
	pub := &stubPublisher{}
	sub.seq.Store(5)
	pub.seq.Store(9)

	c := &Connector{
		leaderProvider: &StaticLeader{Target: ""},
		subscriber:     sub,
		publisher:      pub,
	}

	// Verify ResponseWriter sets Path="/" and only writes Set-Cookie once across multiple Write() calls
	writeRec := httptest.NewRecorder()
	rw := c.ResponseWriter(writeRec)
	_, _ = rw.Write([]byte("part1"))
	_, _ = rw.Write([]byte("part2"))

	cookies := writeRec.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("expected 1 Set-Cookie header, got %d", len(cookies))
	}
	if cookies[0].Name != TXCookieName || cookies[0].Value != "9" || cookies[0].Path != "/" {
		t.Fatalf("unexpected cookie: %+v", cookies[0])
	}

	// Verify ConsistentReaderFunc picks max(3, 8) = 8 across multiple _txseq cookies
	go func() {
		time.Sleep(15 * time.Millisecond)
		sub.seq.Store(8)
	}()

	var observedSeq uint64
	handler := c.ConsistentReaderFunc(func(w http.ResponseWriter, r *http.Request) {
		observedSeq = c.LatestSeq()
		w.WriteHeader(http.StatusOK)
	}, 200*time.Millisecond, http.MethodGet)

	req := httptest.NewRequest(http.MethodGet, "/items", nil)
	req.AddCookie(&http.Cookie{Name: TXCookieName, Value: "3"})
	req.AddCookie(&http.Cookie{Name: TXCookieName, Value: "8"})
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if observedSeq < 8 {
		t.Fatalf("observedSeq = %d, want >= 8", observedSeq)
	}
}

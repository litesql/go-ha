package ha

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var processID = time.Now().UnixNano()

type NoopPublisher struct{}

func (p *NoopPublisher) Publish(cs *ChangeSet) error {
	return nil
}

func (p *NoopPublisher) Sequence() uint64 {
	return 0
}

func NewNoopPublisher() *NoopPublisher {
	return &NoopPublisher{}
}

type ChangeSetSerializer func(*ChangeSet) ([]byte, error)

type WriterPublisher struct {
	writer     io.Writer
	serializer ChangeSetSerializer
}

func NewWriterPublisher(w io.Writer, serializer ChangeSetSerializer) *WriterPublisher {
	return &WriterPublisher{
		writer:     w,
		serializer: serializer,
	}
}

func (p *WriterPublisher) Publish(cs *ChangeSet) error {
	b, err := p.serializer(cs)
	if err != nil {
		return err
	}
	_, err = p.writer.Write(b)
	return err
}

func (p *WriterPublisher) Sequence() uint64 {
	return 0
}

func NewJSONPublisher(w io.Writer) *JSONPublisher {
	return &JSONPublisher{
		writer: w,
	}
}

type JSONPublisher struct {
	writer io.Writer
}

func (p *JSONPublisher) Publish(cs *ChangeSet) error {
	b, err := json.Marshal(cs)
	if err != nil {
		return err
	}
	_, err = p.writer.Write(b)
	return err
}

func (p *JSONPublisher) Sequence() uint64 {
	return 0
}

type NATSPublisher struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	timeout  time.Duration
	sequence uint64
	subject  string
}

func NewNATSPublisher(nc *nats.Conn, subject string, timeout time.Duration, streamConfig *jetstream.StreamConfig) (*NATSPublisher, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if streamConfig != nil {
		// Create a stream to hold the Replication messages
		_, err = js.CreateOrUpdateStream(ctx, *streamConfig)
		if err != nil {
			return nil, err
		}
	}
	return &NATSPublisher{
		nc:      nc,
		js:      js,
		timeout: timeout,
		subject: subject,
	}, nil
}

func (p *NATSPublisher) Publish(cs *ChangeSet) error {
	cs.ProcessID = processID
	data, err := json.Marshal(cs)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()
	pubAck, err := p.js.Publish(ctx, p.subject, data)
	if err != nil {
		return err
	}
	p.sequence = pubAck.Sequence
	slog.Debug("published replication message", "stream", pubAck.Stream, "seq", pubAck.Sequence, "subject", p.subject, "duplicate", pubAck.Duplicate)
	return nil
}

func (p *NATSPublisher) Sequence() uint64 {
	return p.sequence
}

type AsyncNATSPublisher struct {
	*NATSPublisher
	db       *sql.DB
	sequence uint64
	mu       sync.Mutex
	close    chan struct{}
}

func NewAsyncNATSPublisher(nc *nats.Conn, subject string, timeout time.Duration, streamConfig *jetstream.StreamConfig, db *sql.DB) (*AsyncNATSPublisher, error) {
	pub, err := NewNATSPublisher(nc, subject, timeout, streamConfig)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS ha_outbox(subject TEXT, changeset BLOB, timestamp DATETIME);`)
	if err != nil {
		return nil, fmt.Errorf("create outbox table: %w", err)
	}

	asyncPub := &AsyncNATSPublisher{
		NATSPublisher: pub,
		close:         make(chan struct{}),
		db:            db,
	}
	go asyncPub.start()

	return asyncPub, nil
}

func (p *AsyncNATSPublisher) Publish(cs *ChangeSet) error {
	cs.ProcessID = processID
	data, err := json.Marshal(cs)
	if err != nil {
		return err
	}
	p.mu.Lock()
	_, err = p.db.Exec("INSERT INTO ha_outbox(subject, changeset, timestamp) VALUES(?, ?, ?)", p.subject, data, time.Now())
	p.mu.Unlock()
	return err
}

func (p *AsyncNATSPublisher) Sequence() uint64 {
	return p.sequence
}

func (p *AsyncNATSPublisher) Close() error {
	if p.close != nil {
		close(p.close)
	}
	return nil
}

func (p *AsyncNATSPublisher) start() {
	for {
		select {
		case <-p.close:
			return
		default:
			p.relay()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (p *AsyncNATSPublisher) relay() {
	var (
		id        int
		changeset []byte
	)
	err := p.db.QueryRow("SELECT rowid, changeset FROM ha_outbox WHERE subject = ? ORDER BY timestamp, rowid LIMIT 1", p.subject).Scan(&id, &changeset)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			slog.Error("async publisher relay query outbox", "error", err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()
	pubAck, err := p.js.Publish(ctx, p.subject, changeset)
	if err != nil {
		slog.Error("async publisher relay publish", "error", err)
		time.Sleep(5 * time.Second)
		return
	}
	p.sequence = pubAck.Sequence
	slog.Debug("published replication message", "stream", pubAck.Stream, "seq", pubAck.Sequence, "subject", p.subject, "duplicate", pubAck.Duplicate)
	p.mu.Lock()
	_, err = p.db.Exec("DELETE FROM ha_outbox WHERE rowid = ?", id)
	p.mu.Unlock()
	if err != nil {
		slog.Error("async publisher relay remove from outbox", "error", err)
	}
}

type DBPublisher struct {
	db     *sql.DB
	mu     sync.Mutex
	maxAge time.Duration
	once   sync.Once
	quit   chan struct{}
}

func NewDBPublisher(db *sql.DB, maxAge time.Duration) (*DBPublisher, error) {
	p := &DBPublisher{
		db:     db,
		maxAge: maxAge,
		quit:   make(chan struct{}),
	}
	go p.cleaner()
	return p, nil
}

func (p *DBPublisher) Publish(cs *ChangeSet) error {
	data, err := json.Marshal(cs)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	_, err = p.db.Exec("INSERT INTO ha_changesets(changeset) VALUES(?)", data)
	if err != nil {
		return err
	}
	return nil
}

func (p *DBPublisher) Sequence() uint64 {
	if p.db == nil {
		return 0
	}
	var seq sql.NullInt64
	err := p.db.QueryRow("SELECT MAX(seq) FROM ha_changesets").Scan(&seq)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			slog.Error("db publisher query last sequence", "error", err)
		}
		return 0
	}
	if !seq.Valid {
		return 0
	}
	return uint64(seq.Int64)
}

func (p *DBPublisher) Close() error {
	close(p.quit)
	return nil
}

func (p *DBPublisher) cleaner() {
	if p.maxAge <= 0 {
		return
	}
	for {
		select {
		case <-p.quit:
			return
		case <-time.After(p.maxAge / 2):
			if p.db == nil {
				continue
			}
			p.mu.Lock()
			slog.Debug("start cleaning local transactions history")
			now := time.Now()
			res, err := p.db.Exec(`DELETE FROM ha_changesets WHERE changeset->'timestamp_ns' < ?`, fmt.Sprint(time.Now().Add(-p.maxAge).UnixNano()))
			if err != nil {
				slog.Error("cleaning local transactions history", "error", err)
				p.mu.Unlock()
				continue
			}
			rowsAffected, _ := res.RowsAffected()
			slog.Debug("cleaning local transactions history", "count", rowsAffected, "duration", time.Since(now))
			p.mu.Unlock()
		}
	}
}

type delayedStartPublisher struct {
	pub Publisher
}

func (p *delayedStartPublisher) Publish(cs *ChangeSet) error {
	return nil
}

func (p *delayedStartPublisher) Sequence() uint64 {
	return 0
}

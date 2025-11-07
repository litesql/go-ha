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

type NATSPublisher struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	timeout time.Duration
	subject string
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
		// Create a stream to hold the CDC messages
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
	slog.Debug("published CDC message", "stream", pubAck.Stream, "seq", pubAck.Sequence, "subject", p.subject, "duplicate", pubAck.Duplicate)
	return nil
}

type AsyncNATSPublisher struct {
	*NATSPublisher
	db    *sql.DB
	mu    sync.Mutex
	close chan struct{}
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
		return
	}
	slog.Debug("published CDC message", "stream", pubAck.Stream, "seq", pubAck.Sequence, "subject", p.subject, "duplicate", pubAck.Duplicate)
	p.mu.Lock()
	_, err = p.db.Exec("DELETE FROM ha_outbox WHERE rowid = ?", id)
	p.mu.Unlock()
	if err != nil {
		slog.Error("async publisher relay remove from outbox", "error", err)
	}
}

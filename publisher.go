package ha

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
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
			for {
				now := time.Now()
				res, err := p.db.Exec(`DELETE FROM ha_changesets WHERE seq IN (
					SELECT seq FROM ha_changesets WHERE timestamp < ? LIMIT 1000
				)`, fmt.Sprint(time.Now().Add(-p.maxAge).UnixNano()))
				if err != nil {
					slog.Error("cleaning local transactions history", "error", err)
					break
				}
				rowsAffected, _ := res.RowsAffected()
				slog.Debug("cleaning local transactions history", "count", rowsAffected, "duration", time.Since(now))
				if rowsAffected < 1000 {
					break
				}
			}
			p.mu.Unlock()
		}
	}
}

type TwoPhaseCommitPublisher struct {
	sequence   uint64
	timeout    time.Duration
	workers    map[string]sqlv1.DatabaseServiceClient
	recoveryDB *sql.DB
}

func NewTwoPhaseCommitPublisher(workersKeys map[string]string, timeout time.Duration, recoveryDB *sql.DB) (*TwoPhaseCommitPublisher, error) {
	workers, err := connectTwoPhaseCommitWorkers(workersKeys)
	if err != nil {
		return nil, err
	}

	var sequence uint64 = 1
	if recoveryDB != nil {
		recoveryDB.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS ha_2pc_recovery(
			id INTEGER PRIMARY KEY CHECK (id = 1),
			changeset JSONB,
			workers JSONB,
			sequence INTEGER
		)`)
		var recoveryChangeSet, recoveryWorkers string
		recoveryDB.QueryRowContext(context.Background(), `SELECT changeset, workers, sequence FROM ha_2pc_recovery WHERE id = 1`).Scan(&recoveryChangeSet, &recoveryWorkers, &sequence)
		if recoveryChangeSet != "" && recoveryWorkers != "" {
			var (
				cs ChangeSet
				wk map[string]string
			)
			err := json.Unmarshal([]byte(recoveryChangeSet), &cs)
			if err != nil {
				return nil, fmt.Errorf("unmarshal changest: %w", err)
			}
			err = json.Unmarshal([]byte(recoveryWorkers), &wk)
			if err != nil {
				return nil, fmt.Errorf("unmarshal workers: %w", err)
			}
			workers, err := connectTwoPhaseCommitWorkers(wk)
			if err != nil {
				return nil, fmt.Errorf("connect workers to start recovery process: %w", err)
			}

			req, err := changeSetToProto(&cs)
			if err != nil {
				return nil, err
			}

			streams := make(map[string]grpc.BidiStreamingClient[sqlv1.ChangeSetRequest, sqlv1.ChangeSetResponse])
			// connect
			for remote, w := range workers {
				stream, err := w.ChangeSet(context.Background())
				if err != nil {
					return nil, err
				}
				streams[remote] = stream
			}

			defer func() {
				for _, stream := range streams {
					stream.CloseSend()
				}
			}()

			req.Type = sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_UNDO
			// undo
			for remote, stream := range streams {
				err = stream.Send(req)
				if err != nil {
					return nil, err
				}

				resp, err := stream.Recv()
				if err != nil {
					return nil, err
				}
				if resp.Error != "" {
					return nil, fmt.Errorf("worker recovery: %s", resp.Error)
				}
				delete(wk, remote)
				wkJSON, _ := json.Marshal(wk)
				_, err = recoveryDB.ExecContext(context.Background(), `UPDATE ha_2pc_recovery SET workers = ? WHERE id = 1`, string(wkJSON))
				if err != nil {
					return nil, fmt.Errorf("update 2pc recovery table: %w", err)
				}
			}

			wkJSON, _ := json.Marshal(workersKeys)
			_, err = recoveryDB.ExecContext(context.Background(), `UPDATE ha_2pc_recovery SET workers = ? WHERE id = 1`, string(wkJSON))
			if err != nil {
				return nil, fmt.Errorf("update 2pc recovery table: %w", err)
			}
		}
	}

	return &TwoPhaseCommitPublisher{
		sequence:   sequence,
		timeout:    timeout,
		workers:    workers,
		recoveryDB: recoveryDB,
	}, nil
}

func (p *TwoPhaseCommitPublisher) Publish(cs *ChangeSet) (err error) {
	req, err := changeSetToProto(cs)
	if err != nil {
		return err
	}
	req.Type = sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_PREPARE

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	var streams []grpc.BidiStreamingClient[sqlv1.ChangeSetRequest, sqlv1.ChangeSetResponse]
	// connect
	for _, w := range p.workers {
		stream, err := w.ChangeSet(ctx)
		if err != nil {
			return err
		}
		streams = append(streams, stream)
	}

	defer func() {
		for _, stream := range streams {
			stream.CloseSend()
		}
	}()

	// prepare
	for _, stream := range streams {
		err = stream.Send(req)
		if err != nil {
			return err
		}

		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf("worker prepare: %s", resp.Error)
		}
	}

	// commit
	var commitedStreams []grpc.BidiStreamingClient[sqlv1.ChangeSetRequest, sqlv1.ChangeSetResponse]
	defer func() {
		if err == nil {
			return
		}
		req.Type = sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_UNDO
		for _, stream := range commitedStreams {
			err = stream.Send(req)
			if err != nil {
				slog.Error("failed to send undo message", "error", err)
				continue
			}
			resp, err := stream.Recv()
			if err != nil {
				slog.Error("failed to receive undo response", "error", err)
				continue
			}
			if resp.Error != "" {
				slog.Error("failed to undo transaction", "error", resp.Error)
			}
		}
	}()

	if p.recoveryDB != nil {
		csJSON, _ := json.Marshal(cs)
		_, err := p.recoveryDB.ExecContext(context.Background(), `UPDATE ha_2pc_recovery SET changeset = ? WHERE id = 1`, csJSON)
		if err != nil {
			return fmt.Errorf("update ha_2pc_recovery table: %w", err)
		}
	}

	req.Type = sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_COMMIT
	for _, stream := range streams {
		err = stream.Send(req)
		if err != nil {
			return err
		}

		var resp *sqlv1.ChangeSetResponse
		resp, err = stream.Recv()
		if err != nil {
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf("worker commit: %s", resp.Error)
		}
		commitedStreams = append(commitedStreams, stream)
	}
	p.sequence++
	if p.recoveryDB != nil {
		_, err := p.recoveryDB.ExecContext(context.Background(), `UPDATE ha_2pc_recovery SET changeset = '', sequence = ? WHERE id = 1`, p.sequence)
		if err != nil {
			return fmt.Errorf("update ha_2pc_recovery table: %w", err)
		}
	}
	return nil
}

func (p *TwoPhaseCommitPublisher) Sequence() uint64 {
	return p.sequence
}

func connectTwoPhaseCommitWorkers(workersKeys map[string]string) (map[string]sqlv1.DatabaseServiceClient, error) {
	workers := make(map[string]sqlv1.DatabaseServiceClient)
	for remote, token := range workersKeys {
		u, err := url.Parse(remote)
		if err != nil {
			slog.Error("parse url", "error", err)
			return nil, err
		}

		var dialOpts []grpc.DialOption

		if strings.HasPrefix(remote, "http://") {
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		} else {
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
		}
		if token != "" {
			dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(grpcCredentials{token: token}))
		}

		cc, err := grpc.NewClient(u.Host, dialOpts...)
		if err != nil {
			slog.Error("grpc connect", "error", err)
			return nil, err
		}
		workers[remote] = sqlv1.NewDatabaseServiceClient(cc)
	}
	return workers, nil
}

type grpcCredentials struct {
	token string
}

func (c grpcCredentials) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": c.token,
	}, nil
}

func (c grpcCredentials) RequireTransportSecurity() bool {
	return false
}

type CompositePublisher struct {
	publishers []Publisher
}

func NewCompositePublisher(publishers ...Publisher) *CompositePublisher {
	return &CompositePublisher{
		publishers: publishers,
	}
}

func (p *CompositePublisher) Publish(cs *ChangeSet) error {
	for _, pub := range p.publishers {
		err := pub.Publish(cs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *CompositePublisher) Sequence() uint64 {
	var max uint64
	for _, pub := range p.publishers {
		x := pub.Sequence()
		if max < x {
			max = x
		}
	}
	return max
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

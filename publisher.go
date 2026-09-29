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
	"maps"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
	mu         sync.Mutex
	sequence   uint64
	timeout    time.Duration
	workers    map[string]sqlv1.DatabaseServiceClient
	workerKeys map[string]string
	localDB    *sql.DB
}

// TwoPhaseCommitDecisionTable stores local commit decisions for crash recovery.
const TwoPhaseCommitDecisionTable = "ha_2pc_decisions"

// ErrTwoPhaseCommitPending marks a transaction committed locally but awaiting remote recovery.
var ErrTwoPhaseCommitPending = errors.New("two-phase commit is committed locally and pending remote recovery")

// TwoPhaseCommitDecisionWriter records the commit decision in the origin transaction.
type TwoPhaseCommitDecisionWriter func(context.Context, string, ...any) error

// TwoPhaseCommitPreparer starts the remote prepare phase for an origin transaction.
type TwoPhaseCommitPreparer interface {
	PrepareTwoPhaseCommit(*ChangeSet) (PreparedTwoPhaseCommit, error)
}

// PreparedTwoPhaseCommit finalizes or aborts a transaction after remote preparation.
type PreparedTwoPhaseCommit interface {
	RecordCommitDecision(TwoPhaseCommitDecisionWriter) error
	Commit() error
	Abort() error
	ResolveCommit() (bool, error)
	Close()
}

type preparedTwoPhaseCommit struct {
	publisher *TwoPhaseCommitPublisher
	req       *sqlv1.ChangeSetRequest
	streams   map[string]grpc.BidiStreamingClient[sqlv1.ChangeSetRequest, sqlv1.ChangeSetResponse]
	cancel    context.CancelFunc
	recorded  bool
	finish    sync.Once
}

func NewTwoPhaseCommitPublisher(workersKeys map[string]string, timeout time.Duration) (*TwoPhaseCommitPublisher, error) {
	workers, err := connectTwoPhaseCommitWorkers(workersKeys)
	if err != nil {
		return nil, err
	}

	var sequence uint64 = 1
	p := TwoPhaseCommitPublisher{
		sequence:   sequence,
		timeout:    timeout,
		workers:    workers,
		workerKeys: maps.Clone(workersKeys),
	}

	return &p, nil
}

// BindLocalDB configures the origin database used to persist and recover decisions.
func (p *TwoPhaseCommitPublisher) BindLocalDB(db *sql.DB) error {
	if db == nil {
		return errors.New("local database is required for two-phase commit recovery")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.localDB = db
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE IF NOT EXISTS `+TwoPhaseCommitDecisionTable+` (
		transaction_id TEXT PRIMARY KEY,
		changeset BLOB NOT NULL,
		workers BLOB NOT NULL
	)`); err != nil {
		return fmt.Errorf("create two-phase commit decision table: %w", err)
	}
	return p.recoverLocalDecisions(db)
}

// RecoverTwoPhaseCommits replays durable local decisions before new transactions begin.
func (p *TwoPhaseCommitPublisher) RecoverTwoPhaseCommits() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.localDB == nil {
		return errors.New("two-phase commit publisher is not bound to its local database")
	}
	return p.recoverLocalDecisions(p.localDB)
}

// PrepareTwoPhaseCommit prepares each configured remote participant.
func (p *TwoPhaseCommitPublisher) PrepareTwoPhaseCommit(cs *ChangeSet) (PreparedTwoPhaseCommit, error) {
	if p.localDB == nil {
		return nil, errors.New("two-phase commit publisher is not bound to its local database")
	}
	if len(cs.Changes) == 0 {
		return nil, errors.New("cannot prepare an empty changeset")
	}
	p.mu.Lock()
	if cs.TransactionID == "" {
		cs.TransactionID = uuid.NewString()
	}
	if cs.Timestamp == 0 {
		cs.Timestamp = time.Now().UnixNano()
	}
	req, err := changeSetToProto(cs)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	req.Type = sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_PREPARE
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	prepared := &preparedTwoPhaseCommit{
		publisher: p,
		req:       req,
		streams:   make(map[string]grpc.BidiStreamingClient[sqlv1.ChangeSetRequest, sqlv1.ChangeSetResponse]),
		cancel:    cancel,
	}
	for remote, worker := range p.workers {
		stream, err := worker.ChangeSet(ctx)
		if err != nil {
			prepared.abortPrepared()
			prepared.close()
			return nil, err
		}
		prepared.streams[remote] = stream
		if err := stream.Send(req); err != nil {
			prepared.abortPrepared()
			prepared.close()
			return nil, err
		}
		resp, err := stream.Recv()
		if err != nil {
			prepared.abortPrepared()
			prepared.close()
			return nil, err
		}
		if resp.Error != "" {
			prepared.abortPrepared()
			prepared.close()
			return nil, fmt.Errorf("worker prepare: %s", resp.Error)
		}
	}
	return prepared, nil
}

func (tx *preparedTwoPhaseCommit) RecordCommitDecision(write TwoPhaseCommitDecisionWriter) error {
	if write == nil {
		return errors.New("decision writer is required")
	}
	if tx.recorded {
		return errors.New("commit decision already recorded")
	}
	changeset, err := json.Marshal(tx.req)
	if err != nil {
		return err
	}
	workers, err := json.Marshal(tx.publisher.workerKeys)
	if err != nil {
		return err
	}
	err = write(context.Background(), `INSERT INTO `+TwoPhaseCommitDecisionTable+`(transaction_id, changeset, workers) VALUES(?, ?, ?)`, tx.req.TransactionId, changeset, workers)
	if err == nil {
		tx.recorded = true
	}
	return err
}

func (tx *preparedTwoPhaseCommit) Commit() error {
	if !tx.recorded {
		tx.close()
		return errors.New("commit decision was not recorded with the local transaction")
	}
	tx.req.Type = sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_COMMIT
	for _, stream := range tx.streams {
		if err := stream.Send(tx.req); err != nil {
			tx.close()
			return fmt.Errorf("send worker commit: %w", err)
		}
		resp, err := stream.Recv()
		if err != nil {
			tx.close()
			return fmt.Errorf("receive worker commit: %w", err)
		}
		if resp.Error != "" {
			tx.close()
			return fmt.Errorf("worker commit: %s", resp.Error)
		}
	}
	if _, err := tx.publisher.localDB.ExecContext(context.Background(), `DELETE FROM `+TwoPhaseCommitDecisionTable+` WHERE transaction_id = ?`, tx.req.TransactionId); err != nil {
		tx.close()
		return fmt.Errorf("delete completed two-phase commit decision: %w", err)
	}
	tx.publisher.sequence++
	tx.close()
	return nil
}

func (tx *preparedTwoPhaseCommit) Abort() error {
	err := tx.abortPrepared()
	tx.close()
	return err
}

func (tx *preparedTwoPhaseCommit) ResolveCommit() (bool, error) {
	var exists int
	err := tx.publisher.localDB.QueryRowContext(context.Background(), `SELECT 1 FROM `+TwoPhaseCommitDecisionTable+` WHERE transaction_id = ?`, tx.req.TransactionId).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, tx.Abort()
	}
	if err != nil {
		tx.Close()
		return false, err
	}
	tx.recorded = true
	return true, tx.Commit()
}

func (tx *preparedTwoPhaseCommit) Close() {
	tx.close()
}

func (tx *preparedTwoPhaseCommit) abortPrepared() error {
	tx.req.Type = sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_ABORT
	var result error
	for _, stream := range tx.streams {
		if err := stream.Send(tx.req); err != nil {
			result = errors.Join(result, err)
			continue
		}
		if _, err := stream.Recv(); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (tx *preparedTwoPhaseCommit) close() {
	tx.finish.Do(func() {
		for _, stream := range tx.streams {
			_ = stream.CloseSend()
		}
		tx.cancel()
		tx.publisher.mu.Unlock()
	})
}

func (p *TwoPhaseCommitPublisher) recoverLocalDecisions(db *sql.DB) error {
	rows, err := db.QueryContext(context.Background(), `SELECT transaction_id, changeset, workers FROM `+TwoPhaseCommitDecisionTable)
	if err != nil {
		return fmt.Errorf("read pending two-phase commit decisions: %w", err)
	}
	type decision struct {
		id        string
		changeset []byte
		workers   []byte
	}
	var decisions []decision
	for rows.Next() {
		var item decision
		if err := rows.Scan(&item.id, &item.changeset, &item.workers); err != nil {
			rows.Close()
			return err
		}
		decisions = append(decisions, item)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, item := range decisions {
		var req sqlv1.ChangeSetRequest
		var workerKeys map[string]string
		if err := json.Unmarshal(item.changeset, &req); err != nil {
			return fmt.Errorf("decode pending changeset %s: %w", item.id, err)
		}
		if err := json.Unmarshal(item.workers, &workerKeys); err != nil {
			return fmt.Errorf("decode pending workers %s: %w", item.id, err)
		}
		workers, err := connectTwoPhaseCommitWorkers(workerKeys)
		if err != nil {
			return fmt.Errorf("connect workers for pending transaction %s: %w", item.id, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
		for remote, worker := range workers {
			stream, err := worker.ChangeSet(ctx)
			if err != nil {
				cancel()
				return err
			}
			req.Type = sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_STATUS
			if err = stream.Send(&req); err == nil {
				var resp *sqlv1.ChangeSetResponse
				resp, err = stream.Recv()
				if err == nil && resp.Error != "" {
					err = errors.New(resp.Error)
				}
				if err == nil && resp.State != sqlv1.TransactionState_TRANSACTION_STATE_COMMITTED {
					req.Type = sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_COMMIT
					err = stream.Send(&req)
					if err == nil {
						var commitResp *sqlv1.ChangeSetResponse
						commitResp, err = stream.Recv()
						if err == nil && commitResp.Error != "" {
							err = errors.New(commitResp.Error)
						}
					}
				}
			}
			_ = stream.CloseSend()
			if err != nil {
				cancel()
				return fmt.Errorf("recover transaction %s on %s: %w", item.id, remote, err)
			}
		}
		cancel()
		if _, err := db.ExecContext(context.Background(), `DELETE FROM `+TwoPhaseCommitDecisionTable+` WHERE transaction_id = ?`, item.id); err != nil {
			return fmt.Errorf("clear recovered transaction %s: %w", item.id, err)
		}
	}
	return nil
}

func (p *TwoPhaseCommitPublisher) Publish(cs *ChangeSet) (err error) {
	if len(cs.Changes) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if cs.TransactionID == "" {
		cs.TransactionID = uuid.NewString()
	}
	req, err := changeSetToProto(cs)
	if err != nil {
		return err
	}
	req.Type = sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_PREPARE

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	var streams []grpc.BidiStreamingClient[sqlv1.ChangeSetRequest, sqlv1.ChangeSetResponse]

	defer func() {
		for _, stream := range streams {
			stream.CloseSend()
		}
	}()

	// connect
	for _, w := range p.workers {
		stream, err := w.ChangeSet(ctx)
		if err != nil {
			return err
		}
		streams = append(streams, stream)
	}

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

	req.Type = sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_COMMIT
	for _, stream := range streams {
		err = stream.Send(req)
		if err != nil {
			return
		}

		var resp *sqlv1.ChangeSetResponse
		resp, err = stream.Recv()
		if err != nil {
			return
		}
		if resp.Error != "" {
			err = fmt.Errorf("worker commit: %s", resp.Error)
			return
		}
	}	

	return nil
}

func (p *TwoPhaseCommitPublisher) Sequence() uint64 {
	return 0
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

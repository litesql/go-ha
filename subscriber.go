package ha

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	haconnect "github.com/litesql/go-ha/connect"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type NoopSubscriber struct{}

func (s *NoopSubscriber) DB() *sql.DB {
	return nil
}

func (s *NoopSubscriber) SetDB(db *sql.DB) {}

func (s *NoopSubscriber) Start() error {
	return nil
}

func (s *NoopSubscriber) LatestSeq() uint64 {
	return 0
}

func (*NoopSubscriber) RemoveConsumer(ctx context.Context, name string) error {
	return nil
}

func (*NoopSubscriber) DeliveredInfo(ctx context.Context, name string) (any, error) {
	return "", nil
}

func (*NoopSubscriber) HistoryBySeq(ctx context.Context, startSeq uint64) ([]haconnect.HistoryItem, error) {
	return nil, nil
}

func (*NoopSubscriber) HistoryByTime(ctx context.Context, duration time.Duration) ([]haconnect.HistoryItem, error) {
	return nil, nil
}

func (*NoopSubscriber) UndoBySeq(ctx context.Context, startSeq uint64) error {
	return nil
}

func (*NoopSubscriber) UndoByTime(ctx context.Context, duration time.Duration) error {
	return nil
}

func NewNoopSubscriber() *NoopSubscriber {
	return &NoopSubscriber{}
}

type NATSSubscriber struct {
	mu            sync.Mutex
	started       bool
	nc            *nats.Conn
	js            jetstream.JetStream
	consumer      jetstream.Consumer
	node          string
	durable       string
	stream        string
	subject       string
	streamSeq     uint64
	db            *sql.DB
	connProvider  ConnHooksProvider
	interceptor   ChangeSetInterceptor
	applyStrategy sqlStrategy
}

type NATSSubscriberConfig struct {
	Node         string
	Durable      string
	NatsConn     *nats.Conn
	Stream       string
	Subject      string
	Policy       string
	DB           *sql.DB
	ConnProvider ConnHooksProvider
	Interceptor  ChangeSetInterceptor
	RowIdentify  RowIdentify
}

func NewNATSSubscriber(cfg NATSSubscriberConfig) (*NATSSubscriber, error) {
	var (
		deliverPolicy jetstream.DeliverPolicy
		startSeq      uint64
		startTime     *time.Time
	)
	switch cfg.Policy {
	case "all", "":
		deliverPolicy = jetstream.DeliverAllPolicy
	case "last":
		deliverPolicy = jetstream.DeliverLastPolicy
	case "new":
		deliverPolicy = jetstream.DeliverNewPolicy
	default:
		matched, err := regexp.MatchString(`^by_start_sequence=\d+`, cfg.Policy)
		if err != nil {
			return nil, err
		}
		if matched {
			deliverPolicy = jetstream.DeliverByStartSequencePolicy
			_, err := fmt.Sscanf(cfg.Policy, "by_start_sequence=%d", &startSeq)
			if err != nil {
				return nil, fmt.Errorf("invalid subscriber start sequence: %w", err)
			}
			break

		}
		matched, err = regexp.MatchString(`^by_start_time=\w+`, cfg.Policy)
		if err != nil {
			return nil, err
		}
		if matched {
			deliverPolicy = jetstream.DeliverByStartTimePolicy
			dateTime := strings.TrimPrefix(cfg.Policy, "by_start_time=")
			t, err := time.Parse(time.DateTime, dateTime)
			if err != nil {
				return nil, fmt.Errorf("invalid subscriber start time: %w", err)
			}
			startTime = &t
			break
		}
		return nil, fmt.Errorf("invalid deliver policy: %s", cfg.Policy)
	}

	js, err := jetstream.New(cfg.NatsConn)
	if err != nil {
		return nil, err
	}

	strategy, err := getSqlStrategy(cfg.RowIdentify)
	if err != nil {
		return nil, err
	}

	s := NATSSubscriber{
		nc:            cfg.NatsConn,
		js:            js,
		node:          cfg.Node,
		durable:       cfg.Durable,
		stream:        cfg.Stream,
		subject:       cfg.Subject,
		db:            cfg.DB,
		connProvider:  cfg.ConnProvider,
		interceptor:   cfg.Interceptor,
		applyStrategy: strategy,
	}

	consumer, err := s.js.CreateConsumer(context.Background(), s.stream, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: s.subject,
		Durable:       s.durable,
		DeliverPolicy: deliverPolicy,
		OptStartSeq:   startSeq,
		OptStartTime:  startTime,
		MaxAckPending: 1,
	})
	if err != nil {
		if !errors.Is(err, jetstream.ErrConsumerExists) {
			return nil, err
		}
		consumer, err = s.js.Consumer(context.Background(), s.stream, s.durable)
		if err != nil {
			return nil, err
		}
	}

	s.consumer = consumer
	return &s, nil
}

func (s *NATSSubscriber) DB() *sql.DB {
	return s.db
}

func (s *NATSSubscriber) SetDB(db *sql.DB) {
	s.db = db
}

func (s *NATSSubscriber) Start() error {
	conn, err := s.db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	err = s.connProvider.DisableHooks(conn)
	if err != nil {
		return err
	}
	defer s.connProvider.EnableHooks(conn)

	_, err = conn.ExecContext(context.Background(), "CREATE TABLE IF NOT EXISTS "+controlTableName+"(subject TEXT UNIQUE, received_seq INTEGER, updated_at DATETIME)")
	if err != nil {
		return err
	}

	var recv uint64
	err = conn.QueryRowContext(context.Background(), "SELECT received_seq FROM "+controlTableName+" WHERE subject = ?", s.subject).Scan(&recv)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	s.streamSeq = recv
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.mu.Unlock()
	_, err = s.consumer.Consume(s.handler)
	if err != nil {
		slog.Error("failed to start replication consumer", "error", err, "durable", s.durable, "subject", s.subject)
		return err
	}

	return nil
}

func (s *NATSSubscriber) LatestSeq() uint64 {
	return s.streamSeq
}

func (s *NATSSubscriber) RemoveConsumer(ctx context.Context, name string) error {
	stream, err := s.js.Stream(ctx, s.stream)
	if err != nil {
		return err
	}
	return stream.DeleteConsumer(ctx, name)
}

func (s *NATSSubscriber) DeliveredInfo(ctx context.Context, name string) (any, error) {
	stream, err := s.js.Stream(ctx, s.stream)
	if err != nil {
		return nil, err
	}
	if name != "" {
		consumer, err := stream.Consumer(ctx, name)
		if err != nil {
			return nil, err
		}
		info, err := consumer.Info(ctx)
		if err != nil {
			return nil, err
		}
		return info, nil
	}
	listConsumers := stream.ListConsumers(ctx)
	if listConsumers.Err() != nil {
		return nil, listConsumers.Err()
	}
	listInfo := make([]*jetstream.ConsumerInfo, 0)
	for info := range listConsumers.Info() {
		listInfo = append(listInfo, info)
	}
	return listInfo, nil
}

func (s *NATSSubscriber) HistoryBySeq(ctx context.Context, startSeq uint64) ([]haconnect.HistoryItem, error) {
	max := s.streamSeq
	if startSeq == 0 {
		startSeq = max
	}
	if startSeq == 0 || startSeq > max {
		return nil, fmt.Errorf("cannot retrieve history from %d transactions sequence, only %d transactions available in stream", startSeq, s.streamSeq)
	}

	slog.Debug("retrieve history", "subject", s.subject, "start_seq", startSeq)
	return s.history(ctx, jetstream.ConsumerConfig{
		AckPolicy:         jetstream.AckExplicitPolicy,
		FilterSubject:     s.subject,
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       startSeq,
		MaxAckPending:     1,
		InactiveThreshold: 2 * time.Second,
	}, max)
}

func (s *NATSSubscriber) HistoryByTime(ctx context.Context, duration time.Duration) ([]haconnect.HistoryItem, error) {
	startTime := time.Now().Add(-duration)
	s.mu.Lock()
	defer s.mu.Unlock()
	max := s.streamSeq
	if duration <= 0 {
		return nil, fmt.Errorf("duration must be greater than 0")
	}
	slog.Debug("retrieve history by time", "subject", s.subject, "start_time", startTime)
	return s.history(ctx, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: s.subject,
		DeliverPolicy: jetstream.DeliverByStartTimePolicy,
		OptStartTime:  &startTime,
		MaxAckPending: 1,
	}, max)
}

func (s *NATSSubscriber) UndoBySeq(ctx context.Context, startSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	max := s.streamSeq
	if startSeq == 0 {
		startSeq = max
	}
	if startSeq == 0 || startSeq > max {
		return fmt.Errorf("invalid transaction sequence %d, current stream sequence is %d", startSeq, max)
	}
	slog.Debug("starting undo", "subject", s.subject, "start_seq", startSeq)
	return s.undo(ctx, jetstream.ConsumerConfig{
		AckPolicy:         jetstream.AckExplicitPolicy,
		FilterSubject:     s.subject,
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       startSeq,
		MaxAckPending:     1,
		InactiveThreshold: 2 * time.Second,
	}, max)
}

func (s *NATSSubscriber) UndoByTime(ctx context.Context, duration time.Duration) error {
	startTime := time.Now().Add(-duration)
	s.mu.Lock()
	defer s.mu.Unlock()
	max := s.streamSeq
	if duration <= 0 {
		return fmt.Errorf("duration must be greater than 0")
	}
	slog.Debug("starting undo by time", "subject", s.subject, "start_time", startTime)
	return s.undo(ctx, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: s.subject,
		DeliverPolicy: jetstream.DeliverByStartTimePolicy,
		OptStartTime:  &startTime,
		MaxAckPending: 1,
	}, max)
}

func (s *NATSSubscriber) history(ctx context.Context, cc jetstream.ConsumerConfig, max uint64) ([]haconnect.HistoryItem, error) {
	cons, err := s.js.CreateConsumer(ctx, s.stream, cc)
	if err != nil {
		return nil, err
	}

	items := make([]haconnect.HistoryItem, 0)
	dedup := make(map[uint64]struct{})
	done := make(chan struct{})
	var hasData bool
	iter, err := cons.Consume(func(msg jetstream.Msg) {
		hasData = true
		meta, err := msg.Metadata()
		if err != nil {
			slog.Error("failed to get message metadata for history", "error", err, "subject", msg.Subject())
			return
		}
		if _, ok := dedup[meta.Sequence.Stream]; ok {
			err = msg.Ack()
			if err != nil {
				slog.Error("failed to ack message for history", "error", err, "subject", msg.Subject(), "stream_seq", meta.Sequence.Stream)
			}
			return
		}
		dedup[meta.Sequence.Stream] = struct{}{}
		var cs ChangeSet
		err = json.Unmarshal(msg.Data(), &cs)
		if err != nil {
			slog.Error("failed to unmarshal replication message", "error", err, "stream_seq", cs.StreamSeq)
			s.ack(msg, meta)
			return
		}
		cs.StreamSeq = meta.Sequence.Stream
		cs.SetStrategy(s.applyStrategy)
		item := cs.toItem()
		if len(item.SQL) > 0 {
			items = append(items, item)
		}
		err = msg.Ack()
		if err != nil {
			slog.Error("failed to ack message for undo", "error", err, "subject", msg.Subject(), "stream_seq", meta.Sequence.Stream)
		}
		if meta.Sequence.Stream >= max {
			done <- struct{}{}
			return
		}
	})
	if err != nil {
		return nil, err
	}

	for {
		select {
		case <-iter.Closed():
			return nil, fmt.Errorf("consumer was closed while retrieving history")
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-done:
			iter.Stop()
			return items, nil
		case <-time.After(30 * time.Second):
			if !hasData {
				return nil, fmt.Errorf("timed out: no history data available in stream")
			}
		}
	}
}

func (s *NATSSubscriber) undo(ctx context.Context, cc jetstream.ConsumerConfig, max uint64) error {
	cons, err := s.js.CreateConsumer(ctx, s.stream, cc)
	if err != nil {
		return err
	}

	var undoChangeSet ChangeSet
	undoChangeSet.ProcessID = processID
	undoChangeSet.Node = s.node
	undoChangeSet.Subject = s.subject
	undoChangeSet.SetStrategy(s.applyStrategy)
	undoChangeSet.SetConnProvider(s.connProvider)
	undoChangeSet.SetInterceptor(s.interceptor)

	dedup := make(map[uint64]struct{})
	done := make(chan struct{}, 1)
	var hasData bool
	iter, err := cons.Consume(func(msg jetstream.Msg) {
		hasData = true
		meta, err := msg.Metadata()
		if err != nil {
			slog.Error("failed to get message metadata for undo", "error", err, "subject", msg.Subject())
			return
		}
		if _, ok := dedup[meta.Sequence.Stream]; ok {
			err = msg.Ack()
			if err != nil {
				slog.Error("failed to ack message for undo", "error", err, "subject", msg.Subject(), "stream_seq", meta.Sequence.Stream)
			}
			return
		}
		dedup[meta.Sequence.Stream] = struct{}{}
		var cs ChangeSet
		err = json.Unmarshal(msg.Data(), &cs)
		if err != nil {
			slog.Error("failed to unmarshal replication message", "error", err, "stream_seq", cs.StreamSeq)
			s.ack(msg, meta)
			return
		}
		undoChangeSet.Changes = append(undoChangeSet.Changes, cs.Changes...)
		err = msg.Ack()
		if err != nil {
			slog.Error("failed to ack message for undo", "error", err, "subject", msg.Subject(), "stream_seq", meta.Sequence.Stream)
		}
		if meta.Sequence.Stream >= max {
			done <- struct{}{}
			return
		}
	})
	if err != nil {
		return err
	}

	for {
		select {
		case <-iter.Closed():
			return fmt.Errorf("consumer was closed while retrieving history for undo")
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			iter.Stop()
			undoChangeSet.Changes = reverseChanges(undoChangeSet.Changes)
			slices.Reverse(undoChangeSet.Changes)
			return undoChangeSet.propagate(ctx, s.db)
		case <-time.After(30 * time.Second):
			if !hasData {
				return fmt.Errorf("timed out: no history data available in stream for undo")
			}
		}
	}
}

func (s *NATSSubscriber) handler(msg jetstream.Msg) {
	meta, err := msg.Metadata()
	if err != nil {
		slog.Error("failed to get message metadata", "error", err, "subject", msg.Subject())
		return
	}
	var cs ChangeSet
	cs.StreamSeq = meta.Sequence.Stream
	cs.Subject = s.subject
	cs.SetStrategy(s.applyStrategy)
	cs.SetConnProvider(s.connProvider)
	cs.SetInterceptor(s.interceptor)
	err = json.Unmarshal(msg.Data(), &cs)
	if err != nil {
		slog.Error("failed to unmarshal replication message", "error", err, "stream_seq", cs.StreamSeq)
		s.ack(msg, meta)
		return
	}
	if cs.Node == s.node && cs.ProcessID == processID {
		// Ignore changes originated from this process and node itself
		conn, err := s.db.Conn(context.Background())
		if err != nil {
			slog.Error("failed to get db conn to process replication message", "error", err)
			return
		}
		defer conn.Close()

		err = s.connProvider.DisableHooks(conn)
		if err != nil {
			slog.Error("failed to disable hooks on db conn to process replication message", "error", err)
			return
		}
		defer s.connProvider.EnableHooks(conn)

		_, err = conn.ExecContext(context.Background(),
			"REPLACE INTO "+controlTableName+"(subject, received_seq, updated_at) VALUES(?, ?, ?)",
			s.subject, meta.Sequence.Stream, time.Now().Format(time.RFC3339Nano))
		if err != nil {
			slog.Debug("failed to update "+controlTableName+" table", "subject", s.subject, "seq", meta.Sequence.Stream, "error", err)
		}
		s.ack(msg, meta)
		return
	}
	slog.Debug("received replication message", "subject", msg.Subject(), "node", cs.Node, "changes", len(cs.Changes), "seq", meta.Sequence.Stream)
	err = cs.Apply(s.db)
	if err != nil {
		slog.Error("failed to apply replication message", "error", err, "stream_seq", cs.StreamSeq)
		return
	}
	s.ack(msg, meta)
}

func (s *NATSSubscriber) ack(msg jetstream.Msg, meta *jetstream.MsgMetadata) {
	err := msg.Ack()
	if err != nil {
		slog.Error("failed to ack message", "error", err, "subject", msg.Subject(), "stream_seq", meta.Sequence.Stream)
	}
	s.streamSeq = meta.Sequence.Stream
}

type DBSubscriber struct {
	historyDB     *sql.DB
	db            *sql.DB
	connProvider  ConnHooksProvider
	interceptor   ChangeSetInterceptor
	applyStrategy sqlStrategy

	subject string
}

type DBSubscriberConfig struct {
	HistoryDB    *sql.DB
	DB           *sql.DB
	ConnProvider ConnHooksProvider
	Interceptor  ChangeSetInterceptor
	RowIdentify  RowIdentify
}

func NewDBSubscriber(cfg DBSubscriberConfig) (*DBSubscriber, error) {
	strategy, err := getSqlStrategy(cfg.RowIdentify)
	if err != nil {
		return nil, err
	}
	return &DBSubscriber{
		historyDB:     cfg.HistoryDB,
		db:            cfg.DB,
		connProvider:  cfg.ConnProvider,
		interceptor:   cfg.Interceptor,
		applyStrategy: strategy,
	}, nil
}

func (s *DBSubscriber) DB() *sql.DB {
	return s.db
}

func (s *DBSubscriber) SetDB(db *sql.DB) {
	s.db = db
}

func (s *DBSubscriber) Start() error {
	return s.historyDB.QueryRow("SELECT file FROM pragma_database_list WHERE name = 'main'").Scan(&s.subject)
}

func (s *DBSubscriber) LatestSeq() uint64 {
	if s.historyDB == nil {
		return 0
	}
	var seq sql.NullInt64
	err := s.historyDB.QueryRow("SELECT MAX(seq) FROM ha_changesets").Scan(&seq)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0
		}
		slog.Error("failed to get latest changeset sequence", "error", err)
		return 0
	}
	if !seq.Valid {
		return 0
	}
	return uint64(seq.Int64)
}

func (s *DBSubscriber) RemoveConsumer(ctx context.Context, name string) error {
	return nil
}

func (s *DBSubscriber) DeliveredInfo(ctx context.Context, name string) (any, error) {
	return nil, nil
}

func (s *DBSubscriber) HistoryBySeq(ctx context.Context, startSeq uint64) ([]haconnect.HistoryItem, error) {
	if startSeq == 0 {
		startSeq = s.LatestSeq()
	}
	rows, err := s.historyDB.Query("SELECT seq, changeset FROM ha_changesets WHERE seq >= ? ORDER BY seq ASC", startSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]haconnect.HistoryItem, 0)
	for rows.Next() {
		var seq uint64
		var data []byte
		err = rows.Scan(&seq, &data)
		if err != nil {
			slog.Error("failed to scan changeset row", "error", err)
			continue
		}
		var cs ChangeSet
		err = json.Unmarshal(data, &cs)
		if err != nil {
			slog.Error("failed to unmarshal changeset data", "error", err, "seq", seq)
			continue
		}
		cs.StreamSeq = seq
		cs.SetStrategy(s.applyStrategy)
		item := cs.toItem()
		if len(item.SQL) > 0 {
			items = append(items, item)
		}
	}
	return items, nil
}

func (s *DBSubscriber) HistoryByTime(ctx context.Context, duration time.Duration) ([]haconnect.HistoryItem, error) {
	rows, err := s.historyDB.Query("SELECT seq, changeset FROM ha_changesets WHERE timestamp >= ? ORDER BY seq ASC", fmt.Sprint(time.Now().Add(-duration).UnixNano()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]haconnect.HistoryItem, 0)
	for rows.Next() {
		var seq uint64
		var data []byte
		err = rows.Scan(&seq, &data)
		if err != nil {
			slog.Error("failed to scan changeset row", "error", err)
			continue
		}
		var cs ChangeSet
		err = json.Unmarshal(data, &cs)
		if err != nil {
			slog.Error("failed to unmarshal changeset data", "error", err, "seq", seq)
			continue
		}
		cs.StreamSeq = seq
		cs.SetStrategy(fullIdentifyStrategy{})
		item := cs.toItem()
		if len(item.SQL) > 0 {
			items = append(items, item)
		}
	}
	return items, nil
}

func (s *DBSubscriber) UndoBySeq(ctx context.Context, startSeq uint64) error {
	max := s.LatestSeq()
	if startSeq == 0 {
		startSeq = max
	}
	if startSeq == 0 || startSeq > max {
		return fmt.Errorf("invalid transaction sequence %d, current stream sequence is %d", startSeq, max)
	}
	slog.Debug("starting undo", "subject", s.subject, "start_seq", startSeq)
	return s.undo(ctx, "SELECT changeset FROM ha_changesets WHERE seq >= ? ORDER BY seq ASC", startSeq)
}

func (s *DBSubscriber) UndoByTime(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return fmt.Errorf("duration must be greater than 0")
	}

	startTime := time.Now().Add(-duration)
	slog.Debug("starting undo by time", "subject", s.subject, "start_time", startTime)
	return s.undo(ctx, "SELECT changeset FROM ha_changesets WHERE timestamp >= ? ORDER BY seq ASC", fmt.Sprint(startTime.UnixNano()))
}

func (s *DBSubscriber) undo(ctx context.Context, query string, args ...any) error {
	var undoChangeSet ChangeSet
	undoChangeSet.ProcessID = processID
	undoChangeSet.Subject = s.subject
	undoChangeSet.SetStrategy(s.applyStrategy)
	undoChangeSet.SetConnProvider(s.connProvider)
	undoChangeSet.SetInterceptor(s.interceptor)
	rows, err := s.historyDB.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var data []byte
		err = rows.Scan(&data)
		if err != nil {
			slog.Error("failed to scan changeset row for undo by time", "error", err)
			continue
		}
		var cs ChangeSet
		err = json.Unmarshal(data, &cs)
		if err != nil {
			slog.Error("failed to unmarshal changeset data for undo by time", "error", err)
			continue
		}
		undoChangeSet.Changes = append(undoChangeSet.Changes, cs.Changes...)
	}
	undoChangeSet.Changes = reverseChanges(undoChangeSet.Changes)
	slices.Reverse(undoChangeSet.Changes)
	return undoChangeSet.propagate(ctx, s.db)
}

func reverseChanges(changes []Change) []Change {
	reversed := make([]Change, 0)
	for _, change := range changes {
		if change.Operation == "INSERT" || change.Operation == "DELETE" || change.Operation == "UPDATE" {
			reversed = append(reversed, change.Reverse())
		}
	}
	return reversed
}

func getSqlStrategy(rowIdentify RowIdentify) (sqlStrategy, error) {
	var strategy sqlStrategy
	switch rowIdentify {
	case PK:
		strategy = pkIdentifyStrategy{}
	case Rowid:
		strategy = rowidIdentifyStrategy{}
	case Full:
		strategy = fullIdentifyStrategy{}
	default:
		return nil, fmt.Errorf("invalid row identify strategy: %v", rowIdentify)
	}
	return strategy, nil
}

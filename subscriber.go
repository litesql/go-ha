package ha

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type NoopSubscriber struct{}

func (*NoopSubscriber) SetDB(db *sql.DB) {

}

func (*NoopSubscriber) Start() error {
	return nil
}

func (*NoopSubscriber) LatestSeq() uint64 {
	return 0
}

func (*NoopSubscriber) RemoveConsumer(ctx context.Context, name string) error {
	return nil
}

func (*NoopSubscriber) DeliveredInfo(ctx context.Context, name string) (any, error) {
	return "", nil
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
	applyStrategy applyStrategyFn
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

	var applyStrategy applyStrategyFn
	switch cfg.RowIdentify {
	case PK:
		applyStrategy = pkIdentifyStrategy
	case Rowid:
		applyStrategy = rowidIdentifyStrategy
	case Full:
		applyStrategy = fullIdentifyStrategy
	default:
		return nil, fmt.Errorf("invalid row identify strategy: %v", cfg.RowIdentify)
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
		applyStrategy: applyStrategy,
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

	_, err = conn.ExecContext(context.Background(), "CREATE TABLE IF NOT EXISTS ha_stats(subject TEXT UNIQUE, received_seq INTEGER, updated_at DATETIME)")
	if err != nil {
		return err
	}

	var recv uint64
	err = conn.QueryRowContext(context.Background(), "SELECT received_seq FROM ha_stats WHERE subject = ?", s.subject).Scan(&recv)
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

func (s *NATSSubscriber) handler(msg jetstream.Msg) {
	meta, err := msg.Metadata()
	if err != nil {
		slog.Error("failed to get message metadata", "error", err, "subject", msg.Subject())
		return
	}
	var cs ChangeSet
	cs.StreamSeq = meta.Sequence.Stream
	cs.Subject = s.subject
	cs.SetApplyStrategy(s.applyStrategy)
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
			"REPLACE INTO ha_stats(subject, received_seq, updated_at) VALUES(?, ?, ?)",
			s.subject, meta.Sequence.Stream, time.Now().Format(time.RFC3339Nano))
		if err != nil {
			slog.Debug("failed to update ha_stats table", "subject", s.subject, "seq", meta.Sequence.Stream, "error", err)
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

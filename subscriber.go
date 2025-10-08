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
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type subscriber struct {
	nc        *nats.Conn
	js        jetstream.JetStream
	consumer  jetstream.Consumer
	node      string
	subject   string
	streamSeq uint64
	db        *sql.DB
}

func newSubscriber(node string, nc *nats.Conn, subject string, policy string, db *sql.DB) (*subscriber, error) {
	var (
		deliverPolicy jetstream.DeliverPolicy
		startSeq      uint64
		startTime     *time.Time
	)
	switch policy {
	case "all", "":
		deliverPolicy = jetstream.DeliverAllPolicy
	case "last":
		deliverPolicy = jetstream.DeliverLastPolicy
	case "new":
		deliverPolicy = jetstream.DeliverNewPolicy
	default:
		matched, err := regexp.MatchString(`^by_start_sequence=\d+`, policy)
		if err != nil {
			return nil, err
		}
		if matched {
			deliverPolicy = jetstream.DeliverByStartSequencePolicy
			_, err := fmt.Sscanf(policy, "by_start_sequence=%d", &startSeq)
			if err != nil {
				return nil, fmt.Errorf("invalid CDC subscriber start sequence: %w", err)
			}
			break

		}
		matched, err = regexp.MatchString(`^by_start_time=\w+`, policy)
		if err != nil {
			return nil, err
		}
		if matched {
			deliverPolicy = jetstream.DeliverByStartTimePolicy
			dateTime := strings.TrimPrefix(policy, "by_start_time=")
			t, err := time.Parse(time.DateTime, dateTime)
			if err != nil {
				return nil, fmt.Errorf("invalid CDC subscriber start time: %w", err)
			}
			startTime = &t
			break
		}
		return nil, fmt.Errorf("invalid deliver policy: %s", policy)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	s := subscriber{
		nc:      nc,
		js:      js,
		node:    node,
		subject: subject,
		db:      db,
	}

	consumer, err := s.js.CreateConsumer(context.Background(), s.subject, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: s.subject,
		Durable:       s.node,
		DeliverPolicy: deliverPolicy,
		OptStartSeq:   startSeq,
		OptStartTime:  startTime,
	})
	if err != nil {
		if !errors.Is(err, jetstream.ErrConsumerExists) {
			return nil, err
		}
		consumer, err = s.js.Consumer(context.Background(), subject, s.node)
		if err != nil {
			return nil, err
		}
	}

	_, err = consumer.Consume(s.handler)
	if err != nil {
		slog.Error("failed to start CDC consumer", "error", err, "node", s.node, "subject", s.subject)
		return nil, err
	}
	s.consumer = consumer
	return &s, nil
}

func (s *subscriber) LatestSeq() uint64 {
	return s.streamSeq
}

func (s *subscriber) RemoveConsumer(ctx context.Context, name string) error {
	stream, err := s.js.Stream(ctx, s.subject)
	if err != nil {
		return err
	}
	return stream.DeleteConsumer(ctx, name)
}

func (s *subscriber) DeliveredInfo(ctx context.Context, name string) ([]*jetstream.ConsumerInfo, error) {
	stream, err := s.js.Stream(ctx, s.subject)
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
		return []*jetstream.ConsumerInfo{info}, nil
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

func (s *subscriber) handler(msg jetstream.Msg) {
	meta, err := msg.Metadata()
	if err != nil {
		slog.Error("failed to get message metadata", "error", err, "subject", msg.Subject())
		return
	}
	var cs ChangeSet
	cs.StreamSeq = meta.Sequence.Stream
	err = json.Unmarshal(msg.Data(), &cs)
	if err != nil {
		slog.Error("failed to unmarshal CDC message", "error", err, "stream_seq", cs.StreamSeq)
		s.ack(msg, meta)
		return
	}
	if cs.Node == s.node && cs.ProcessID == processID {
		// Ignore changes originated from this process and node itself
		s.ack(msg, meta)
		return
	}
	slog.Debug("received CDC message", "subject", msg.Subject(), "node", cs.Node, "changes", len(cs.Changes), "seq", meta.Sequence.Stream)
	err = cs.Apply(s.db)
	if err != nil {
		slog.Error("failed to apply CDC message", "error", err, "stream_seq", cs.StreamSeq)
		return
	}
	s.ack(msg, meta)
}

func (s *subscriber) ack(msg jetstream.Msg, meta *jetstream.MsgMetadata) {
	err := msg.Ack()
	if err != nil {
		slog.Error("failed to ack message", "error", err, "subject", msg.Subject(), "stream_seq", meta.Sequence.Stream)
	}
	s.streamSeq = meta.Sequence.Stream
}

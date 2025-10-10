package ha

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var processID = time.Now().UnixNano()

type publisher struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	timeout time.Duration
	subject string
}

func newPublisher(nc *nats.Conn, replicas int, subject string, maxAge time.Duration, timeout time.Duration) (*publisher, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// Create a stream to hold the CDC messages
	streamConfig := jetstream.StreamConfig{
		Name:      subject,
		Replicas:  replicas,
		Subjects:  []string{subject},
		Storage:   jetstream.FileStorage,
		MaxAge:    maxAge,
		Discard:   jetstream.DiscardOld,
		Retention: jetstream.LimitsPolicy,
	}
	_, err = js.CreateOrUpdateStream(ctx, streamConfig)
	if err != nil {
		return nil, err
	}
	return &publisher{
		nc:      nc,
		js:      js,
		timeout: timeout,
		subject: subject,
	}, nil
}

func (p *publisher) Publish(cs *ChangeSet) error {
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
	slog.Debug("published CDC message", "stream", pubAck.Stream, "seq", pubAck.Sequence, "duplicate", pubAck.Duplicate)
	return nil
}

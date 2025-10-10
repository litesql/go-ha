package ha

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
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
	slog.Debug("published CDC message", "stream", pubAck.Stream, "seq", pubAck.Sequence, "duplicate", pubAck.Duplicate)
	return nil
}

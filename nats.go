package ha

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var processID = time.Now().UnixNano()

func runEmbeddedNATSServer(cfg EmbeddedNatsConfig) (*nats.Conn, *server.Server, error) {
	var (
		opts *server.Options
		err  error
	)
	if cfg.File != "" {
		opts, err = server.ProcessConfigFile(cfg.File)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to process nats config file: %w", err)
		}
	} else {
		opts = &server.Options{
			ServerName: cfg.Name,
			Port:       cfg.Port,
			StoreDir:   cfg.StoreDir,
		}
		if cfg.User != "" && cfg.Pass != "" {
			appAcct := server.NewAccount("app")
			appAcct.EnableJetStream(map[string]server.JetStreamAccountLimits{
				"": {
					MaxMemory:    -1,
					MaxStore:     -1,
					MaxStreams:   -1,
					MaxConsumers: -1,
				},
			})
			opts.Accounts = []*server.Account{appAcct}
			opts.Users = []*server.User{
				{
					Username: cfg.User,
					Password: cfg.Pass,
					Account:  appAcct,
				},
			}
		}
	}
	opts.JetStream = true
	opts.DisableJetStreamBanner = true
	if opts.Cluster.Name != "" && opts.ServerName == "" {
		opts.ServerName, err = os.Hostname()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get hostname for nats server name: %w", err)
		}
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, nil, err
	}
	if cfg.EnableLogs {
		ns.ConfigureLogger()
	}
	slog.Debug("starting NATS server", "port", opts.Port, "store_dir", opts.StoreDir)
	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		return nil, nil, err
	}

	if ns.ClusterName() != "" {
		// wait for the leader
		waitForLeader(ns, opts.Cluster.PoolSize)
	}

	slog.Debug("embedded NATS server is ready", "cluster", ns.ClusterName(), "jetstream", ns.JetStreamEnabled())
	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		ns.Shutdown()
		return nil, nil, err
	}
	return nc, ns, nil
}

func waitForLeader(ns *server.Server, size int) {
	for {
		raftz := ns.Raftz(&server.RaftzOptions{})
		if raftz != nil {
			for _, raftGroup := range *raftz {
				for _, raft := range raftGroup {
					if raft.Leader != "" {
						return
					}
				}
			}
		}
		slog.Debug("waiting for the cluster leader", "peers", len(ns.JetStreamClusterPeers()), "size", size)
		time.Sleep(500 * time.Millisecond)
	}
}

type natsPublisher struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	timeout time.Duration
	subject string
}

func newNatsPublisher(nc *nats.Conn, replicas int, subject string, maxAge time.Duration, timeout time.Duration) (*natsPublisher, error) {
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
	return &natsPublisher{
		nc:      nc,
		js:      js,
		timeout: timeout,
		subject: subject,
	}, nil
}

func (p *natsPublisher) Publish(cs *ChangeSet) error {
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

type natsSubscriber struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	consumer jetstream.Consumer
	node     string
	subject  string
	db       *sql.DB
}

func newNatsSubscriber(node string, nc *nats.Conn, subject string, policy string, db *sql.DB) (*natsSubscriber, error) {
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

	s := natsSubscriber{
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

func (s *natsSubscriber) handler(msg jetstream.Msg) {
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
	slog.Info("received CDC message", "subject", msg.Subject(), "node", cs.Node, "changes", len(cs.Changes), "seq", meta.Sequence.Stream)
	err = cs.Apply(s.db)
	if err != nil {
		slog.Error("failed to apply CDC message", "error", err, "stream_seq", cs.StreamSeq)
		return
	}
	s.ack(msg, meta)
}

func (s *natsSubscriber) ack(msg jetstream.Msg, meta *jetstream.MsgMetadata) {
	err := msg.Ack()
	if err != nil {
		slog.Error("failed to ack message", "error", err, "subject", msg.Subject(), "stream_seq", meta.Sequence.Stream)
	}
}

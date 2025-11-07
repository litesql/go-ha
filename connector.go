package ha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const DefaultStream = "ha_replication"

var (
	connectors        = make(map[string]*Connector)
	natsClientServers = make(map[*EmbeddedNatsConfig]*natsClientServer)
	muConnectors      sync.Mutex
)

type ConnHooksProvider interface {
	RegisterHooks(driver.Conn) (driver.Conn, error)
	DisableHooks(*sql.Conn) error
	EnableHooks(*sql.Conn) error
}

type ConnHooksFactory func(nodeName string, filename string, disableDDLSync bool, publisher CDCPublisher) ConnHooksProvider

func NewConnector(dsn string, driver driver.Driver, connHooksFactory ConnHooksFactory, backupFn BackupFn, options ...Option) (*Connector, error) {
	muConnectors.Lock()
	defer muConnectors.Unlock()
	if c, ok := connectors[dsn]; ok {
		return c, nil
	}

	c := Connector{
		dsn:               dsn,
		driver:            driver,
		replicationStream: DefaultStream,
		publisherTimeout:  15 * time.Second,
		replicas:          1,
		natsOptions: []nats.Option{
			nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
				if err != nil {
					slog.Error("NATS got disconnected!", "reason", err)
				}
			}),
			nats.ReconnectHandler(func(nc *nats.Conn) {
				slog.Info("NATS got reconnected!", "url", nc.ConnectedUrl())
			}),
			nats.ClosedHandler(func(nc *nats.Conn) {
				if err := nc.LastError(); err != nil {
					slog.Error("NATS connection closed.", "reason", err)
				}
			}),
		},
	}
	for _, opt := range options {
		opt(&c)
	}
	if c.name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("failed to get hostname, define a replication name using ha.WithName(\"node_name\") option: %w", err)
		}
		c.name = hostname
	}
	var (
		err      error
		natsConn *nats.Conn
	)
	if c.embeddedNatsConfig != nil {
		var (
			ncs *natsClientServer
			ok  bool
		)
		if ncs, ok = natsClientServers[c.embeddedNatsConfig]; !ok {
			ncs, err = runEmbeddedNATSServer(*c.embeddedNatsConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to start embedded NATS server: %w", err)
			}
			natsClientServers[c.embeddedNatsConfig] = ncs
			natsConn = ncs.client
		} else {
			ncs.count++
			natsClientServers[c.embeddedNatsConfig] = ncs
			natsConn = ncs.client
		}
	}

	if natsConn == nil && c.replicationURL != "" {
		natsConn, err = nats.Connect(c.replicationURL, c.natsOptions...)
		if err != nil {
			return nil, fmt.Errorf("failed to conect to NATS server at %q: %w", c.replicationURL, err)
		}
	}

	filename := filenameFromDSN(dsn)
	subject := c.replicationStream
	if filename != "" {
		filename = filepath.Base(filename)
		subject = fmt.Sprintf("%s.%s", c.replicationStream, normalizeNatsIdentifier(filename))
	}

	if natsConn != nil && c.publisher == nil {
		streamConfig := jetstream.StreamConfig{
			Name:      c.replicationStream,
			Replicas:  c.replicas,
			Subjects:  []string{c.replicationStream, fmt.Sprintf("%s.>", c.replicationStream)},
			Storage:   jetstream.FileStorage,
			MaxAge:    c.streamMaxAge,
			Discard:   jetstream.DiscardOld,
			Retention: jetstream.LimitsPolicy,
		}
		if c.asyncPublisher {
			db := sql.OpenDB(&noHooksConnector{
				driver: driver,
				dsn:    "file:" + filepath.Join(c.asyncPublisherOutboxDir, strings.TrimSuffix(filename, ".db")+"_outbox.db"),
			})

			asyncPublisher, err := NewAsyncNATSPublisher(natsConn, subject, c.publisherTimeout, &streamConfig, db)
			if err != nil {
				return nil, fmt.Errorf("failed to start async NATS publisher: %w", err)
			}
			c.publisher = asyncPublisher
			c.closers = append(c.closers, asyncPublisher)
			c.closers = append(c.closers, db)
		} else {
			c.publisher, err = NewNATSPublisher(natsConn, subject, c.publisherTimeout, &streamConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to start NATS publisher: %w", err)
			}
		}
	}

	c.connHooksProvider = connHooksFactory(c.name, filename, c.disableDDLSync, c.publisher)

	if natsConn != nil {
		db := sql.OpenDB(&c)
		c.closers = append(c.closers, db)
		if c.subscriber == nil {
			durable := normalizeNatsIdentifier(fmt.Sprintf("%s_%s", filename, c.name))
			c.subscriber, err = NewNATSSubscriber(c.name, durable, natsConn, c.replicationStream, subject, c.deliverPolicy, db, c.connHooksProvider, c.interceptor)
			if err != nil {
				return nil, fmt.Errorf("failed to create NATS subscriber: %w", err)
			}
		}
		if c.snapshotter == nil {
			c.snapshotter, err = NewNATSSnapshotter(context.Background(), natsConn, c.replicas, c.replicationStream, db, backupFn, c.snapshotInterval, c.subscriber, filename)
			if err != nil {
				return nil, fmt.Errorf("failed to create NATS snapshotter: %w", err)
			}
		}
	}
	if c.waitFor == nil {
		if c.subscriber != nil {
			err = c.subscriber.Start()
			if err != nil {
				return nil, fmt.Errorf("failed to start subscriber: %w", err)
			}
		}
		if c.snapshotter != nil {
			c.snapshotter.Start()
		}
	} else {
		go func() {
			<-c.waitFor
			if c.subscriber != nil {
				err := c.subscriber.Start()
				if err != nil {
					slog.Error("failed to start subscriber", "error", err)
				}
			}
			if c.snapshotter != nil {
				c.snapshotter.Start()
			}
		}()
	}

	connectors[dsn] = &c
	return &c, nil
}

type Connector struct {
	driver                  driver.Driver
	connHooksProvider       ConnHooksProvider
	dsn                     string
	name                    string
	extensions              []string
	embeddedNatsConfig      *EmbeddedNatsConfig
	replicas                int
	streamMaxAge            time.Duration
	replicationURL          string
	natsOptions             []nats.Option
	replicationStream       string
	deliverPolicy           string
	publisherTimeout        time.Duration
	asyncPublisher          bool
	asyncPublisherOutboxDir string
	snapshotInterval        time.Duration
	disableDDLSync          bool
	waitFor                 chan struct{}

	publisher   CDCPublisher
	subscriber  CDCSubscriber
	interceptor ChangeSetInterceptor
	snapshotter DBSnapshotter

	closers []io.Closer
}

func GetConnector(dsn string) *Connector {
	muConnectors.Lock()
	defer muConnectors.Unlock()
	return connectors[dsn]
}

var (
	ErrSubscriberNotConfigured  = errors.New("subscriber not configured")
	ErrSnapshotterNotConfigured = errors.New("snapshotter not configured")
)

func (c *Connector) NodeName() string {
	return c.name
}

func (c *Connector) Publisher() CDCPublisher {
	return c.publisher
}

func (c *Connector) Subscriber() CDCSubscriber {
	return c.subscriber
}

func (c *Connector) Snapshotter() DBSnapshotter {
	return c.snapshotter
}

func (c *Connector) DeliveredInfo(ctx context.Context, name string) (any, error) {
	if c.subscriber == nil {
		return nil, ErrSubscriberNotConfigured
	}
	return c.subscriber.DeliveredInfo(ctx, name)
}

func (c *Connector) RemoveConsumer(ctx context.Context, name string) error {
	if c.subscriber == nil {
		return ErrSubscriberNotConfigured
	}
	return c.subscriber.RemoveConsumer(ctx, name)
}

func (c *Connector) TakeSnapshot(ctx context.Context) (sequence uint64, err error) {
	if c.snapshotter == nil {
		return 0, ErrSnapshotterNotConfigured
	}
	return c.snapshotter.TakeSnapshot(ctx)
}

func (c *Connector) LatestSnapshot(ctx context.Context) (uint64, io.ReadCloser, error) {
	if c.snapshotter == nil {
		return 0, nil, ErrSnapshotterNotConfigured
	}
	return c.snapshotter.LatestSnapshot(ctx)
}

func (c *Connector) LatestSeq() uint64 {
	if c.subscriber == nil {
		return 0
	}
	return c.subscriber.LatestSeq()
}

func (c *Connector) Close() {
	for _, closer := range c.closers {
		closer.Close()
	}

	muConnectors.Lock()
	defer muConnectors.Unlock()

	delete(connectors, c.dsn)

	if c.embeddedNatsConfig != nil {
		ncs, ok := natsClientServers[c.embeddedNatsConfig]
		if ok {
			ncs.count--
			natsClientServers[c.embeddedNatsConfig] = ncs
			if ncs.count < 0 {
				if !ncs.client.IsClosed() {
					ncs.client.Close()
				}
				ncs.server.WaitForShutdown()
			}
			delete(natsClientServers, c.embeddedNatsConfig)
		}
		return
	}
}

func (c *Connector) Driver() driver.Driver {
	return c.driver
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return c.connHooksProvider.RegisterHooks(conn)
}

type noHooksConnector struct {
	driver driver.Driver
	dsn    string
}

func (c *noHooksConnector) Driver() driver.Driver {
	return c.driver
}

func (c *noHooksConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.driver.Open(c.dsn)
}

func LatestSnapshot(ctx context.Context, dsn string, options ...Option) (sequence uint64, reader io.ReadCloser, err error) {
	var c Connector
	for _, opt := range options {
		opt(&c)
	}
	muConnectors.Lock()
	defer muConnectors.Unlock()
	var nc *nats.Conn
	if c.embeddedNatsConfig != nil {
		if ncs, ok := natsClientServers[c.embeddedNatsConfig]; ok {
			nc = ncs.client
		} else {
			ncs, err = runEmbeddedNATSServer(*c.embeddedNatsConfig)
			if err != nil {
				return 0, nil, err
			}
			natsClientServers[c.embeddedNatsConfig] = ncs
			nc = ncs.client
		}
	}
	if nc == nil {
		if c.replicationURL == "" {
			return 0, nil, fmt.Errorf("embedded NATS or replicationURL not configured")
		}
		nc, err = nats.Connect(c.replicationURL, c.natsOptions...)
		if err != nil {
			return 0, nil, err
		}
		defer nc.Close()
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return 0, nil, err
	}
	bucketName := c.replicationStream + "_SNAPSHOTS"
	objectStore, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      bucketName,
		Storage:     jetstream.FileStorage,
		Compression: true,
		Replicas:    c.replicas,
	})
	if err != nil {
		if !errors.Is(err, jetstream.ErrBucketExists) {
			return 0, nil, err
		}
		objectStore, err = js.ObjectStore(ctx, bucketName)
		if err != nil {
			return 0, nil, err
		}
	}
	objectName := filenameFromDSN(dsn)
	info, err := objectStore.GetInfo(ctx, objectName)
	if err != nil {
		return 0, nil, err
	}
	sequenceStr := info.Headers.Get("seq")
	if sequenceStr != "" {
		sequence, err = strconv.ParseUint(sequenceStr, 10, 64)
		if err != nil {
			return 0, nil, fmt.Errorf("convert sequence header: %w", err)
		}
	}

	reader, err = objectStore.Get(ctx, objectName)
	return sequence, reader, err
}

type CDCPublisher interface {
	Publish(cs *ChangeSet) error
}

type CDCSubscriber interface {
	SetDB(*sql.DB)
	Start() error
	LatestSeq() uint64
	RemoveConsumer(ctx context.Context, name string) error
	DeliveredInfo(ctx context.Context, name string) (any, error)
}

type DriverProvider interface {
	driver.Driver
	ConnWithoutHooks() (*sql.Conn, error)
	EnableHooks(conn *sql.Conn)
	OnConnect(c driver.Conn) (driver.Conn, error)
}

type ChangeSetInterceptor interface {
	BeforeApply(*ChangeSet, *sql.Conn) (skip bool, err error)
	AfterApply(*ChangeSet, *sql.Conn, error) error
}

type DBSnapshotter interface {
	SetDB(*sql.DB)
	Start()
	TakeSnapshot(ctx context.Context) (sequence uint64, err error)
	LatestSnapshot(ctx context.Context) (sequence uint64, reader io.ReadCloser, err error)
}

func filenameFromDSN(dsn string) string {
	var filename string
	u, err := url.Parse(dsn)
	if err == nil {
		filename = u.Path
	}
	if filename == "" {
		filename = strings.TrimPrefix(dsn, "file:")
		if i := strings.Index(filename, "?"); i > 0 {
			filename = filename[0:i]
		}
	}
	if filename != "" {
		return filepath.Base(filename)
	}
	return ""
}

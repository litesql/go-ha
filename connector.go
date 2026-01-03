package ha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	hagrpc "github.com/litesql/go-ha/wire/grpc"
	"github.com/litesql/go-ha/wire/mysql"
)

const DefaultStream = "ha_replication"

var (
	connectors        = make(map[string]*Connector)
	natsClientServers = make(map[*EmbeddedNatsConfig]*natsClientServer)
	grpcServers       = make(map[int]*hagrpc.Server)
	mysqlServers      = make(map[int]*mysql.Server)
	muConnectors      sync.RWMutex
)

type ConnHooksProvider interface {
	RegisterHooks(driver.Conn) (driver.Conn, error)
	DisableHooks(*sql.Conn) error
	EnableHooks(*sql.Conn) error
}

type TxSeqTracker interface {
	LatestSeq() uint64
}

type TxSeqTrackerProvider func() TxSeqTracker

type ConnHooksConfig struct {
	NodeName             string
	ReplicationID        string
	DisableDDLSync       bool
	Publisher            Publisher
	CDC                  CDCPublisher
	TxSeqTrackerProvider TxSeqTrackerProvider
	Leader               LeaderProvider
	GrpcTimeout          time.Duration
}

type ConnHooksFactory func(cfg ConnHooksConfig) ConnHooksProvider

func NewConnector(dsn string, drv driver.Driver, connHooksFactory ConnHooksFactory, backupFn BackupFn, options ...Option) (*Connector, error) {
	if c, ok := LookupConnector(dsn); ok {
		return c, nil
	}

	muConnectors.Lock()
	defer muConnectors.Unlock()

	c := Connector{
		clusterSize:       1,
		dsn:               dsn,
		driver:            drv,
		rowIdentify:       PK,
		replicationStream: DefaultStream,
		publisherTimeout:  15 * time.Second,
		grpcTimeout:       5 * time.Second,
		replicas:          1,
		leaderProvider:    &StaticLeader{},
		autoStart:         true,
		mysqlUser:         "root",
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
			nats.MaxReconnects(-1),
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
			return nil, fmt.Errorf("failed to connect to NATS server at %q: %w", c.replicationURL, err)
		}
	}

	if c.replicationID == "" {
		c.replicationID = filenameFromDSN(dsn)
		if c.replicationID != "" {
			c.replicationID = filepath.Base(c.replicationID)
		}
	}
	subject := c.replicationStream
	if c.replicationID != "" {
		subject = fmt.Sprintf("%s.%s", c.replicationStream, normalizeNatsIdentifier(c.replicationID))
	}

	if c.leaderElectionLocalTarget != "" {
		if natsConn == nil {
			return nil, fmt.Errorf("no NATS connection available to start leader election")
		}
		leaderProvider, err := startLeaderElection(context.Background(),
			c.leaderElectionLocalTarget, natsConn, subject, c.clusterSize, filepath.Join(os.TempDir(), fmt.Sprintf("ha-election-%s.log", c.name)))
		if err != nil {
			return nil, fmt.Errorf("failed to start leader election: %w", err)
		}
		c.leaderProvider = leaderProvider
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
				driver: drv,
				dsn:    "file:" + filepath.Join(c.asyncPublisherOutboxDir, strings.TrimSuffix(c.replicationID, ".db")+"_outbox.db"),
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

	c.connHooksProvider = connHooksFactory(ConnHooksConfig{
		NodeName:       c.name,
		ReplicationID:  c.replicationID,
		DisableDDLSync: c.disableDDLSync,
		Publisher:      c.publisher,
		CDC:            c.cdcPublisher,
		TxSeqTrackerProvider: func() TxSeqTracker {
			return c.subscriber
		},
		Leader:      c.leaderProvider,
		GrpcTimeout: c.grpcTimeout,
	})
	c.db = sql.OpenDB(&c)
	c.closers = append(c.closers, c.db)
	if natsConn != nil {
		if c.subscriber == nil {
			durable := normalizeNatsIdentifier(fmt.Sprintf("%s_%s", c.replicationID, c.name))
			c.subscriber, err = NewNATSSubscriber(NATSSubscriberConfig{
				Node:         c.name,
				Durable:      durable,
				NatsConn:     natsConn,
				Stream:       c.replicationStream,
				Subject:      subject,
				Policy:       c.deliverPolicy,
				DB:           c.db,
				ConnProvider: c.connHooksProvider,
				Interceptor:  c.interceptor,
				RowIdentify:  c.rowIdentify,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create NATS subscriber: %w", err)
			}
		}
		if c.snapshotter == nil {
			c.snapshotter, err = NewNATSSnapshotter(context.Background(), natsConn, c.replicas, c.replicationStream, c.db, backupFn, c.snapshotInterval, c.subscriber, c.replicationID)
			if err != nil {
				return nil, fmt.Errorf("failed to create NATS snapshotter: %w", err)
			}
		}
	}
	if c.publisher == nil {
		c.publisher = NewNoopPublisher()
	}
	if c.subscriber == nil {
		c.subscriber = NewNoopSubscriber()
	}
	if c.autoStart {
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
			c.publisher = &delayedStartPublisher{
				pub: c.publisher,
			}
			go func() {
				<-c.waitFor
				if delayedStartPub, ok := c.publisher.(*delayedStartPublisher); ok {
					c.publisher = delayedStartPub.pub
				}
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
	}

	if c.grpcPort > 0 {
		if _, ok := grpcServers[c.grpcPort]; !ok {
			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", c.grpcPort))
			if err != nil {
				return nil, fmt.Errorf("failed to start gRPC server: %w", err)
			}
			s := grpc.NewServer()
			sqlv1.RegisterDatabaseServiceServer(s, &hagrpc.Service{
				DBProvider: func(id string) (hagrpc.HADB, bool) {
					connector, ok := LookupConnectorByReplicationID(id)
					if !ok {
						return nil, false
					}
					return connector, true
				},
			})
			slog.Info("gRPC server listening", "addr", lis.Addr())
			go func() {
				if err := s.Serve(lis); err != nil {
					log.Fatalf("failed to serve grpc: %v", err)
				}
			}()
			grpcServers[c.grpcPort] = &hagrpc.Server{
				Server: s,
			}
		} else {
			grpcServers[c.grpcPort].ReferenceCount++
		}
	}

	if c.mysqlPort > 0 {
		if _, ok := mysqlServers[c.mysqlPort]; !ok {
			mysqlServer := &mysql.Server{
				DBProvider: func(dbName string) (*sql.DB, bool) {
					connector, ok := LookupConnector(dbName)
					if !ok {
						return nil, false
					}
					return connector.db, true
				},
				Databases: ListDSN,
				Port:      c.mysqlPort,
				User:      c.mysqlUser,
				Pass:      c.mysqlPass,
			}
			err := mysqlServer.ListenAndServe()
			if err != nil {
				return nil, fmt.Errorf("failed to start MySQL server on port %d: %w", c.mysqlPort, err)
			}
			mysqlServers[c.mysqlPort] = mysqlServer
		} else {
			mysqlServers[c.mysqlPort].ReferenceCount++
		}
	}

	connectors[dsn] = &c
	return &c, nil
}

func Shutdown() {
	for _, connector := range connectors {
		connector.Close()
	}
}

type RowIdentify string

const (
	PK    RowIdentify = "pk"
	Rowid RowIdentify = "rowid"
	Full  RowIdentify = "full"
)

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
	replicationID           string
	rowIdentify             RowIdentify
	autoStart               bool
	clusterSize             int
	waitFor                 chan struct{}

	publisher   Publisher
	subscriber  Subscriber
	interceptor ChangeSetInterceptor
	snapshotter DBSnapshotter

	cdcPublisher CDCPublisher

	leaderElectionLocalTarget string
	leaderProvider            LeaderProvider

	db *sql.DB

	grpcPort    int
	grpcTimeout time.Duration

	mysqlPort int
	mysqlUser string
	mysqlPass string

	closers []io.Closer
}

func LookupConnector(dsn string) (*Connector, bool) {
	key, _, _ := NameToOptions(dsn)
	muConnectors.RLock()
	defer muConnectors.RUnlock()
	conn, ok := connectors[key]
	return conn, ok
}

func ListDSN() []string {
	muConnectors.RLock()
	defer muConnectors.RUnlock()
	keys := make([]string, 0, len(connectors))
	for k := range connectors {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func LookupConnectorByReplicationID(id string) (*Connector, bool) {
	muConnectors.RLock()
	defer muConnectors.RUnlock()
	for _, conn := range connectors {
		if conn.replicationID == id {
			return conn, true
		}
	}
	return nil, false
}

func ListReplicationIDs() []string {
	muConnectors.RLock()
	defer muConnectors.RUnlock()
	set := make(map[string]struct{})

	for _, c := range connectors {
		if c.replicationID == "" {
			continue
		}
		set[c.replicationID] = struct{}{}
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

var ErrSnapshotterNotConfigured = errors.New("snapshotter not configured")

func (c *Connector) Start(db *sql.DB) error {
	c.db = db
	if c.autoStart {
		return nil
	}
	if delayedStartPub, ok := c.publisher.(*delayedStartPublisher); ok {
		c.publisher = delayedStartPub.pub
	}
	var err error
	if c.subscriber != nil {
		if db != nil {
			c.subscriber.SetDB(db)
		}
		err = c.subscriber.Start()
	}
	if c.snapshotter != nil {
		if db != nil {
			c.snapshotter.SetDB(db)
		}
		c.snapshotter.Start()
	}
	return err
}

func (c *Connector) NodeName() string {
	return c.name
}

func (c *Connector) Publisher() Publisher {
	return c.publisher
}

func (c *Connector) CDCPublisher() CDCPublisher {
	return c.cdcPublisher
}

func (c *Connector) Subscriber() Subscriber {
	return c.subscriber
}

func (c *Connector) Snapshotter() DBSnapshotter {
	return c.snapshotter
}

func (c *Connector) DB() *sql.DB {
	return c.db
}

func (c *Connector) DeliveredInfo(ctx context.Context, name string) (any, error) {
	return c.subscriber.DeliveredInfo(ctx, name)
}

func (c *Connector) RemoveConsumer(ctx context.Context, name string) error {
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

func (c *Connector) PubSeq() uint64 {
	if c.publisher == nil {
		return 0
	}
	return c.publisher.Sequence()
}

func (c *Connector) LatestSeq() uint64 {
	return c.subscriber.LatestSeq()
}

func (c *Connector) LeaderProvider() LeaderProvider {
	return c.leaderProvider
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
			if ncs.count <= 0 {
				if !ncs.client.IsClosed() {
					ncs.client.Close()
				}
				ncs.server.WaitForShutdown()
				delete(natsClientServers, c.embeddedNatsConfig)
			}
		}
		return
	}

	if c.grpcPort > 0 {
		if server, ok := grpcServers[c.grpcPort]; ok {
			server.ReferenceCount--
			if server.ReferenceCount <= 0 {
				server.GracefulStop()
				delete(grpcServers, c.grpcPort)
			}
		}
	}

	if c.mysqlPort > 0 {
		if server, ok := mysqlServers[c.mysqlPort]; ok {
			server.ReferenceCount--
			if server.ReferenceCount <= 0 {
				server.Close()
				delete(mysqlServers, c.mysqlPort)
			}
		}
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

type Publisher interface {
	Publish(cs *ChangeSet) error
	Sequence() uint64
}

type CDCPublisher interface {
	Publish(data []DebeziumData) error
}

type Subscriber interface {
	TxSeqTracker
	SetDB(*sql.DB)
	Start() error
	RemoveConsumer(ctx context.Context, name string) error
	DeliveredInfo(ctx context.Context, name string) (any, error)
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

type LeaderProvider interface {
	IsLeader() bool
	Ready() chan struct{}
	RedirectTarget() string
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

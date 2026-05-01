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
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	haconnect "github.com/litesql/go-ha/connect"
)

type Connector struct {
	driver                  driver.Driver
	connHooksProvider       ConnHooksProvider
	backupFn                BackupFn
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

	db          *sql.DB
	queryRouter *regexp.Regexp

	grpcPort     int
	grpcTimeout  time.Duration
	grpcToken    string
	grpcInsecure bool

	proxiedDB *sql.DB

	closers []io.Closer
}

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
		backupFn:          backupFn,
		rowIdentify:       PK,
		replicationStream: DefaultStream,
		publisherTimeout:  15 * time.Second,
		grpcTimeout:       5 * time.Second,
		replicas:          1,
		leaderProvider:    &StaticLeader{},
		autoStart:         true,
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

	if c.replicationURL != "" {
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

	var (
		localDBPub *DBPublisher
		localDBSub *DBSubscriber
	)
	if c.streamMaxAge > 0 && c.publisher == nil && c.subscriber == nil {
		localDBPub, err = NewDBPublisher(nil, c.streamMaxAge)
		if err != nil {
			return nil, fmt.Errorf("failed to start DB publisher: %w", err)
		}
		c.closers = append(c.closers, localDBPub)

		c.publisher = localDBPub

		localDBSub, err = NewDBSubscriber(DBSubscriberConfig{
			ConnProvider: c.connHooksProvider,
			Interceptor:  c.interceptor,
			RowIdentify:  c.rowIdentify,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to start DB subscriber: %w", err)
		}
		c.subscriber = localDBSub
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
		Leader:       c.leaderProvider,
		GrpcTimeout:  c.grpcTimeout,
		GrpcToken:    c.grpcToken,
		GrpcInsecure: c.grpcInsecure,
		QueryRouter:  c.queryRouter,
	})

	if localDBPub != nil && localDBSub != nil {
		tempDB := sql.OpenDB(&c)
		defer tempDB.Close()
		var filename string
		err := tempDB.QueryRow("SELECT file FROM pragma_database_list WHERE name = 'main'").Scan(&filename)
		if err != nil {
			return nil, fmt.Errorf("failed to get database filename: %w", err)
		}

		historyDB := sql.OpenDB(&noHooksConnector{
			driver: drv,
			dsn:    "file:" + filepath.Join(filepath.Dir(filename), strings.TrimSuffix(c.replicationID, ".db")+"_history.db"),
		})
		c.closers = append(c.closers, historyDB)

		_, err = historyDB.Exec(`PRAGMA journal_mode=WAL; 
			CREATE TABLE IF NOT EXISTS ha_changesets(
				seq INTEGER PRIMARY KEY AUTOINCREMENT, 
				changeset JSONB,
				timestamp INTEGER GENERATED ALWAYS AS (jsonb_extract(changeset, '$.timestamp_ns')) VIRTUAL
			);
			CREATE INDEX IF NOT EXISTS idx_timestamp 
				ON ha_changesets(timestamp);`)
		if err != nil {
			return nil, fmt.Errorf("create changesets table: %w", err)
		}
		localDBPub.db = historyDB
		localDBSub.historyDB = historyDB
	}
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
			c.db = sql.OpenDB(&c)
			c.closers = append(c.closers, c.db)
			if c.subscriber.DB() == nil {
				c.subscriber.SetDB(c.db)
			}
			err = c.subscriber.Start()
			if err != nil {
				return nil, fmt.Errorf("failed to start subscriber: %w", err)
			}
			if c.snapshotter != nil {
				if c.snapshotter.DB() == nil {
					c.snapshotter.SetDB(c.db)
				}
				c.snapshotter.Start()
			}
		} else {
			c.publisher = &delayedStartPublisher{
				pub: c.publisher,
			}
			go func() {
				<-c.waitFor
				c.db = sql.OpenDB(&c)
				c.closers = append(c.closers, c.db)
				if c.subscriber.DB() == nil {
					c.subscriber.SetDB(c.db)
				}
				if delayedStartPub, ok := c.publisher.(*delayedStartPublisher); ok {
					c.publisher = delayedStartPub.pub
				}
				err := c.subscriber.Start()
				if err != nil {
					slog.Error("failed to start subscriber", "error", err)
				}
				if c.snapshotter != nil {
					if c.snapshotter.DB() == nil {
						c.snapshotter.SetDB(c.db)
					}
					c.snapshotter.Start()
				}
			}()
		}
	} else {
		c.db = sql.OpenDB(&c)
		c.closers = append(c.closers, c.db)

		if c.subscriber.DB() == nil {
			c.subscriber.SetDB(c.db)
		}
		if c.snapshotter != nil && c.snapshotter.DB() == nil {
			c.snapshotter.SetDB(c.db)
		}
	}

	if c.grpcPort > 0 {
		if _, ok := grpcServers[c.grpcPort]; !ok {
			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", c.grpcPort))
			if err != nil {
				return nil, fmt.Errorf("failed to start gRPC server: %w", err)
			}
			opts := make([]connect.HandlerOption, 0)
			if c.grpcToken == "" {
				slog.Warn("no gRPC token configured, the gRPC server will be unauthenticated. Do not use this configuration in production environments!")
			} else {
				authInterceptor := haconnect.NewAuthInterceptor(c.grpcToken)
				opts = append(opts, connect.WithInterceptors(authInterceptor))
			}
			path, handler := ConnectHandler(opts...)
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			p := new(http.Protocols)
			p.SetHTTP1(true)
			p.SetUnencryptedHTTP2(true)
			s := http.Server{
				Handler:   mux,
				Protocols: p,
			}
			slog.Info("HA gRPC/connect server listening", "addr", lis.Addr())
			go func() {
				if err := s.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Fatalf("failed to serve grpc: %v", err)
				}
			}()
			grpcServers[c.grpcPort] = &haconnect.Server{
				Server: &s,
			}
		} else {
			grpcServers[c.grpcPort].ReferenceCount++
		}
	}

	slog.Debug("go-ha connector created", "dsn", dsn, "replication_id", c.replicationID, "node_name", c.name, "pub", fmt.Sprintf("%T", c.publisher), "sub", fmt.Sprintf("%T", c.subscriber), "snapshotter", fmt.Sprintf("%T", c.snapshotter))

	connectors[dsn] = &c
	return &c, nil
}

func (c *Connector) SetProxiedDB(db *sql.DB) {
	c.proxiedDB = db
}

func (c *Connector) ProxiedDB() *sql.DB {
	return c.proxiedDB
}

func (c *Connector) Start(db *sql.DB) error {
	c.db = db
	if c.autoStart {
		return nil
	}
	if delayedStartPub, ok := c.publisher.(*delayedStartPublisher); ok {
		c.publisher = delayedStartPub.pub
	}
	var err error

	if db != nil {
		c.subscriber.SetDB(db)
	}
	err = c.subscriber.Start()

	if c.snapshotter != nil {
		if db != nil {
			c.snapshotter.SetDB(db)
		}
		c.snapshotter.Start()
	}
	return err
}

func (c *Connector) Backup(ctx context.Context, writer io.Writer) error {
	if c.backupFn == nil {
		return fmt.Errorf("backup function not configured")
	}
	return c.backupFn(ctx, c.db, writer)
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

func (c *Connector) HistoryBySeq(ctx context.Context, startSeq uint64) ([]haconnect.HistoryItem, error) {
	return c.subscriber.HistoryBySeq(ctx, startSeq)
}

func (c *Connector) HistoryByTime(ctx context.Context, duration time.Duration) ([]haconnect.HistoryItem, error) {
	return c.subscriber.HistoryByTime(ctx, duration)
}

func (c *Connector) UndoBySeq(ctx context.Context, startSeq uint64, filterType haconnect.UndoFilter, filterEntities map[string][]int64) error {
	return c.subscriber.UndoBySeq(ctx, startSeq, filterType, filterEntities)
}

func (c *Connector) UndoByTime(ctx context.Context, duration time.Duration, filterType haconnect.UndoFilter, filterEntities map[string][]int64) error {
	return c.subscriber.UndoByTime(ctx, duration, filterType, filterEntities)
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
				delete(natsClientServers, c.embeddedNatsConfig)
				if ncs.server.Running() {
					go ncs.server.Shutdown()
					ncs.server.WaitForShutdown()
				}
			}
		}
	}

	if c.grpcPort > 0 {
		if server, ok := grpcServers[c.grpcPort]; ok {
			server.ReferenceCount--
			if server.ReferenceCount <= 0 {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				server.Shutdown(ctx)
				delete(grpcServers, c.grpcPort)
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
	return c.connHooksProvider.RegisterHooks(conn, c)
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

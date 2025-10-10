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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/litesql/go-sqlite3"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const DefaultStream = "ha_replication"

var (
	connectors        = make(map[string]*Connector)
	natsClientServers = make(map[*EmbeddedNatsConfig]*natsClientServer)
	muConnectors      sync.Mutex
)

func init() {
	sql.Register("ha", &Driver{})
}

type Driver struct {
	Extensions  []string
	ConnectHook ConnectHookFn
	Options     []Option
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	dsn, opts, err := nameToOptions(name)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	opts = append(opts, d.Options...)
	if len(d.Extensions) > 0 {
		opts = append(opts, WithExtensions(d.Extensions...))
	}
	if d.ConnectHook != nil {
		opts = append(opts, WithConnectHook(d.ConnectHook))
	}
	return NewConnector(dsn, opts...)
}

func NewConnector(dsn string, options ...Option) (*Connector, error) {
	muConnectors.Lock()
	defer muConnectors.Unlock()
	if c, ok := connectors[dsn]; ok {
		return c, nil
	}

	c := Connector{
		dsn:                dsn,
		replicationSubject: DefaultStream,
		publisherTimeout:   15 * time.Second,
		replicas:           1,
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
	var err error
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
			c.nc = ncs.client
			c.ns = ncs.server
		} else {
			ncs.count++
			natsClientServers[c.embeddedNatsConfig] = ncs
			c.nc = ncs.client
			c.ns = ncs.server
		}

	}

	if c.nc == nil && c.replicationURL != "" {
		c.nc, err = nats.Connect(c.replicationURL, c.natsOptions...)
		if err != nil {
			return nil, fmt.Errorf("failed to conect to NATS server at %q: %w", c.replicationURL, err)
		}
	}

	if c.nc != nil && c.publisher == nil {
		c.publisher, err = newPublisher(c.nc, c.replicas, c.replicationSubject, c.streamMaxAge, c.publisherTimeout)
		if err != nil {
			return nil, fmt.Errorf("failed to start NATS publisher: %w", err)
		}
	}

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

	c.driver = &sqlite3.SQLiteDriver{
		Extensions: c.extensions,
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if c.connectHook != nil {
				if err := c.connectHook(conn); err != nil {
					return err
				}
			}
			enableCDCHooks(conn, filename, c.name, c.publisher)
			return nil
		},
	}

	if c.nc != nil {
		db := sql.OpenDB(&c)
		durable := normalizeNatsIdentifier(fmt.Sprintf("%s_%s", dsn, c.name))

		c.subscriber, err = newSubscriber(c.name, durable, c.nc, c.replicationSubject, c.deliverPolicy, filename, db)
		if err != nil {
			return nil, fmt.Errorf("failed to start NATS subscriber: %w", err)
		}
		if c.waitFor == nil {
			err = c.subscriber.startConsumer()
			if err != nil {
				return nil, fmt.Errorf("failed to start NATS subscriber consumer: %w", err)
			}
		} else {
			go func() {
				<-c.waitFor
				c.subscriber.startConsumer()
			}()
		}
		c.snapshotter, err = newSnapshotter(context.Background(), c.nc, c.replicas, c.replicationSubject, db, c.snapshotInterval)
		if err != nil {
			return nil, fmt.Errorf("failed to start NATS snapshotter: %w", err)
		}
		c.snapshotter.SetSeqProvider(c.subscriber)
	}
	connectors[dsn] = &c
	return &c, nil
}

type Connector struct {
	driver             driver.Driver
	dsn                string
	connectHook        ConnectHookFn
	name               string
	extensions         []string
	embeddedNatsConfig *EmbeddedNatsConfig
	replicas           int
	streamMaxAge       time.Duration
	replicationURL     string
	natsOptions        []nats.Option
	replicationSubject string
	deliverPolicy      string
	publisher          CDCPublisher
	publisherTimeout   time.Duration
	snapshotInterval   time.Duration
	disableDDLSync     bool
	waitFor            chan struct{}

	nc          *nats.Conn
	ns          *server.Server
	subscriber  *subscriber
	snapshotter *snapshotter
}

var ErrNatsNotConfigured = errors.New("NATS not configured")

func (c *Connector) DeliveredInfo(ctx context.Context, name string) ([]*jetstream.ConsumerInfo, error) {
	if c.subscriber == nil {
		return nil, ErrNatsNotConfigured
	}
	return c.subscriber.DeliveredInfo(ctx, name)
}

func (c *Connector) RemoveConsumer(ctx context.Context, name string) error {
	if c.subscriber == nil {
		return ErrNatsNotConfigured
	}
	return c.subscriber.RemoveConsumer(ctx, name)
}

func (c *Connector) TakeSnapshot(ctx context.Context, db *sql.DB) (sequence uint64, err error) {
	if c.snapshotter == nil {
		return 0, ErrNatsNotConfigured
	}
	return c.snapshotter.TakeSnapshot(ctx, db)
}

func (c *Connector) LatestSnapshot(ctx context.Context) (uint64, io.ReadCloser, error) {
	if c.snapshotter == nil {
		return 0, nil, ErrNatsNotConfigured
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
	muConnectors.Lock()
	defer muConnectors.Unlock()

	delete(connectors, c.dsn)

	if c.embeddedNatsConfig != nil {
		ncs := natsClientServers[c.embeddedNatsConfig]
		ncs.count--
		natsClientServers[c.embeddedNatsConfig] = ncs
		if ncs.count < 0 {
			if !ncs.client.IsClosed() {
				ncs.client.Close()
			}
			ncs.server.WaitForShutdown()
		}
		delete(natsClientServers, c.embeddedNatsConfig)
		return
	}
	if c.nc != nil && !c.nc.IsClosed() {
		c.nc.Close()
	}
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	sqliteConn, _ := conn.(*sqlite3.SQLiteConn)
	return &Conn{
		SQLiteConn:     sqliteConn,
		disableDDLSync: c.disableDDLSync,
	}, nil
}

func (c *Connector) Driver() driver.Driver {
	return c.driver
}

func LatestSnapshot(ctx context.Context, options ...Option) (sequence uint64, reader io.ReadCloser, err error) {
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
	bucketName := c.replicationSubject + "_SNAPSHOTS"
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
	info, err := objectStore.GetInfo(ctx, latestSnapshotName)
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

	reader, err = objectStore.Get(ctx, latestSnapshotName)
	return sequence, reader, err
}

type CDCPublisher interface {
	Publish(cs *ChangeSet) error
}

var (
	changeSetSessions   = make(map[*sqlite3.SQLiteConn]*ChangeSet)
	changeSetSessionsMu sync.Mutex
)

func addSQLChange(conn *sqlite3.SQLiteConn, sql string, args []any) error {
	cs := changeSetSessions[conn]
	if cs == nil {
		return errors.New("no changeset session for the connection")
	}
	cs.AddChange(Change{
		Operation: "SQL",
		SQL:       sql,
		SQLArgs:   args,
	})
	return nil
}

func removeLastChange(conn *sqlite3.SQLiteConn) error {
	cs := changeSetSessions[conn]
	if cs == nil {
		return errors.New("no changeset session for the connection")
	}
	if len(cs.Changes) > 0 {
		cs.Changes = cs.Changes[:len(cs.Changes)-1]
	}
	return nil
}

type tableInfo struct {
	columns []string
	types   []string
}

func enableCDCHooks(conn *sqlite3.SQLiteConn, filename string, nodeName string, publisher CDCPublisher) {
	changeSetSessionsMu.Lock()
	defer changeSetSessionsMu.Unlock()

	cs := NewChangeSet(nodeName, filename, publisher)
	changeSetSessions[conn] = cs
	tableColumns := make(map[string]tableInfo)
	conn.RegisterPreUpdateHook(func(d sqlite3.SQLitePreUpdateData) {
		change, ok := getChange(&d)
		if !ok {
			return
		}
		fullTableName := fmt.Sprintf("%s.%s", change.Database, change.Table)
		var types []string
		if ti, ok := tableColumns[fullTableName]; ok {
			change.Columns = ti.columns
			types = ti.types
		} else {
			rows, err := conn.Query(fmt.Sprintf("SELECT name, type FROM %s.PRAGMA_TABLE_INFO('%s')", change.Database, change.Table), nil)
			if err != nil {
				slog.Error("failed to read columns", "error", err, "database", change.Database, "table", change.Table)
				return
			}
			defer rows.Close()
			var columns []string
			for {
				dataRow := []driver.Value{new(string), new(string)}

				err := rows.Next(dataRow)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						slog.Error("failed to read table columns", "error", err, "table", change.Table)
					}
					break
				}
				if v, ok := dataRow[0].(string); ok {
					columns = append(columns, v)
				}
				if v, ok := dataRow[1].(string); ok {
					types = append(types, v)
				}
			}
			change.Columns = columns
			tableColumns[fullTableName] = tableInfo{
				columns: columns,
				types:   types,
			}
		}
		for i, t := range types {
			if t != "BLOB" {
				if i < len(change.OldValues) && change.OldValues[i] != nil {
					change.OldValues[i] = convert(change.OldValues[i])
				}
				if i < len(change.NewValues) && change.NewValues[i] != nil {
					change.NewValues[i] = convert(change.NewValues[i])
				}
			}
		}

		cs.AddChange(change)
	})

	conn.RegisterCommitHook(func() int {
		if err := cs.Send(publisher); err != nil {
			slog.Error("failed to send changeset", "error", err)
			return 1
		}
		return 0
	})
	conn.RegisterRollbackHook(func() {
		cs.Clear()
	})
}

func disableCDCHooks(conn *sqlite3.SQLiteConn) {
	conn.RegisterPreUpdateHook(nil)
	conn.RegisterCommitHook(nil)
	conn.RegisterRollbackHook(nil)
}

func convert(src any) any {
	switch v := src.(type) {
	case []byte:
		return string(v)
	default:
		return src
	}
}

func sqliteConn(conn *sql.Conn) (*sqlite3.SQLiteConn, error) {
	var sqlite3Conn *sqlite3.SQLiteConn
	err := conn.Raw(func(driverConn any) error {
		switch c := driverConn.(type) {
		case *Conn:
			sqlite3Conn = c.SQLiteConn
			return nil
		case *sqlite3.SQLiteConn:
			sqlite3Conn = c
			return nil
		default:
			return fmt.Errorf("not a sqlite3 connection")
		}
	})
	return sqlite3Conn, err
}

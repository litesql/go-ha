package ha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/litesql/go-ha/api/sql/v1/sqlv1connect"
	haconnect "github.com/litesql/go-ha/connect"
)

const DefaultStream = "ha_replication"

var (
	connectors        = make(map[string]*Connector)
	natsClientServers = make(map[*EmbeddedNatsConfig]*natsClientServer)
	grpcServers       = make(map[int]*haconnect.Server)
	muConnectors      sync.RWMutex
)

type ConnHooksConfig struct {
	NodeName             string
	ReplicationID        string
	DisableDDLSync       bool
	Publisher            Publisher
	CDC                  CDCPublisher
	TxSeqTrackerProvider TxSeqTrackerProvider
	Leader               LeaderProvider
	GrpcTimeout          time.Duration
	GrpcToken            string
	GrpcInsecure         bool
	QueryRouter          *regexp.Regexp
}

type ConnHooksFactory func(cfg ConnHooksConfig) ConnHooksProvider

type ConnHooksProvider interface {
	RegisterHooks(driver.Conn, *Connector) (driver.Conn, error)
	DisableHooks(*sql.Conn) error
	EnableHooks(*sql.Conn) error
}

type TxSeqTracker interface {
	LatestSeq() uint64
}

type TxSeqTrackerProvider func() TxSeqTracker

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

func ConnectHandler(opts ...connect.HandlerOption) (path string, handler http.Handler) {
	return sqlv1connect.NewDatabaseServiceHandler(&haconnect.Service{
		DBProvider: func(id string) (haconnect.HADB, bool) {
			connector, ok := LookupConnectorByReplicationID(id)
			if !ok {
				return nil, false
			}
			return connector, true
		},
		DSNList:           ListDSN,
		ReplicationIDList: ListReplicationIDs,
		SQLExpectResultSet: func(ctx context.Context, sql string) (bool, error) {
			stmt, err := ParseStatement(ctx, sql)
			if err != nil {
				return false, err
			}
			expectResultSet := stmt.HasReturning() || stmt.IsSelect() || stmt.IsExplain()
			return expectResultSet, nil
		},
	}, opts...)
}

type ctxKey int

const localDBKey ctxKey = iota

func ContextLocalDB(ctx context.Context, useLocalDB bool) context.Context {
	return context.WithValue(ctx, localDBKey, useLocalDB)
}

func LocalDB(ctx context.Context) bool {
	v := ctx.Value(localDBKey)
	localDB, ok := v.(bool)
	return ok && localDB
}

func LatestSnapshot(ctx context.Context, dsn string, options ...Option) (sequence uint64, reader io.ReadCloser, err error) {
	if len(options) == 0 {
		connector, ok := LookupConnector(dsn)
		if !ok {
			return 0, nil, fmt.Errorf("no database available with DSN: %s", dsn)
		}
		return connector.LatestSnapshot(ctx)
	}
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
	DB() *sql.DB
	SetDB(*sql.DB)
	Start() error
	RemoveConsumer(ctx context.Context, name string) error
	DeliveredInfo(ctx context.Context, name string) (any, error)
	HistoryBySeq(ctx context.Context, startSeq uint64) ([]haconnect.HistoryItem, error)
	HistoryByTime(ctx context.Context, duration time.Duration) ([]haconnect.HistoryItem, error)
	UndoBySeq(ctx context.Context, startSeq uint64, filterType haconnect.UndoFilter, filterEntities map[string][]int64) error
	UndoByTime(ctx context.Context, duration time.Duration, filterType haconnect.UndoFilter, filterEntities map[string][]int64) error
}

type ChangeSetInterceptor interface {
	BeforeApply(*ChangeSet, *sql.Conn) (skip bool, err error)
	AfterApply(*ChangeSet, *sql.Conn, error) error
}

type DBSnapshotter interface {
	DB() *sql.DB
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

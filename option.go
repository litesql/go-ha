package ha

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type Option func(*Connector)

func WithName(name string) Option {
	return func(c *Connector) {
		c.name = name
	}
}

func WithExtensions(extensions ...string) Option {
	return func(c *Connector) {
		if len(extensions) > 0 {
			c.extensions = extensions
		}
	}
}

type EmbeddedNatsConfig struct {
	Name       string
	Port       int
	StoreDir   string
	User       string
	Pass       string
	File       string
	EnableLogs bool
}

func (e EmbeddedNatsConfig) empty() bool {
	return e.Name == "" && e.Port == 0 && e.StoreDir == "" &&
		e.User == "" && e.Pass == "" && e.File == "" && !e.EnableLogs
}

func WithEmbeddedNatsConfig(cfg *EmbeddedNatsConfig) Option {
	return func(c *Connector) {
		c.embeddedNatsConfig = cfg
	}
}

func WithNatsOptions(options ...nats.Option) Option {
	return func(c *Connector) {
		if len(options) > 0 {
			c.natsOptions = options
		}
	}
}

func WithReplicationURL(url string) Option {
	return func(c *Connector) {
		c.replicationURL = url
	}
}

func WithReplicationStream(stream string) Option {
	return func(c *Connector) {
		c.replicationStream = stream
	}
}

func WithDeliverPolicy(deliverPolicy string) Option {
	return func(c *Connector) {
		c.deliverPolicy = deliverPolicy
	}
}

func WithReplicationID(id string) Option {
	return func(c *Connector) {
		c.replicationID = id
	}
}

func WithReplicationPublisher(pub Publisher) Option {
	return func(c *Connector) {
		c.publisher = pub
	}
}

func WithReplicationSubscriber(sub Subscriber) Option {
	return func(c *Connector) {
		c.subscriber = sub
	}
}

func WithChangeSetInterceptor(interceptor ChangeSetInterceptor) Option {
	return func(c *Connector) {
		c.interceptor = interceptor
	}
}

func WithDBSnapshotter(snap DBSnapshotter) Option {
	return func(c *Connector) {
		c.snapshotter = snap
	}
}

func WithPublisherTimeout(timeout time.Duration) Option {
	return func(c *Connector) {
		c.publisherTimeout = timeout
	}
}

func WithAsyncPublisher() Option {
	return func(c *Connector) {
		c.asyncPublisher = true
	}
}

func WithAsyncPublisherOutboxDir(dir string) Option {
	return func(c *Connector) {
		c.asyncPublisherOutboxDir = dir
	}
}

func WithSnapshotInterval(interval time.Duration) Option {
	return func(c *Connector) {
		c.snapshotInterval = interval
	}
}

func WithStreamMaxAge(maxAge time.Duration) Option {
	return func(c *Connector) {
		c.streamMaxAge = maxAge
	}
}

func WithReplicas(replicas int) Option {
	return func(c *Connector) {
		c.replicas = replicas
	}
}

func WithDisableDDLSync() Option {
	return func(c *Connector) {
		c.disableDDLSync = true
	}
}

func WithRowIdentify(i RowIdentify) Option {
	return func(c *Connector) {
		c.rowIdentify = i
	}
}

func WithLeaderElectionLocalTarget(localEndpoint string) Option {
	return func(c *Connector) {
		c.leaderElectionLocalTarget = localEndpoint
	}
}

func WithLeaderProvider(p LeaderProvider) Option {
	return func(c *Connector) {
		c.leaderProvider = p
	}
}

func WithWaitFor(ch chan struct{}) Option {
	return func(c *Connector) {
		c.waitFor = ch
	}
}

func WithAutoStart(enabled bool) Option {
	return func(c *Connector) {
		c.autoStart = enabled
	}
}

func WithClusterSize(size int) Option {
	return func(c *Connector) {
		c.clusterSize = size
	}
}

func WithCDCPublisher(p CDCPublisher) Option {
	return func(c *Connector) {
		c.cdcPublisher = p
	}
}

func WithGrpcPort(port int) Option {
	return func(c *Connector) {
		c.grpcPort = port
	}
}

func WithGrpcTimeout(timeout time.Duration) Option {
	return func(c *Connector) {
		c.grpcTimeout = timeout
	}
}

func WithGrpcToken(token string) Option {
	return func(c *Connector) {
		c.grpcToken = token
	}
}

func WithQueryRouter(re *regexp.Regexp) Option {
	return func(c *Connector) {
		c.queryRouter = re
	}
}

func NameToOptions(name string) (string, []Option, error) {
	dsn := name
	var queryParams string
	if i := strings.Index(name, "?"); i != -1 {
		dsn = name[0:i]
		queryParams = name[i+1:]
	}
	if queryParams == "" {
		return dsn, nil, nil
	}
	values, err := url.ParseQuery(queryParams)
	if err != nil {
		return "", nil, err
	}

	// Sort the keys to ensure deterministic order
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	opts, err := envToOptions()
	if err != nil {
		return "", nil, err
	}
	var dsnOptions []string
	var natsConfig EmbeddedNatsConfig
	for _, k := range keys {
		v := values[k]
		if len(v) == 0 {
			continue
		}
		value := v[0]
		switch k {
		case "name":
			opts = append(opts, WithName(value))
		case "rowIdentify":
			var rowIdentify RowIdentify
			switch value {
			case string(PK):
				rowIdentify = PK
			case string(Rowid):
				rowIdentify = Rowid
			case string(Full):
				rowIdentify = Full
			default:
				return "", nil, fmt.Errorf("invalid rowIdentify value. Use pk, rowid or full")
			}
			opts = append(opts, WithRowIdentify(rowIdentify))
		case "replicationURL":
			opts = append(opts, WithReplicationURL(value))
		case "replicationStream":
			opts = append(opts, WithReplicationStream(value))
		case "deliverPolicy":
			opts = append(opts, WithDeliverPolicy(value))
		case "publisherTimeout":
			timeout, err := time.ParseDuration(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid publisherTimeout: %w", err)
			}
			opts = append(opts, WithPublisherTimeout(timeout))
		case "asyncPublisher":
			b, err := strconv.ParseBool(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid asyncPublisher: %w", err)
			}
			if b {
				opts = append(opts, WithAsyncPublisher())
			}
		case "asyncPublisherOutboxDir":
			opts = append(opts, WithAsyncPublisherOutboxDir(value))
		case "replID", "replicationID":
			opts = append(opts, WithReplicationID(value))
		case "clusterSize":
			size, err := strconv.Atoi(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid clusterSize: %w", err)
			}
			opts = append(opts, WithClusterSize(size))
		case "replicas":
			replicas, err := strconv.Atoi(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid replicas: %w", err)
			}
			opts = append(opts, WithReplicas(replicas))
		case "streamMaxAge":
			maxAge, err := time.ParseDuration(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid streamMaxAge: %w", err)
			}
			opts = append(opts, WithStreamMaxAge(maxAge))
		case "snapshotInterval":
			interval, err := time.ParseDuration(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid snapshotInterval: %w", err)
			}
			opts = append(opts, WithSnapshotInterval(interval))
		case "natsName":
			natsConfig.Name = value
		case "natsPort":
			port, err := strconv.Atoi(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid natsPort: %w", err)
			}
			natsConfig.Port = port
		case "natsConfigFile":
			natsConfig.File = value
		case "natsStoreDir":
			natsConfig.StoreDir = value
		case "natsUser":
			natsConfig.User = value
		case "natsPass":
			natsConfig.Pass = value
		case "disableDDLSync":
			disable, err := strconv.ParseBool(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid disableDDLSync: %w", err)
			}
			if disable {
				opts = append(opts, WithDisableDDLSync())
			}
		case "disablePublisher":
			disable, err := strconv.ParseBool(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid disablePublisher: %w", err)
			}
			if disable {
				opts = append(opts, WithReplicationPublisher(NewNoopPublisher()))
			}
		case "disableSubscriber":
			disable, err := strconv.ParseBool(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid disableSubscriber: %w", err)
			}
			if disable {
				opts = append(opts, WithReplicationSubscriber(NewNoopSubscriber()))
			}
		case "disableDBSnapshotter":
			disable, err := strconv.ParseBool(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid disableDBSnapshotter: %w", err)
			}
			if disable {
				opts = append(opts, WithDBSnapshotter(NewNoopSnapshotter()))
			}
		case "grpcPort":
			port, err := strconv.Atoi(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid grpcPort: %w", err)
			}
			opts = append(opts, WithGrpcPort(port))
		case "grpcTimeout":
			timeout, err := time.ParseDuration(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid grpcTimeout: %w", err)
			}
			opts = append(opts, WithGrpcTimeout(timeout))
		case "grpcToken":
			opts = append(opts, WithGrpcToken(value))
		case "leaderProvider":
			typ, target, ok := strings.Cut(value, ":")
			if !ok {
				return "", nil, fmt.Errorf("invalid leaderStrategy. Use leaderStrategy=dynamic:http://localhost:8080 or leaderStrategy=static:http://host:port")
			}
			switch typ {
			case "dynamic":
				opts = append(opts, WithLeaderElectionLocalTarget(target))
			case "static":
				opts = append(opts, WithLeaderProvider(&StaticLeader{
					Target: target,
				}))
			default:
				return "", nil, fmt.Errorf("invalid leaderStrategy, prefix with static or dynamic option. Examples: leaderStrategy=dynamic:http://localhost:8080 or leaderStrategy=static:http://host:port")
			}
		case "queryRouter":
			re, err := regexp.Compile(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid queryRouter: %w", err)
			}
			opts = append(opts, WithQueryRouter(re))
		case "autoStart":
			autoStart, err := strconv.ParseBool(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid autoStart: %w", err)
			}
			opts = append(opts, WithAutoStart(autoStart))
		default:
			for _, v := range values[k] {
				dsnOptions = append(dsnOptions, fmt.Sprintf("%s=%s", k, v))
			}
		}
	}

	if !natsConfig.empty() {
		opts = append(opts, WithEmbeddedNatsConfig(&natsConfig))
	}

	// Sort DSN options to ensure deterministic order
	slices.Sort(dsnOptions)

	if len(dsnOptions) > 0 {
		dsn = fmt.Sprintf("%s?%s", dsn, strings.Join(dsnOptions, "&"))
	}
	return dsn, opts, nil
}

func envToOptions() ([]Option, error) {
	opts := make([]Option, 0)
	if v := os.Getenv("HA_NAME"); v != "" {
		opts = append(opts, WithName(v))
	}
	if v := os.Getenv("HA_ROW_IDENTIFY"); v != "" {
		var rowIdentify RowIdentify
		switch v {
		case string(PK):
			rowIdentify = PK
		case string(Rowid):
			rowIdentify = Rowid
		case string(Full):
			rowIdentify = Full
		default:
			return nil, fmt.Errorf("invalid rowIdentify value. Use pk, rowid or full")
		}
		opts = append(opts, WithRowIdentify(rowIdentify))
	}
	if v := os.Getenv("HA_REPLICATION_URL"); v != "" {
		opts = append(opts, WithReplicationURL(v))
	}
	if v := os.Getenv("HA_REPLICATION_STREAM"); v != "" {
		opts = append(opts, WithReplicationStream(v))
	}
	if v := os.Getenv("HA_DELIVER_POLICY"); v != "" {
		opts = append(opts, WithDeliverPolicy(v))
	}
	if v := os.Getenv("HA_PUBLISHER_TIMEOUT"); v != "" {
		timeout, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid publisherTimeout: %w", err)
		}
		opts = append(opts, WithPublisherTimeout(timeout))
	}
	if v := os.Getenv("HA_ASYNC_PUBLISHER"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid asyncPublisher: %w", err)
		}
		if b {
			opts = append(opts, WithAsyncPublisher())
		}
	}
	if v := os.Getenv("HA_ASYNC_PUBLISHER_OUTBOX_DIR"); v != "" {
		opts = append(opts, WithAsyncPublisherOutboxDir(v))
	}
	if v := os.Getenv("HA_REPLICATION_ID"); v != "" {
		opts = append(opts, WithReplicationID(v))
	}
	if v := os.Getenv("HA_CLUSTER_SIZE"); v != "" {
		size, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid clusterSize: %w", err)
		}
		opts = append(opts, WithClusterSize(size))
	}
	if v := os.Getenv("HA_REPLICAS"); v != "" {
		replicas, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid replicas: %w", err)
		}
		opts = append(opts, WithReplicas(replicas))
	}
	if v := os.Getenv("HA_STREAM_MAX_AGE"); v != "" {
		maxAge, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid streamMaxAge: %w", err)
		}
		opts = append(opts, WithStreamMaxAge(maxAge))
	}
	if v := os.Getenv("HA_SNAPSHOT_INTERVAL"); v != "" {
		interval, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid snapshotInterval: %w", err)
		}
		opts = append(opts, WithSnapshotInterval(interval))
	}
	var natsConfig EmbeddedNatsConfig
	if v := os.Getenv("HA_NATS_NAME"); v != "" {
		natsConfig.Name = v
	}
	if v := os.Getenv("HA_NATS_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid natsPort: %w", err)
		}
		natsConfig.Port = port
	}
	if v := os.Getenv("HA_NATS_CONFIG_FILE"); v != "" {
		natsConfig.File = v
	}
	if v := os.Getenv("HA_NATS_STORE_DIR"); v != "" {
		natsConfig.StoreDir = v
	}
	if v := os.Getenv("HA_NATS_USER"); v != "" {
		natsConfig.User = v
	}
	if v := os.Getenv("HA_NATS_PASS"); v != "" {
		natsConfig.Pass = v
	}
	if v := os.Getenv("HA_DISABLE_DDL_SYNC"); v != "" {
		disable, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid disableDDLSync: %w", err)
		}
		if disable {
			opts = append(opts, WithDisableDDLSync())
		}
	}
	if v := os.Getenv("HA_DISABLE_PUBLISHER"); v != "" {
		disable, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid disablePublisher: %w", err)
		}
		if disable {
			opts = append(opts, WithReplicationPublisher(NewNoopPublisher()))
		}
	}
	if v := os.Getenv("HA_DISABLE_SUBSCRIBER"); v != "" {
		disable, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid disableSubscriber: %w", err)
		}
		if disable {
			opts = append(opts, WithReplicationSubscriber(NewNoopSubscriber()))
		}
	}
	if v := os.Getenv("HA_DISABLE_DB_SNAPSHOTTER"); v != "" {
		disable, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid disableDBSnapshotter: %w", err)
		}
		if disable {
			opts = append(opts, WithDBSnapshotter(NewNoopSnapshotter()))
		}
	}
	if v := os.Getenv("HA_GRPC_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid grpcPort: %w", err)
		}
		opts = append(opts, WithGrpcPort(port))
	}
	if v := os.Getenv("HA_GRPC_TIMEOUT"); v != "" {
		timeout, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid grpcTimeout: %w", err)
		}
		opts = append(opts, WithGrpcTimeout(timeout))
	}
	if v := os.Getenv("HA_GRPC_TOKEN"); v != "" {
		opts = append(opts, WithGrpcToken(v))
	}
	if v := os.Getenv("HA_LEADER_PROVIDER"); v != "" {
		typ, target, ok := strings.Cut(v, ":")
		if !ok {
			return nil, fmt.Errorf("invalid leaderStrategy. Use leaderStrategy=dynamic:http://localhost:8080 or leaderStrategy=static:http://host:port")
		}
		switch typ {
		case "dynamic":
			opts = append(opts, WithLeaderElectionLocalTarget(target))
		case "static":
			opts = append(opts, WithLeaderProvider(&StaticLeader{
				Target: target,
			}))
		default:
			return nil, fmt.Errorf("invalid leaderStrategy, prefix with static or dynamic option. Examples: leaderStrategy=dynamic:http://localhost:8080 or leaderStrategy=static:http://host:port")
		}
	}
	if v := os.Getenv("HA_QUERY_ROUTER"); v != "" {
		re, err := regexp.Compile(v)
		if err != nil {
			return nil, fmt.Errorf("invalid queryRouter: %w", err)
		}
		opts = append(opts, WithQueryRouter(re))
	}
	if v := os.Getenv("HA_AUTO_START"); v != "" {
		autoStart, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid autoStart: %w", err)
		}
		opts = append(opts, WithAutoStart(autoStart))
	}

	if !natsConfig.empty() {
		opts = append(opts, WithEmbeddedNatsConfig(&natsConfig))
	}
	return opts, nil
}

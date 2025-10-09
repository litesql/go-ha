package ha

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/litesql/go-sqlite3"
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

func WithReplicationSubject(subject string) Option {
	return func(c *Connector) {
		c.replicationSubject = subject
	}
}

func WithDeliverPolicy(deliverPolicy string) Option {
	return func(c *Connector) {
		c.deliverPolicy = deliverPolicy
	}
}

func WithCDCPublisher(pub CDCPublisher) Option {
	return func(c *Connector) {
		c.publisher = pub
	}
}

func WithPublisherTimeout(timeout time.Duration) Option {
	return func(c *Connector) {
		c.publisherTimeout = timeout
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

type ConnectHookFn func(conn *sqlite3.SQLiteConn) error

func WithConnectHook(fn ConnectHookFn) Option {
	return func(c *Connector) {
		c.connectHook = fn
	}
}

func nameToOptions(name string) (string, []Option, error) {
	dsn := name
	var queryParams string
	if i := strings.Index(name, "?"); i != -1 {
		dsn = name[0:i]
		queryParams = name[i:]
	}
	if queryParams == "" {
		return dsn, nil, nil
	}
	values, err := url.ParseQuery(queryParams)
	if err != nil {
		return "", nil, err
	}
	var opts []Option
	var dsnOptions []string
	var natsConfig EmbeddedNatsConfig
	for k, v := range values {
		if len(v) == 0 {
			continue
		}
		value := v[0]
		switch k {
		case "name":
			opts = append(opts, WithName(value))
		case "replicationURL":
			opts = append(opts, WithReplicationURL(value))
		case "replicationSubject":
			opts = append(opts, WithReplicationSubject(value))
		case "deliverPolicy":
			opts = append(opts, WithDeliverPolicy(value))
		case "publisherTimeout":
			timeout, err := time.ParseDuration(value)
			if err != nil {
				return "", nil, fmt.Errorf("invalid publisherTimeout: %w", err)
			}
			opts = append(opts, WithPublisherTimeout(timeout))
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
				return "", nil, fmt.Errorf("invalid streamMaxAge: %w", err)
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
		default:
			dsnOptions = append(dsnOptions, fmt.Sprintf("%s=%s", k, value))
		}
	}

	if !natsConfig.empty() {
		opts = append(opts, WithEmbeddedNatsConfig(&natsConfig))
	}

	if len(dsnOptions) > 0 {
		dsn = fmt.Sprintf("%s?%s", dsn, strings.Join(dsnOptions, "&"))
	}
	return dsn, opts, nil
}

package ha

import (
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

type EmbeddedNatsConfig struct {
	Name       string
	Port       int
	StoreDir   string
	User       string
	Pass       string
	File       string
	EnableLogs bool
}

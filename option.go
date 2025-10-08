package ha

import "time"

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

func WithNatsConfigFile(path string) Option {
	return func(c *Connector) {
		c.embeddedNatsConfig = path
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

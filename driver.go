package ha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/litesql/go-sqlite3"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const DefaultStream = "ha_replication"

func NewConnector(dsn string, options ...Option) (*Connector, error) {
	c := Connector{
		replicationSubject: DefaultStream,
		publisherTimeout:   15 * time.Second,
		streamMaxAge:       24 * time.Hour,
		replicas:           1,
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
		c.nc, c.ns, err = runEmbeddedNATSServer(*c.embeddedNatsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to start embedded NATS server: %w", err)
		}
	}

	if c.nc == nil && c.replicationURL != "" {
		c.nc, err = nats.Connect(c.replicationURL, c.natsOptions...)
		if err != nil {
			return nil, fmt.Errorf("failed to conect to NATS server at %q: %w", c.replicationURL, err)
		}
	}

	if c.nc != nil && c.publisher == nil {
		c.publisher, err = newNatsPublisher(c.nc, c.replicas, c.replicationSubject, c.streamMaxAge, c.publisherTimeout)
		if err != nil {
			return nil, fmt.Errorf("failed to start NATS publisher: %w", err)
		}
	}

	c.driver = &sqlite3.SQLiteDriver{
		Extensions: c.extensions,
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			enableCDCHooks(conn, c.name, c.publisher)
			return nil
		},
	}

	if c.nc != nil {
		db := sql.OpenDB(&c)
		_, err := newNatsSubscriber(c.name, c.nc, c.replicationSubject, c.deliverPolicy, db)
		if err != nil {
			return nil, fmt.Errorf("failed to start NATS subscriber: %w", err)
		}
	}
	return &c, nil
}

type Connector struct {
	driver             driver.Driver
	dsn                string
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

	nc *nats.Conn
	ns *server.Server
}

func (c *Connector) Close() {
	if c.nc != nil && !c.nc.IsClosed() {
		c.nc.Close()
	}
	if c.ns != nil {
		c.ns.WaitForShutdown()
	}
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	sqliteConn, _ := conn.(*sqlite3.SQLiteConn)
	return &Conn{
		SQLiteConn: sqliteConn,
	}, nil
}

func (c *Connector) Driver() driver.Driver {
	return c.driver
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

func enableCDCHooks(conn *sqlite3.SQLiteConn, nodeName string, publisher CDCPublisher) {
	changeSetSessionsMu.Lock()
	defer changeSetSessionsMu.Unlock()

	cs := NewChangeSet(nodeName, publisher)
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

package ha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	haconnect "github.com/litesql/go-ha/connect"
)

type HookCallbacks struct {
	OnPreUpdate func(Change)
	OnCommit    func() error
	OnRollback  func()
}

type SQLiteHooksAdapter interface {
	WrapConn(c driver.Conn, connector *Connector) (SQLiteConn, error)
	EnableHooks(conn SQLiteConn, cb HookCallbacks)
	DisableHooks(conn SQLiteConn)
}

type sqliteHooksProvider struct {
	adapter SQLiteHooksAdapter
}

func NewSQLiteHooksProvider(adapter SQLiteHooksAdapter) ConnHooksProvider {
	return &sqliteHooksProvider{adapter: adapter}
}

func (p *sqliteHooksProvider) RegisterHooks(c driver.Conn, connector *Connector) (driver.Conn, error) {
	telemetry, err := defaultSQLiteTelemetry()
	if err != nil {
		return nil, fmt.Errorf("initialize SQLite telemetry: %w", err)
	}
	sqliteConn, err := p.adapter.WrapConn(c, connector)
	if err != nil {
		return nil, err
	}
	conn := &Conn{
		SQLiteConn:     sqliteConn,
		connector:      connector,
		telemetry:      telemetry,
		hooksAdapter:   p.adapter,
		enableRedirect: true,
		reqCh:          make(chan *sqlv1.QueryRequest),
		resCh:          make(chan *sqlv1.QueryResponse),
		txseqTracker:   connector.Subscriber(),
	}
	conn.enableCDCHooks()
	return conn, conn.start()
}

func (p *sqliteHooksProvider) DisableHooks(conn *sql.Conn) error {
	sconn, err := HAConn(conn)
	if err != nil {
		return err
	}
	p.adapter.DisableHooks(sconn.SQLiteConn)
	sconn.enableRedirect = false
	return nil
}

func (p *sqliteHooksProvider) EnableHooks(conn *sql.Conn) error {
	sconn, err := HAConn(conn)
	if err != nil {
		return err
	}
	sconn.enableCDCHooks()
	sconn.enableRedirect = true
	return sconn.start()
}

type tableSchema struct {
	columns, types, pkColumns []string
}

var (
	tableSchemaCache   = make(map[string]map[string]*tableSchema)
	tableSchemaCacheMu sync.RWMutex
)

func ClearTableSchemaCache(replicationID string) {
	tableSchemaCacheMu.Lock()
	delete(tableSchemaCache, replicationID)
	tableSchemaCacheMu.Unlock()
}

func getTableSchema(c *Conn, replicationID string, database string, table string) (*tableSchema, error) {
	key := fmt.Sprintf("%s.%s", database, table)
	tableSchemaCacheMu.RLock()
	if replicationTables, ok := tableSchemaCache[replicationID]; ok {
		if schema, ok := replicationTables[key]; ok {
			tableSchemaCacheMu.RUnlock()
			return schema, nil
		}
	}
	tableSchemaCacheMu.RUnlock()

	var schema tableSchema
	query := fmt.Sprintf("SELECT name, type, pk FROM %s.PRAGMA_TABLE_INFO('%s') ORDER BY cid", database, table)
	rows, err := c.baseQueryContext(context.Background(), query, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rows.Columns()
	for {
		dataRow := []driver.Value{new(string), new(string), new(int64)}
		err := rows.Next(dataRow)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Error("failed to read table columns", "error", err, "table", table)
			}
			break
		}
		if v, ok := dataRow[0].(string); ok {
			schema.columns = append(schema.columns, v)
		} else {
			continue
		}
		if v, ok := dataRow[1].(string); ok {
			schema.types = append(schema.types, v)
		}
		if v, ok := dataRow[2].(int64); ok && v > 0 {
			schema.pkColumns = append(schema.pkColumns, dataRow[0].(string))
		}
	}
	tableSchemaCacheMu.Lock()
	replicationTables, ok := tableSchemaCache[replicationID]
	if !ok {
		replicationTables = make(map[string]*tableSchema)
		tableSchemaCache[replicationID] = replicationTables
	}
	replicationTables[key] = &schema
	tableSchemaCacheMu.Unlock()
	return &schema, nil
}

func (c *Conn) enableCDCHooks() {
	cs := NewChangeSet(c.connector.NodeName(), c.connector.ReplicationID())
	c.changeSet = cs
	if c.hooksAdapter == nil {
		return
	}
	c.hooksAdapter.EnableHooks(c.SQLiteConn, HookCallbacks{
		OnPreUpdate: func(change Change) {
			if change.Table == TwoPhaseCommitDecisionTable || change.Table == OutboxTable {
				return
			}
			schema, err := getTableSchema(c, c.connector.ReplicationID(), change.Database, change.Table)
			if err != nil {
				slog.Error("failed to read columns", "error", err, "database", change.Database, "table", change.Table)
				return
			}
			change.Columns = schema.columns
			change.PKColumns = schema.pkColumns
			for i, t := range schema.types {
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
		},
		OnCommit: func() error {
			if _, ok := c.connector.Publisher().(TwoPhaseCommitPreparer); ok {
				return nil
			}
			if err := cs.Send(c.Context(), c.connector.Publisher()); err != nil {
				slog.Error("failed to send changeset", "error", err, "pub", fmt.Sprintf("%T", c.connector.Publisher()))
				return err
			}
			if c.connector.CDCPublisher() != nil {
				data := cs.DebeziumData()
				if len(data) > 0 {
					if err := c.connector.CDCPublisher().Publish(data); err != nil {
						slog.Error("failed to send cdc", "error", err)
						return err
					}
				}
			}
			return nil
		},
		OnRollback: func() {
			cs.Clear()
		},
	})
}

func (c *Conn) addSQLChange(sql string, args []any) error {
	if c.changeSet == nil {
		return errors.New("no changeset session for the connection")
	}
	c.changeSet.AddChange(Change{
		Operation: "SQL",
		Command:   sql,
		Args:      args,
	})
	return nil
}

func (c *Conn) removeLastChange() error {
	if c.changeSet == nil {
		return errors.New("no changeset session for the connection")
	}
	if len(c.changeSet.Changes) > 0 {
		c.changeSet.Changes = c.changeSet.Changes[:len(c.changeSet.Changes)-1]
	}
	return nil
}

func (c *Conn) snapshotChangeSet() *ChangeSet {
	if c.changeSet == nil {
		return nil
	}
	snapshot := *c.changeSet
	snapshot.Changes = append([]Change(nil), c.changeSet.Changes...)
	return &snapshot
}

func (c *Conn) clearChangeSet() {
	if c.changeSet != nil {
		c.changeSet.Clear()
	}
}

func publishChangeSetCDC(connector *Connector, cs *ChangeSet) {
	if connector.CDCPublisher() == nil {
		return
	}
	data := cs.DebeziumData()
	if len(data) == 0 {
		return
	}
	if err := connector.CDCPublisher().Publish(data); err != nil {
		slog.Error("failed to send cdc after local commit", "error", err)
	}
}

func convert(src any) any {
	switch v := src.(type) {
	case []byte:
		return string(v)
	default:
		return src
	}
}

func UndoFunc(connector *Connector, filter haconnect.UndoFilter) func(int64) string {
	return func(seq int64) string {
		if seq < 0 {
			return "param must be non-negative"
		}
		if err := connector.UndoBySeq(context.Background(), uint64(seq), filter, nil); err != nil {
			return err.Error()
		}
		if seq == 0 {
			return "undone last transaction"
		}
		if filter == haconnect.UndoFilterEntity {
			return fmt.Sprintf("undone entity changes until stream sequence %d", seq)
		}
		return fmt.Sprintf("undone transactions until stream sequence %d", seq)
	}
}

func HistoryTableDeclaration(tableName string) string {
	return fmt.Sprintf(`
		CREATE TABLE %s (
			seq INT,
			sql TEXT,
			timestamp TEXT
		)`, tableName)
}

type HistoryCursor struct {
	Connector *Connector
	Pos       int
	Rows      []haconnect.HistoryItem
}

func (vc *HistoryCursor) FilterValue(hasVal bool, firstVal any) error {
	var seq uint64
	if hasVal {
		switch v := firstVal.(type) {
		case int64:
			if v < 0 {
				v = 1
			}
			seq = uint64(v)
		default:
			return fmt.Errorf("invalid seq value type: %T", firstVal)
		}
	}
	var err error
	vc.Rows, err = vc.Connector.HistoryBySeq(context.Background(), seq)
	if err != nil {
		return err
	}
	vc.Pos = 0
	return nil
}

func (vc *HistoryCursor) ColumnValue(col int) any {
	if vc.Pos < len(vc.Rows) {
		switch col {
		case 0:
			return int64(vc.Rows[vc.Pos].Seq)
		case 1:
			return strings.Join(vc.Rows[vc.Pos].SQL, ";\n")
		case 2:
			return time.Unix(0, vc.Rows[vc.Pos].Timestamp).Format("2006-01-02T15:04:05.999")
		}
	}
	return nil
}

func (vc *HistoryCursor) Next() error {
	if vc.Pos < len(vc.Rows) {
		vc.Pos++
	}
	return nil
}

func (vc *HistoryCursor) EOF() bool {
	return vc.Pos >= len(vc.Rows)
}

func (vc *HistoryCursor) Rowid() (int64, error) {
	return int64(vc.Pos + 1), nil
}

func (vc *HistoryCursor) Close() error {
	return nil
}

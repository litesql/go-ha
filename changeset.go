package ha

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
)

const controlTableName = "ha_stats"

type ChangeSet struct {
	interceptor   ChangeSetInterceptor
	connProvider  ConnHooksProvider
	applyStrategy applyStrategyFn
	Node          string   `json:"node"`
	ProcessID     int64    `json:"process_id"`
	Filename      string   `json:"filename"`
	Changes       []Change `json:"changes"`
	Timestamp     int64    `json:"timestamp_ns"`
	Subject       string   `json:"-"`
	StreamSeq     uint64   `json:"-"`
}

type applyStrategyFn func(*ChangeSet, *sql.Tx) error

func NewChangeSet(node string, replicationID string) *ChangeSet {
	return &ChangeSet{
		Node:          node,
		Filename:      replicationID,
		applyStrategy: pkIdentifyStrategy,
	}
}

func (cs *ChangeSet) SetApplyStrategy(fn applyStrategyFn) {
	cs.applyStrategy = fn
}

func (cs *ChangeSet) SetInterceptor(interceptor ChangeSetInterceptor) {
	cs.interceptor = interceptor
}

func (cs *ChangeSet) SetConnProvider(connProvider ConnHooksProvider) {
	cs.connProvider = connProvider
}

func (cs *ChangeSet) AddChange(change Change) {
	change.TsNs = time.Now().UnixNano()
	cs.Changes = append(cs.Changes, change)
}

func (cs *ChangeSet) Clear() {
	cs.Changes = nil
}

func (cs *ChangeSet) Send(pub Publisher) error {
	if len(cs.Changes) == 0 || pub == nil {
		return nil
	}
	defer cs.Clear()

	slog.Debug("Sending changeset", "changes", len(cs.Changes))
	cs.Timestamp = time.Now().UnixNano()
	return pub.Publish(cs)
}

func (cs *ChangeSet) Apply(db *sql.DB) (err error) {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return
	}
	err = cs.connProvider.DisableHooks(conn)
	if err != nil {
		return
	}
	defer cs.connProvider.EnableHooks(conn)
	if cs.interceptor != nil {
		defer func() {
			err = cs.interceptor.AfterApply(cs, conn, err)
		}()
		skip, err := cs.interceptor.BeforeApply(cs, conn)
		if err != nil {
			return err
		}
		if skip {
			return nil
		}
	}
	if len(cs.Changes) == 0 {
		return nil
	}
	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return
	}
	defer tx.Rollback()
	err = cs.applyStrategy(cs, tx)
	if err != nil {
		return
	}

	_, errStats := conn.ExecContext(context.Background(), "REPLACE INTO "+controlTableName+"(subject, received_seq, updated_at) VALUES(?, ?, ?)",
		cs.Subject, cs.StreamSeq, time.Now().Format(time.RFC3339Nano))
	if errStats != nil {
		slog.Error("failed to update "+controlTableName+" table when applying changeset", "subject", cs.Subject, "seq", cs.StreamSeq, "error", errStats)
	}
	err = tx.Commit()
	return
}

func (cs *ChangeSet) DebeziumData() []DebeziumData {
	var list []DebeziumData
	transactionID := uuid.New().String()
	for _, change := range cs.Changes {
		var data DebeziumData
		switch change.Operation {
		case "INSERT":
			data.Payload.Op = "c"
			data.Payload.After = change.after()
		case "UPDATE":
			data.Payload.Op = "u"
			data.Payload.Before = change.before()
			data.Payload.After = change.after()
		case "DELETE":
			data.Payload.Op = "d"
			data.Payload.Before = change.before()
		default:
			continue
		}
		data.Payload.Source.Version = "0.8.0"
		data.Payload.Source.Connector = "go-ha-connector"
		data.Payload.Source.Name = cs.Filename
		data.Payload.Source.DB = change.Database
		data.Payload.Source.Table = change.Table
		data.Payload.Source.TsNs = cs.Timestamp
		data.Payload.TsNs = change.TsNs
		data.Transaction.ID = transactionID
		list = append(list, data)
	}
	return list
}

func fullIdentifyStrategy(cs *ChangeSet, tx *sql.Tx) error {
	for _, change := range cs.Changes {
		if change.Table == controlTableName {
			continue
		}
		var (
			err error
			sql string
		)
		switch change.Operation {
		case "INSERT":
			sql = fmt.Sprintf("REPLACE INTO %s.%s (%s) VALUES (%s)", change.Database, change.Table, strings.Join(change.Columns, ", "), placeholders(len(change.NewValues)))
			_, err = tx.Exec(sql, change.NewValues...)
		case "UPDATE":
			setClause := make([]string, len(change.Columns))
			for i, col := range change.Columns {
				setClause[i] = fmt.Sprintf("%s = ?", col)
			}
			sql = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", change.Database, change.Table, strings.Join(setClause, ", "), strings.Join(setClause, " AND "))
			args := append(change.NewValues, change.OldValues...)
			_, err = tx.Exec(sql, args...)
		case "DELETE":
			whereClause := make([]string, len(change.Columns))
			for i, col := range change.Columns {
				whereClause[i] = fmt.Sprintf("%s = ?", col)
			}
			sql = fmt.Sprintf("DELETE FROM %s.%s WHERE %s", change.Database, change.Table, strings.Join(whereClause, " AND "))
			_, err = tx.Exec(sql, change.OldValues...)
		case "SQL":
			sql = change.Command
			_, err = tx.Exec(sql, change.Args...)
		default:
			slog.Warn("unknown operation", "operation", change.Operation)
			continue
		}
		if err != nil {
			slog.Error("failed to apply change", "error", err, "stream_seq", cs.StreamSeq, "sql", sql)
			err = errors.Join(err, tx.Rollback())
			return err
		}
	}
	return nil
}

func pkIdentifyStrategy(cs *ChangeSet, tx *sql.Tx) error {
	for _, change := range cs.Changes {
		if change.Table == controlTableName {
			continue
		}
		var (
			err error
			sql string
		)
		switch change.Operation {
		case "INSERT":
			sql = fmt.Sprintf("REPLACE INTO %s.%s (%s) VALUES (%s)", change.Database, change.Table, strings.Join(change.Columns, ", "), placeholders(len(change.NewValues)))
			_, err = tx.Exec(sql, change.NewValues...)
		case "UPDATE":
			setClause := make([]string, len(change.Columns))
			for i, col := range change.Columns {
				setClause[i] = fmt.Sprintf("%s = ?", col)
			}
			pkColumns := change.PKColumnsNames()
			whereClause := make([]string, len(pkColumns))
			for i, col := range pkColumns {
				whereClause[i] = fmt.Sprintf("%s = ?", col)
			}
			sql = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", change.Database, change.Table, strings.Join(setClause, ", "), strings.Join(whereClause, " AND "))
			args := append(change.NewValues, change.PKOldValues()...)
			_, err = tx.Exec(sql, args...)
		case "DELETE":
			pkColumns := change.PKColumnsNames()
			whereClause := make([]string, len(pkColumns))
			for i, col := range pkColumns {
				whereClause[i] = fmt.Sprintf("%s = ?", col)
			}
			sql = fmt.Sprintf("DELETE FROM %s.%s WHERE %s", change.Database, change.Table, strings.Join(whereClause, " AND "))
			_, err = tx.Exec(sql, change.PKOldValues()...)
		case "SQL":
			sql = change.Command
			_, err = tx.Exec(sql, change.Args...)
		default:
			slog.Warn("unknown operation", "operation", change.Operation)
			continue
		}
		if err != nil {
			slog.Error("failed to apply change", "error", err, "stream_seq", cs.StreamSeq, "sql", sql)
			err = errors.Join(err, tx.Rollback())
			return err
		}
	}
	return nil
}

func rowidIdentifyStrategy(cs *ChangeSet, tx *sql.Tx) error {
	for _, change := range cs.Changes {
		if change.Table == controlTableName {
			continue
		}
		var (
			err error
			sql string
		)
		switch change.Operation {
		case "INSERT":
			setClause := make([]string, len(change.Columns))
			for i, col := range change.Columns {
				setClause[i] = fmt.Sprintf("%s = ?%d", col, i+1)
			}
			sql = fmt.Sprintf("INSERT INTO %s.%s (%s, `rowid`) VALUES (%s) ON CONFLICT DO UPDATE SET %s, `rowid` = ?%d;", change.Database, change.Table, strings.Join(change.Columns, ", "), placeholders(len(change.NewValues)+1), strings.Join(setClause, ", "), len(change.NewValues)+1)
			_, err = tx.Exec(sql, append(change.NewValues, change.NewRowID)...)
		case "UPDATE":
			setClause := make([]string, len(change.Columns))
			for i, col := range change.Columns {
				setClause[i] = fmt.Sprintf("%s = ?", col)
			}
			sql = fmt.Sprintf("UPDATE %s.%s SET %s WHERE `rowid` = ?;", change.Database, change.Table, strings.Join(setClause, ", "))
			args := append(change.NewValues, change.OldRowID)
			_, err = tx.Exec(sql, args...)
		case "DELETE":
			sql = fmt.Sprintf("DELETE FROM %s.%s WHERE `rowid` = ?;", change.Database, change.Table)
			_, err = tx.Exec(sql, change.OldRowID)
		case "SQL":
			sql = change.Command
			_, err = tx.Exec(sql, change.Args...)
		default:
			slog.Warn("unknown operation", "operation", change.Operation)
			continue
		}
		if err != nil {
			slog.Error("failed to apply change", "error", err, "stream_seq", cs.StreamSeq, "sql", sql)
			err = errors.Join(err, tx.Rollback())
			return err
		}
	}
	return nil
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	var b strings.Builder
	for i := range n {
		fmt.Fprintf(&b, "?%d,", i+1)
	}
	return strings.TrimRight(b.String(), ",")
}

type Change struct {
	Database  string   `json:"database,omitempty"`
	Table     string   `json:"table,omitempty"`
	Columns   []string `json:"columns,omitempty"`
	PKColumns []string `json:"pk_columns,omitempty"`
	Operation string   `json:"operation"` // "INSERT", "UPDATE", "DELETE", "SQL", "CUSTOM"
	OldRowID  int64    `json:"old_rowid,omitempty"`
	NewRowID  int64    `json:"new_rowid,omitempty"`
	OldValues []any    `json:"old_values,omitempty"`
	NewValues []any    `json:"new_values,omitempty"`
	Command   string   `json:"command,omitempty"`
	Args      []any    `json:"args,omitempty"`
	TsNs      int64    `json:"ts_ns,omitempty"`
}

func (c Change) PKColumnsNames() []string {
	if len(c.PKColumns) == 0 {
		return []string{"rowid"}
	}
	return c.PKColumns
}

func (c Change) PKOldValues() []any {
	if len(c.PKColumns) == 0 {
		return []any{c.OldRowID}
	}
	pkValues := make([]any, len(c.PKColumns))
	if len(c.OldValues) == len(c.Columns) {
		pkIndex := 0
		for i, col := range c.Columns {
			if col == c.PKColumns[pkIndex] {
				pkValues[pkIndex] = c.OldValues[i]
				pkIndex++
				if pkIndex >= len(c.PKColumns) {
					break
				}
			}
		}
	}
	return pkValues
}

func (c Change) PKNewValues() []any {
	if len(c.PKColumns) == 0 {
		return []any{c.NewRowID}
	}
	pkValues := make([]any, len(c.PKColumns))
	if len(c.NewValues) == len(c.Columns) {
		pkIndex := 0
		for i, col := range c.Columns {
			if col == c.PKColumns[pkIndex] {
				pkValues[pkIndex] = c.NewValues[i]
				pkIndex++
				if pkIndex >= len(c.PKColumns) {
					break
				}
			}
		}
	}
	return pkValues
}

func (c Change) before() map[string]any {
	before := make(map[string]any)
	for i, col := range c.Columns {
		if i < len(c.OldValues) {
			before[col] = c.OldValues[i]
		} else {
			before[col] = nil
		}
	}
	return before
}

func (c Change) after() map[string]any {
	after := make(map[string]any)
	for i, col := range c.Columns {
		if i < len(c.NewValues) {
			after[col] = c.NewValues[i]
		} else {
			after[col] = nil
		}
	}
	return after
}

type DebeziumData struct {
	Schema      any                 `json:"schema"`
	Payload     DebeziumPayload     `json:"payload"`
	Transaction DebeziumTransaction `json:"transaction"`
}

type DebeziumPayload struct {
	Before map[string]any `json:"before"`
	After  map[string]any `json:"after"`
	Source DebeziumSource `json:"source"`
	Op     string         `json:"op"`
	TsNs   int64          `json:"ts_ns"`
}

type DebeziumSource struct {
	Version   string `json:"version"`
	Connector string `json:"connector"`
	Name      string `json:"name"`
	ServerID  int64  `json:"server_id"`
	TsNs      int64  `json:"ts_ns"`
	DB        string `json:"db"`
	Table     string `json:"table"`
}

type DebeziumTransaction struct {
	ID string `json:"id"`
}

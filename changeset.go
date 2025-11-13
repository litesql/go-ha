package ha

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

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

func NewChangeSet(node string, filename string) *ChangeSet {
	return &ChangeSet{
		Node:          node,
		Filename:      filename,
		applyStrategy: rowidIdentifyStrategy,
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
	cs.Changes = append(cs.Changes, change)
}

func (cs *ChangeSet) Clear() {
	cs.Changes = nil
}

func (cs *ChangeSet) Send(pub CDCPublisher) error {
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

	_, errStats := conn.ExecContext(context.Background(), "REPLACE INTO ha_stats(subject, received_seq, updated_at) VALUES(?, ?, ?)",
		cs.Subject, cs.StreamSeq, time.Now().Format(time.RFC3339Nano))
	if errStats != nil {
		slog.Error("failed to update ha_stats table when applying changeset", "subject", cs.Subject, "seq", cs.StreamSeq, "error", errStats)
	}
	err = tx.Commit()
	return
}

func fullIdentifyStrategy(cs *ChangeSet, tx *sql.Tx) error {
	for _, change := range cs.Changes {
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

func rowidIdentifyStrategy(cs *ChangeSet, tx *sql.Tx) error {
	for _, change := range cs.Changes {
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
			sql = fmt.Sprintf("INSERT INTO %s.%s (%s, rowid) VALUES (%s) ON CONFLICT DO UPDATE SET %s, rowid = ?%d;", change.Database, change.Table, strings.Join(change.Columns, ", "), placeholders(len(change.NewValues)+1), strings.Join(setClause, ", "), len(change.NewValues)+1)
			_, err = tx.Exec(sql, append(change.NewValues, change.NewRowID)...)
		case "UPDATE":
			setClause := make([]string, len(change.Columns))
			for i, col := range change.Columns {
				setClause[i] = fmt.Sprintf("%s = ?", col)
			}
			sql = fmt.Sprintf("UPDATE %s.%s SET %s WHERE rowid = ?;", change.Database, change.Table, strings.Join(setClause, ", "))
			args := append(change.NewValues, change.OldRowID)
			_, err = tx.Exec(sql, args...)
		case "DELETE":
			sql = fmt.Sprintf("DELETE FROM %s.%s WHERE rowid = ?;", change.Database, change.Table)
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
		b.WriteString(fmt.Sprintf("?%d,", i+1))
	}
	return strings.TrimRight(b.String(), ",")
}

type Change struct {
	Database  string   `json:"database,omitempty"`
	Table     string   `json:"table,omitempty"`
	Columns   []string `json:"columns,omitempty"`
	Operation string   `json:"operation"` // "INSERT", "UPDATE", "DELETE", "SQL", "CUSTOM"
	OldRowID  int64    `json:"old_rowid,omitempty"`
	NewRowID  int64    `json:"new_rowid,omitempty"`
	OldValues []any    `json:"old_values,omitempty"`
	NewValues []any    `json:"new_values,omitempty"`
	Command   string   `json:"command,omitempty"`
	Args      []any    `json:"args,omitempty"`
}

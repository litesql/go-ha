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
	interceptor  ChangeSetInterceptor
	connProvider ConnHooksProvider
	Node         string   `json:"node"`
	ProcessID    int64    `json:"process_id"`
	Filename     string   `json:"filename"`
	Changes      []Change `json:"changes"`
	Timestamp    int64    `json:"timestamp_ns"`
	Subject      string   `json:"-"`
	StreamSeq    uint64   `json:"-"`
	LocalProdSeq uint64   `json:"-"`
}

func NewChangeSet(node string, filename string) *ChangeSet {
	return &ChangeSet{
		Node:     node,
		Filename: filename,
	}
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
	for _, change := range cs.Changes {
		var sql string
		switch change.Operation {
		case "INSERT":
			if cs.LocalProdSeq > cs.StreamSeq {
				sql = fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT DO NOTHING", change.Database, change.Table, strings.Join(change.Columns, ", "), placeholders(len(change.NewValues)))
			} else {
				sql = fmt.Sprintf("REPLACE INTO %s.%s (%s) VALUES (%s)", change.Database, change.Table, strings.Join(change.Columns, ", "), placeholders(len(change.NewValues)))
			}
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
			return
		}
	}
	_, err = conn.ExecContext(context.Background(), "REPLACE INTO ha_stats(subject, received_seq, produced_seq, updated_at) VALUES(?, ?, ?, ?)",
		cs.Subject, cs.StreamSeq, cs.LocalProdSeq, time.Now().Format(time.RFC3339))
	if err != nil {
		slog.Error("failed to update ha_stats table", "error", err)
		return
	}
	err = tx.Commit()
	return
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

package grpc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/anypb"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	"github.com/litesql/go-ha/api/sql/v1/sqlv1connect"
)

type Service struct {
	sqlv1connect.UnimplementedDatabaseServiceHandler
	DBProvider                    DBProvider
	DSNList                       DataSourceNamesFn
	ReplicationIDList             ReplicationIDsFn
	SQLExpectResultSet            SQLExpectResultSetFn
	TwoPhaseCommitWorkerConverter TwoPhaseCommitWorkerConverter
}

type HistoryItem struct {
	Seq       uint64   `json:"seq"`
	SQL       []string `json:"sql"`
	Timestamp int64    `json:"timestamp_ns"`
}

type UndoFilter int

const (
	UndoFilterNone UndoFilter = iota
	UndoFilterEntity
	UndoFilterTransaction
)

func historyToData(items []HistoryItem) *sqlv1.Data {
	var data sqlv1.Data
	data.Columns = []string{"seq", "sql", "timestamp"}
	for _, item := range items {
		seq, _ := ToAnypb(item.Seq)
		sqls := make([]string, len(item.SQL))
		for i, sql := range item.SQL {
			sqls[i] = strings.TrimSuffix(sql, ";")
		}
		sqlListAny, _ := ToAnypb(strings.Join(sqls, ";\n"))
		timestamp, _ := ToAnypb(time.Unix(0, item.Timestamp))
		data.Rows = append(data.Rows, &sqlv1.Row{
			Values: []*anypb.Any{
				seq,
				sqlListAny,
				timestamp,
			},
		})
	}

	return &data
}

type HADB interface {
	PubSeq() uint64
	DB() *sql.DB
	LatestSnapshot(context.Context) (uint64, io.ReadCloser, error)
	Backup(context.Context, io.Writer) error
	HistoryBySeq(context.Context, uint64) ([]HistoryItem, error)
	HistoryByTime(context.Context, time.Duration) ([]HistoryItem, error)
	UndoBySeq(context.Context, uint64, UndoFilter, map[string][]int64) error
	UndoByTime(context.Context, time.Duration, UndoFilter, map[string][]int64) error
	Mutex() *sync.Mutex
	DisableHooks(*sql.Conn) error
	EnableHooks(*sql.Conn) error
}

type TwoPhaseCommitWorker interface {
	Prepare(*sql.DB) (*ConnHooksEnabler, *sql.Tx, error)
	AfterCommit(conn *sql.Conn, err error) error
	UndoLocal(*sql.DB) error
}

type TwoPhaseCommitWorkerConverter func(*sqlv1.ChangeSetRequest) (TwoPhaseCommitWorker, error)

type ConnHooksEnabler struct {
	SqlConn         *sql.Conn
	ConnHooksEnable func(*sql.Conn) error
}

func (c *ConnHooksEnabler) Close() error {
	return errors.Join(c.ConnHooksEnable(c.SqlConn), c.SqlConn.Close())
}

type DBProvider func(id string) (HADB, bool)

type DataSourceNamesFn func() []string
type ReplicationIDsFn func() []string
type SQLExpectResultSetFn func(context.Context, string) (bool, error)

type execQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (s *Service) Query(ctx context.Context, stream *connect.BidiStream[sqlv1.QueryRequest, sqlv1.QueryResponse]) error {
	var (
		hadb HADB
		db   *sql.DB
		conn *sql.Conn
		tx   *sql.Tx
	)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
		if conn != nil {
			conn.Close()
		}
	}()
	for {
		req, err := stream.Receive()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if req.Type == sqlv1.QueryType_QUERY_TYPE_PING {
			err := stream.Send(&sqlv1.QueryResponse{})
			if err != nil {
				return err
			}
			continue
		}

		id := req.GetReplicationId()
		if id == "" {
			if list := s.ReplicationIDList(); len(list) == 1 {
				id = list[0]
			}
		}
		var ok bool
		hadb, ok = s.DBProvider(id)
		if !ok {
			err := stream.Send(&sqlv1.QueryResponse{
				Error: fmt.Sprintf("database provider %q not found. Use 'SET DATABASE = ?;' command to choose between available databases: %v", req.GetReplicationId(), s.ReplicationIDList()),
			})
			if err != nil {
				return err
			}
			continue
		}

		newdb := hadb.DB()
		if newdb == nil {
			err := stream.Send(&sqlv1.QueryResponse{
				Error: fmt.Sprintf("database pool for %q not found", req.GetReplicationId()),
			})
			if err != nil {
				return err
			}
			continue
		}
		if newdb != db {
			newConn, err := newdb.Conn(ctx)
			if err != nil {
				err := stream.Send(&sqlv1.QueryResponse{
					Error: fmt.Sprintf("failed to get connection from database pool for %q: %v", req.GetReplicationId(), err),
				})
				if err != nil {
					return err
				}
				continue
			}
			if tx != nil {
				tx.Rollback()
				tx = nil
			}
			if conn != nil {
				conn.Close()
				conn = nil
			}
			conn = newConn
			db = newdb
		}

		sqlQuery := req.GetSql()
		slog.Debug("gRPC service", "query", sqlQuery)

		upperSQL := strings.ToUpper(sqlQuery)
		switch {
		case strings.HasPrefix(upperSQL, "BEGIN"):
			if tx != nil {
				//NOOP
				err = stream.Send(&sqlv1.QueryResponse{})
				if err != nil {
					return err
				}
				continue
			}
			tx, err = conn.BeginTx(ctx, nil)
			if err != nil {
				err := stream.Send(&sqlv1.QueryResponse{
					Error: err.Error(),
				})
				if err != nil {
					return err
				}
				continue
			}
			err = stream.Send(&sqlv1.QueryResponse{})
			if err != nil {
				return err
			}
			continue
		case strings.HasPrefix(upperSQL, "COMMIT"), strings.HasPrefix(upperSQL, "END"):
			if tx == nil {
				//NOOP
				err = stream.Send(&sqlv1.QueryResponse{})
				if err != nil {
					return err
				}
				continue
			}
			if err := tx.Commit(); err != nil {
				err := stream.Send(&sqlv1.QueryResponse{
					Error: err.Error(),
				})
				if err != nil {
					return err
				}
				continue
			}
			tx = nil
			err = stream.Send(&sqlv1.QueryResponse{})
			if err != nil {
				return err
			}
			continue
		case strings.HasPrefix(upperSQL, "ROLLBACK"):
			if tx == nil {
				//NOOP
				err = stream.Send(&sqlv1.QueryResponse{})
				if err != nil {
					return err
				}
				continue
			}
			if err := tx.Rollback(); err != nil {
				err := stream.Send(&sqlv1.QueryResponse{
					Error: err.Error(),
				})
				if err != nil {
					return err
				}
				continue
			}
			tx = nil
			err = stream.Send(&sqlv1.QueryResponse{})
			if err != nil {
				return err
			}
			continue
		case strings.HasPrefix(upperSQL, "HISTORY"):
			sqlQuery = strings.TrimSpace(sqlQuery)
			sqlQuery = strings.ReplaceAll(sqlQuery, "\t", " ")
			sqlQuery = strings.ReplaceAll(sqlQuery, "\n", " ")
			sqlQuery = strings.ReplaceAll(sqlQuery, "\r", " ")
			sqlQuery = strings.TrimSuffix(sqlQuery, ";")
			parts := strings.Fields(sqlQuery)
			var seq uint64
			if len(parts) > 2 {
				err = stream.Send(&sqlv1.QueryResponse{
					Error: "HISTORY command requires exactly one optional argument: HISTORY <stream sequence|time duration>",
				})
				if err != nil {
					return err
				}
				continue
			}
			if len(parts) == 2 {
				seq, err = strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					duration, err := time.ParseDuration(parts[1])
					if err != nil {
						err = stream.Send(&sqlv1.QueryResponse{
							Error: fmt.Sprintf("HISTORY command got %q but requires a positive integer argument or a time duration: HISTORY <stream sequence|time duration>", parts[1]),
						})
						if err != nil {
							return err
						}
						continue
					}
					items, err := hadb.HistoryByTime(ctx, duration)
					if err != nil {
						err = stream.Send(&sqlv1.QueryResponse{
							Error: fmt.Sprintf("failed to retrieve history within %v: %v", duration, err),
						})
						if err != nil {
							return err
						}
						continue
					}
					err = stream.Send(&sqlv1.QueryResponse{
						ResultSet: historyToData(items),
					})
					if err != nil {
						return err
					}
					continue
				}
			}
			items, err := hadb.HistoryBySeq(ctx, seq)
			if err != nil {
				err = stream.Send(&sqlv1.QueryResponse{
					Error: fmt.Sprintf("failed to retrieve history: %v", err),
				})
				if err != nil {
					return err
				}
				continue
			}
			err = stream.Send(&sqlv1.QueryResponse{
				ResultSet: historyToData(items),
			})
			if err != nil {
				return err
			}
			continue
		case strings.HasPrefix(upperSQL, "UNDO"):
			if tx != nil {
				err = stream.Send(&sqlv1.QueryResponse{
					Error: "cannot execute UNDO command within a transaction",
				})
				if err != nil {
					return err
				}
				continue
			}
			sqlQuery = strings.TrimSpace(sqlQuery)
			sqlQuery = strings.ReplaceAll(sqlQuery, "\t", " ")
			sqlQuery = strings.ReplaceAll(sqlQuery, "\n", " ")
			sqlQuery = strings.ReplaceAll(sqlQuery, "\r", " ")
			sqlQuery = strings.TrimSuffix(sqlQuery, ";")
			parts := strings.Fields(sqlQuery)
			// UNDO command can be in the form of (WHERE clause is optional):
			// UNDO <stream sequence> WHERE <table>(<rowids>)
			// UNDO <time duration> WHERE <table>(<rowids>)
			if len(parts) > 4 || (len(parts) > 2 && !strings.EqualFold(parts[2], "WHERE")) {
				err = stream.Send(&sqlv1.QueryResponse{
					Error: "UNDO command syntax: UNDO <stream sequence|time duration> [WHERE <table>(<rowids>)]",
				})
				if err != nil {
					return err
				}
				continue
			}
			filterEntities := make(map[string][]int64)
			if len(parts) == 4 {
				whereClause := parts[3]
				openParen := strings.Index(whereClause, "(")
				closeParen := strings.Index(whereClause, ")")
				if openParen == -1 || closeParen == -1 || closeParen < openParen {
					err = stream.Send(&sqlv1.QueryResponse{
						Error: "invalid WHERE clause syntax. Expected format: WHERE <table>(<rowids>)",
					})
					if err != nil {
						return err
					}
					continue
				}
				table := whereClause[:openParen]
				rowIdsStr := whereClause[openParen+1 : closeParen]
				rowIdStrs := strings.Split(rowIdsStr, ",")
				var rowIds []int64
				for _, idStr := range rowIdStrs {
					idStr = strings.TrimSpace(idStr)
					id, err := strconv.ParseInt(idStr, 10, 64)
					if err != nil {
						err = stream.Send(&sqlv1.QueryResponse{
							Error: fmt.Sprintf("invalid row id %q in WHERE clause", idStr),
						})
						if err != nil {
							return err
						}
						continue
					}
					rowIds = append(rowIds, id)
				}
				if len(rowIds) == 0 {
					err = stream.Send(&sqlv1.QueryResponse{
						Error: "no valid row ids found in WHERE clause",
					})
					if err != nil {
						return err
					}
					continue
				}
				if table == "" {
					err = stream.Send(&sqlv1.QueryResponse{
						Error: "table name cannot be empty in WHERE clause",
					})
					if err != nil {
						return err
					}
					continue
				}
				filterEntities[table] = rowIds
			}

			var filter UndoFilter
			switch {
			case strings.HasPrefix(upperSQL, "UNDOT"):
				filter = UndoFilterTransaction
			case strings.HasPrefix(upperSQL, "UNDOE"):
				filter = UndoFilterEntity
			default:
				filter = UndoFilterNone
			}
			var seq uint64
			if len(parts) > 1 {
				seq, err = strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					if filter != UndoFilterNone {
						err = stream.Send(&sqlv1.QueryResponse{
							Error: fmt.Sprintf("%s command got %q but requires a positive integer argument: %s <stream sequence>", parts[0], parts[1], parts[0]),
						})
						if err != nil {
							return err
						}
						continue
					}
					duration, err := time.ParseDuration(parts[1])
					if err != nil {
						err = stream.Send(&sqlv1.QueryResponse{
							Error: fmt.Sprintf("UNDO command got %q but requires a positive integer argument or a time duration: UNDO <stream sequence|time duration>", parts[1]),
						})
						if err != nil {
							return err
						}
						continue
					}
					err = hadb.UndoByTime(ctx, duration, filter, filterEntities)
					if err != nil {
						err = stream.Send(&sqlv1.QueryResponse{
							Error: fmt.Sprintf("failed to undo transactions within %v: %v", duration, err),
						})
						if err != nil {
							return err
						}
						continue
					}
					err = stream.Send(&sqlv1.QueryResponse{})
					if err != nil {
						return err
					}
					continue
				}
			}

			err = hadb.UndoBySeq(ctx, seq, filter, filterEntities)
			if err != nil {
				err = stream.Send(&sqlv1.QueryResponse{
					Error: fmt.Sprintf("failed to undo transactions from stream sequence %d: %v", seq, err),
				})
				if err != nil {
					return err
				}
				continue
			}
			err = stream.Send(&sqlv1.QueryResponse{})
			if err != nil {
				return err
			}
			continue
		default:
			var ex execQuerier
			if tx != nil {
				ex = tx
			} else {
				ex = conn
			}

			params := req.GetParams()
			args := make([]any, len(params))
			if len(params) > 0 {
				if params[0].Name != "" {
					for i, param := range params {
						args[i] = sql.Named(param.Name, FromAnypb(param.Value))
					}
				} else {
					max := int64(len(params))
					for _, param := range params {
						if param.Ordinal < 1 || param.Ordinal > max {
							err = stream.Send(&sqlv1.QueryResponse{
								Error: fmt.Sprintf("Invalid param: index(%d) max(%d)", param.Ordinal, max),
							})
							if err != nil {
								return err
							}
							continue
						}
						args[param.Ordinal-1] = FromAnypb(param.Value)
					}
				}
			}

			switch req.Type {
			case sqlv1.QueryType_QUERY_TYPE_EXEC_QUERY:
				err := s.execQuery(ctx, stream, hadb, ex, sqlQuery, args)
				if err != nil {
					return err
				}
			case sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE:
				err := s.execUpdate(ctx, stream, hadb, ex, sqlQuery, args)
				if err != nil {
					return err
				}
			default:
				expectResultSet, err := s.SQLExpectResultSet(ctx, sqlQuery)
				if err != nil {
					err := stream.Send(&sqlv1.QueryResponse{
						Error: err.Error(),
					})
					if err != nil {
						return err
					}
					continue
				}
				if expectResultSet {
					err = s.execQuery(ctx, stream, hadb, ex, sqlQuery, args)
					if err != nil {
						return err
					}
				} else {
					err = s.execUpdate(ctx, stream, hadb, ex, sqlQuery, args)
					if err != nil {
						return err
					}
				}
			}
		}
	}
}

func (s *Service) execQuery(ctx context.Context, stream *connect.BidiStream[sqlv1.QueryRequest, sqlv1.QueryResponse], hadb HADB, ex execQuerier, sqlQuery string, args []any) error {
	resp := query(ctx, ex, sqlQuery, args...)
	resp.Txseq = hadb.PubSeq()
	return stream.Send(resp)
}

func (s *Service) execUpdate(ctx context.Context, stream *connect.BidiStream[sqlv1.QueryRequest, sqlv1.QueryResponse], hadb HADB, ex execQuerier, sqlQuery string, args []any) error {
	res, err := ex.ExecContext(ctx, sqlQuery, args...)
	if err != nil {
		return stream.Send(&sqlv1.QueryResponse{
			Error: err.Error(),
		})
	}
	lastInsertID, _ := res.LastInsertId()
	rowsAffected, _ := res.RowsAffected()
	return stream.Send(&sqlv1.QueryResponse{
		LastInsertId: lastInsertID,
		RowsAffected: rowsAffected,
		Txseq:        hadb.PubSeq(),
	})
}

func (s *Service) DataSourceNames(ctx context.Context, req *connect.Request[sqlv1.DataSourceNamesRequest]) (*connect.Response[sqlv1.DataSourceNamesResponse], error) {
	return connect.NewResponse(&sqlv1.DataSourceNamesResponse{
		Dsn: s.DSNList(),
	}), nil
}

const chunkSize = 64 * 1024 // 64KB

func (s *Service) Download(ctx context.Context, req *connect.Request[sqlv1.DownloadRequest], resp *connect.ServerStream[sqlv1.DownloadResponse]) error {
	db, ok := s.DBProvider(req.Msg.ReplicationId)
	if !ok {
		return fmt.Errorf("database with replication id %q not found", req.Msg.ReplicationId)
	}

	err := db.Backup(ctx, &streamWriter{resp: resp})
	if err != nil {
		return err
	}

	return nil
}

type streamWriter struct {
	resp *connect.ServerStream[sqlv1.DownloadResponse]
}

func (sw *streamWriter) Write(p []byte) (n int, err error) {
	total := len(p)
	sent := 0
	for sent < total {
		chunkSize := chunkSize
		remaining := total - sent
		if remaining < chunkSize {
			chunkSize = remaining
		}
		err := sw.resp.Send(&sqlv1.DownloadResponse{
			Data: p[sent : sent+chunkSize],
		})
		if err != nil {
			return sent, err
		}
		sent += chunkSize
	}
	return total, nil
}

func (s *Service) LatestSnapshot(ctx context.Context, req *connect.Request[sqlv1.LatestSnapshotRequest], resp *connect.ServerStream[sqlv1.LatestSnapshotResponse]) error {
	db, ok := s.DBProvider(req.Msg.ReplicationId)
	if !ok {
		return fmt.Errorf("database with replication id %q not found", req.Msg.ReplicationId)
	}
	_, reader, err := db.LatestSnapshot(ctx)
	if err != nil {
		return err
	}
	defer reader.Close()

	buf := make([]byte, chunkSize)
	for {
		n, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if n == 0 {
			break
		}
		err = resp.Send(&sqlv1.LatestSnapshotResponse{
			Data: buf[:n],
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) ReplicationIDs(ctx context.Context, req *connect.Request[sqlv1.ReplicationIDsRequest]) (*connect.Response[sqlv1.ReplicationIDsResponse], error) {
	return connect.NewResponse(&sqlv1.ReplicationIDsResponse{
		ReplicationId: s.ReplicationIDList(),
	}), nil
}

var (
	changeSetUndoSchemaInit   = make(map[string]struct{})
	muChangeSetUndoSchemaInit sync.Mutex
)

// used on two phase commit recovery process
func createChangeSetUndoTable(id string, hadb HADB) error {
	muChangeSetUndoSchemaInit.Lock()
	defer muChangeSetUndoSchemaInit.Unlock()

	if _, ok := changeSetUndoSchemaInit[id]; ok {
		return nil
	}

	conn, err := hadb.DB().Conn(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get connection for changeset undo table: %w", err)
	}
	defer conn.Close()

	err = hadb.DisableHooks(conn)
	if err != nil {
		return fmt.Errorf("failed to disable hooks for changeset undo table: %w", err)
	}
	defer hadb.EnableHooks(conn)

	_, err = conn.ExecContext(context.Background(),
		`CREATE TABLE IF NOT EXISTS ha_2pc_latest_undo(
			id INTEGER PRIMARY KEY CHECK (id = 1),			
			timestamp_ns INTEGER
	)`)
	if err != nil {
		return fmt.Errorf("create changeset recovery table for %q: %w", id, err)
	}
	return nil
}

func (s *Service) ChangeSet(ctx context.Context, stream *connect.BidiStream[sqlv1.ChangeSetRequest, sqlv1.ChangeSetResponse]) error {
	var (
		replicationID string
		hadb          HADB
		db            *sql.DB
		conn          *ConnHooksEnabler
		tx            *sql.Tx
	)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
		if conn != nil {
			conn.Close()
		}
	}()
	for {
		req, err := stream.Receive()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if req.Type == sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_PING {
			err := stream.Send(&sqlv1.ChangeSetResponse{})
			if err != nil {
				return err
			}
			continue
		}

		if hadb == nil {
			replicationID = req.GetReplicationId()
			if replicationID == "" {
				if list := s.ReplicationIDList(); len(list) == 1 {
					replicationID = list[0]
				}
			}
			var ok bool
			hadb, ok = s.DBProvider(replicationID)
			if !ok {
				err := stream.Send(&sqlv1.ChangeSetResponse{
					Error: fmt.Sprintf("database provider %q not found. Available databases: %v", req.GetReplicationId(), s.ReplicationIDList()),
				})
				if err != nil {
					return err
				}
				continue
			}
			db = hadb.DB()
			if db == nil {
				err := stream.Send(&sqlv1.ChangeSetResponse{
					Error: fmt.Sprintf("database pool for %q not found", req.GetReplicationId()),
				})
				if err != nil {
					return err
				}
				continue
			}
			hadb.Mutex().Lock()
			defer hadb.Mutex().Unlock()
			if err := createChangeSetUndoTable(replicationID, hadb); err != nil {
				return err
			}
		}

		worker, err := s.TwoPhaseCommitWorkerConverter(req)
		if err != nil {
			err := stream.Send(&sqlv1.ChangeSetResponse{
				Error: err.Error(),
			})
			if err != nil {
				return err
			}
			continue
		}

		switch req.Type {
		case sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_PREPARE:
			if tx != nil {
				tx.Rollback()
				tx = nil
			}
			if conn != nil {
				conn.Close()
				conn = nil
			}
			conn, tx, err = worker.Prepare(db)
			if err != nil {
				if tx != nil {
					err = errors.Join(err, tx.Rollback())
					tx = nil
				}
				if conn != nil {
					err = errors.Join(err, conn.Close())
					conn = nil
				}
				err := stream.Send(&sqlv1.ChangeSetResponse{
					Error: fmt.Sprintf("prepare: %s", err.Error()),
				})
				if err != nil {
					return err
				}
				continue
			}
			err := stream.Send(&sqlv1.ChangeSetResponse{})
			if err != nil {
				return err
			}
		case sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_COMMIT:
			if tx == nil {
				err := stream.Send(&sqlv1.ChangeSetResponse{
					Error: "no active transaction",
				})
				if err != nil {
					return err
				}
				continue
			}

			err = tx.Commit()
			tx = nil
			var msg string
			err = errors.Join(err, worker.AfterCommit(conn.SqlConn, err))
			if err != nil {
				msg = err.Error()
			}
			if conn != nil {
				err = errors.Join(err, conn.Close())
				conn = nil
			}
			if err := stream.Send(&sqlv1.ChangeSetResponse{Error: msg}); err != nil {
				return err
			}
			continue
		case sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_ABORT:
			if tx == nil {
				err := stream.Send(&sqlv1.ChangeSetResponse{
					Error: "no active transaction",
				})
				if err != nil {
					return err
				}
				continue
			}

			err = tx.Rollback()
			tx = nil
			if conn != nil {
				err = errors.Join(err, conn.Close())
				conn = nil
			}
			var msg string
			if err != nil {
				msg = err.Error()
			}
			if err := stream.Send(&sqlv1.ChangeSetResponse{Error: msg}); err != nil {
				return err
			}
			continue
		case sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_UNDO:
			if tx != nil {
				err := stream.Send(&sqlv1.ChangeSetResponse{
					Error: "transaction is active",
				})
				if err != nil {
					return err
				}
				continue
			}
			err = worker.UndoLocal(db)
			if err != nil {
				err := stream.Send(&sqlv1.ChangeSetResponse{
					Error: fmt.Sprintf("undo: %s", err.Error()),
				})
				if err != nil {
					return err
				}
				continue
			}
		case sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_UNDO_AFTER_CRASH:
			if tx != nil {
				err := stream.Send(&sqlv1.ChangeSetResponse{
					Error: "transaction is active",
				})
				if err != nil {
					return err
				}
				continue
			}
			var latestUndoTimestamp int64
			db.QueryRowContext(ctx, "SELECT timestamp_ns FROM ha_2pc_latest_undo WHERE id = 1").Scan(&latestUndoTimestamp)
			if latestUndoTimestamp == req.TimestampNs {
				// The latest undo operation has already been applied
				err := stream.Send(&sqlv1.ChangeSetResponse{})
				if err != nil {
					return err
				}
				continue
			}
			err = worker.UndoLocal(db)
			if err != nil {
				err := stream.Send(&sqlv1.ChangeSetResponse{
					Error: fmt.Sprintf("undo after crash: %s", err.Error()),
				})
				if err != nil {
					return err
				}
				continue
			}
		}
	}
}

func query(ctx context.Context, ex execQuerier, query string, args ...any) *sqlv1.QueryResponse {
	rows, err := ex.QueryContext(ctx, query, args...)
	if err != nil {
		return &sqlv1.QueryResponse{
			Error: err.Error(),
		}
	}
	defer rows.Close()

	var data sqlv1.Data
	data.Columns, err = rows.Columns()
	if err != nil {
		return &sqlv1.QueryResponse{
			Error: err.Error(),
		}
	}

	columns := make([]any, len(data.Columns))
	columnPointers := make([]any, len(data.Columns))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}

	for rows.Next() {
		if err := rows.Scan(columnPointers...); err != nil {
			return &sqlv1.QueryResponse{
				Error: err.Error(),
			}
		}
		row := make([]*anypb.Any, len(data.Columns))
		for i, c := range columnPointers {
			if c == nil {
				row[i] = nil
				continue
			}
			row[i], err = ToAnypb(*c.(*any))
			if err != nil {
				return &sqlv1.QueryResponse{
					Error: err.Error(),
				}
			}
		}

		data.Rows = append(data.Rows, &sqlv1.Row{
			Values: row,
		})
	}

	if err := rows.Err(); err != nil {
		return &sqlv1.QueryResponse{
			Error: err.Error(),
		}
	}

	return &sqlv1.QueryResponse{
		ResultSet: &data,
	}
}

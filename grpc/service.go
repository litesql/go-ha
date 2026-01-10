package grpc

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
)

type Service struct {
	sqlv1.UnimplementedDatabaseServiceServer
	DBProvider         DBProvider
	DSNList            DataSourceNamesFn
	ReplicationIDList  ReplicationIDsFn
	SQLExpectResultSet SQLExpectResultSetFn
}

type HADB interface {
	PubSeq() uint64
	DB() *sql.DB
	LatestSnapshot(context.Context) (uint64, io.ReadCloser, error)
}

type DBProvider func(id string) (HADB, bool)

type DataSourceNamesFn func() []string
type ReplicationIDsFn func() []string
type SQLExpectResultSetFn func(context.Context, string) (bool, error)

type execQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (s *Service) Query(stream grpc.BidiStreamingServer[sqlv1.QueryRequest, sqlv1.QueryResponse]) error {
	var (
		hadb HADB
		db   *sql.DB
		tx   *sql.Tx
	)
	if list := s.ReplicationIDList(); len(list) == 1 {
		hadb, _ = s.DBProvider(list[0])
		db = hadb.DB()
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		id := req.GetReplicationId()
		if id != "" {
			var ok bool
			hadb, ok = s.DBProvider(id)
			if !ok {
				err := stream.Send(&sqlv1.QueryResponse{
					Error: fmt.Sprintf("database provider %q not found", req.GetReplicationId()),
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
			if tx != nil && db != newdb {
				tx.Rollback()
				tx = nil
			}
			db = newdb
		}
		sqlQuery := req.GetSql()
		slog.Debug("gRPC service", "query", sqlQuery)
		upperSQL := strings.ToUpper(sqlQuery)
		switch {
		case strings.HasPrefix(upperSQL, "BEGIN"):
			if tx != nil {
				err := stream.Send(&sqlv1.QueryResponse{
					Error: "trasaction already exists",
				})
				if err != nil {
					return err
				}
				continue
			}
			tx, err = db.Begin()
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
		case strings.HasPrefix(upperSQL, "COMMIT"):
			if tx == nil {
				err := stream.Send(&sqlv1.QueryResponse{
					Error: "trasaction not exists",
				})
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
				err := stream.Send(&sqlv1.QueryResponse{
					Error: "trasaction not exists",
				})
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
		default:
			var ex execQuerier
			if tx != nil {
				ex = tx
			} else {
				ex = db
			}

			params := req.GetParams()
			args := make([]any, len(params))
			for i, param := range params {
				if param.Name != "" {
					args[i] = sql.Named(param.Name, FromAnypb(param.Value))
				}
			}

			switch req.Type {
			case sqlv1.QueryType_QUERY_TYPE_EXEC_QUERY:
				err := s.execQuery(stream, hadb, ex, sqlQuery, args)
				if err != nil {
					return err
				}
			case sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE:
				err := s.execUpdate(stream, hadb, ex, sqlQuery, args)
				if err != nil {
					return err
				}
			default:
				expectResultSet, err := s.SQLExpectResultSet(stream.Context(), sqlQuery)
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
					err = s.execQuery(stream, hadb, ex, sqlQuery, args)
					if err != nil {
						return err
					}
				} else {
					err = s.execUpdate(stream, hadb, ex, sqlQuery, args)
					if err != nil {
						return err
					}
				}

			}
		}
	}
}

func (s *Service) execQuery(stream grpc.BidiStreamingServer[sqlv1.QueryRequest, sqlv1.QueryResponse], hadb HADB, ex execQuerier, sqlQuery string, args []any) error {
	res, err := ex.ExecContext(stream.Context(), sqlQuery, args...)
	if err != nil {
		return stream.Send(&sqlv1.QueryResponse{
			Error: err.Error(),
		})
	}
	lastInsertID, err := res.LastInsertId()
	if err != nil {
		return stream.Send(&sqlv1.QueryResponse{
			Error: err.Error(),
		})
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return stream.Send(&sqlv1.QueryResponse{
			Error: err.Error(),
		})
	}

	return stream.Send(&sqlv1.QueryResponse{
		LastInsertId: lastInsertID,
		RowsAffected: rowsAffected,
		Txseq:        hadb.PubSeq(),
	})
}

func (s *Service) execUpdate(stream grpc.BidiStreamingServer[sqlv1.QueryRequest, sqlv1.QueryResponse], hadb HADB, ex execQuerier, sqlQuery string, args []any) error {
	resp := query(stream.Context(), ex, sqlQuery, args...)
	resp.Txseq = hadb.PubSeq()
	return stream.Send(resp)
}

func (s *Service) DataSourceNames(ctx context.Context, req *sqlv1.DataSourceNamesRequest) (*sqlv1.DataSourceNamesResponse, error) {
	return &sqlv1.DataSourceNamesResponse{
		Dsn: s.DSNList(),
	}, nil
}

func (s *Service) LatestSnapshot(ctx context.Context, req *sqlv1.LatestSnapshotRequest) (*sqlv1.LatestSnapshotResponse, error) {
	db, ok := s.DBProvider(req.ReplicationId)
	if !ok {
		return nil, fmt.Errorf("database with replication id %q not found", req.ReplicationId)
	}
	sequence, reader, err := db.LatestSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return &sqlv1.LatestSnapshotResponse{
		Sequence: sequence,
		Data:     data,
	}, nil

}

func (s *Service) ReplicationIDs(ctx context.Context, req *sqlv1.ReplicationIDsRequest) (*sqlv1.ReplicationIDsResponse, error) {
	return &sqlv1.ReplicationIDsResponse{
		ReplicationId: s.ReplicationIDList(),
	}, nil
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

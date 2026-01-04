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
	DBProvider DBProvider
}

type HADB interface {
	PubSeq() uint64
	DB() *sql.DB
}

type DBProvider func(id string) (HADB, bool)

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

			if req.Type == sqlv1.QueryType_QUERY_TYPE_UNSPECIFIED {
				resp := query(stream.Context(), ex, sqlQuery, args...)
				resp.Txseq = hadb.PubSeq()
				err := stream.Send(resp)
				if err != nil {
					return err
				}
				continue
			} else {
				res, err := ex.ExecContext(stream.Context(), sqlQuery, args...)
				if err != nil {
					err := stream.Send(&sqlv1.QueryResponse{
						Error: err.Error(),
					})
					if err != nil {
						return err
					}
					continue
				}
				lastInsertID, err := res.LastInsertId()
				if err != nil {
					err := stream.Send(&sqlv1.QueryResponse{
						Error: err.Error(),
					})
					if err != nil {
						return err
					}
					continue
				}

				rowsAffected, err := res.RowsAffected()
				if err != nil {
					err := stream.Send(&sqlv1.QueryResponse{
						Error: err.Error(),
					})
					if err != nil {
						return err
					}
					continue
				}

				err = stream.Send(&sqlv1.QueryResponse{
					LastInsertId: lastInsertID,
					RowsAffected: rowsAffected,
					Txseq:        hadb.PubSeq(),
				})
				if err != nil {
					return err
				}
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

package ha

import (
	"context"
	"database/sql/driver"

	"github.com/mattn/go-sqlite3"
)

type Conn struct {
	*sqlite3.SQLiteConn
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmt, errParse := Parse(context.Background(), query)
	if errParse != nil {
		return nil, errParse
	}
	if stmt.DDL() {
		anyArgs := make([]any, len(args))
		for i, arg := range args {
			anyArgs[i] = arg
		}
		if err := addSQLChange(c.SQLiteConn, query, anyArgs); err != nil {
			return nil, err
		}
	}
	res, err := c.SQLiteConn.ExecContext(ctx, query, args)
	if err != nil && stmt.DDL() {
		removeLastChange(c.SQLiteConn)
	}
	return res, err
}

func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	stmt, errParse := Parse(context.Background(), query)
	if errParse != nil {
		return nil, errParse
	}
	if stmt.DDL() {
		anyArgs := make([]any, len(args))
		for i, arg := range args {
			anyArgs[i] = arg
		}
		if err := addSQLChange(c.SQLiteConn, query, anyArgs); err != nil {
			return nil, err
		}
	}
	res, err := c.SQLiteConn.Exec(query, args)
	if err != nil && stmt.DDL() {
		removeLastChange(c.SQLiteConn)
	}
	return res, err
}

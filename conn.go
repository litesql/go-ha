package ha

import (
	"context"
	"database/sql/driver"
	"strings"

	"github.com/litesql/go-sqlite3"
)

type Conn struct {
	*sqlite3.SQLiteConn
	disableDDLSync bool
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmts, errParse := Parse(context.Background(), query)
	if errParse != nil {
		return nil, errParse
	}
	var ddlCommands strings.Builder
	if !c.disableDDLSync {
		for _, stmt := range stmts {
			if stmt.DDL() {
				ddlCommands.WriteString(stmt.sourceWithIfExists())
			}
		}
	}
	if ddlCommands.Len() > 0 {
		if err := addSQLChange(c.SQLiteConn, ddlCommands.String(), nil); err != nil {
			return nil, err
		}
	}
	res, err := c.SQLiteConn.ExecContext(ctx, query, args)
	if err != nil && ddlCommands.Len() > 0 {
		removeLastChange(c.SQLiteConn)
	}
	return res, err
}

func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	stmts, errParse := Parse(context.Background(), query)
	if errParse != nil {
		return nil, errParse
	}
	var ddlCommands strings.Builder
	if !c.disableDDLSync {
		for _, stmt := range stmts {
			if stmt.DDL() {
				ddlCommands.WriteString(stmt.sourceWithIfExists())
			}
		}
	}
	if ddlCommands.Len() > 0 {
		if err := addSQLChange(c.SQLiteConn, ddlCommands.String(), nil); err != nil {
			return nil, err
		}
	}
	res, err := c.SQLiteConn.Exec(query, args)
	if err != nil && ddlCommands.Len() > 0 {
		removeLastChange(c.SQLiteConn)
	}
	return res, err
}

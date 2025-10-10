package ha

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/litesql/go-ha/parser"
)

var (
	ErrInvalidSQL = fmt.Errorf("invalid SQL")
	cache, _      = lru.New[string, []*Statement](256)
)

const (
	TypeExplain            = "EXPLAIN"
	TypeSelect             = "SELECT"
	TypeInsert             = "INSERT"
	TypeUpdate             = "UPDATE"
	TypeDelete             = "DELETE"
	TypeCreateTable        = "CREATE TABLE"
	TypeCreateIndex        = "CREATE INDEX"
	TypeCreateView         = "CREATE VIEW"
	TypeCreateTrigger      = "CREATE TRIGGER"
	TypeCreateVirtualTable = "CREATE VIRTUAL TABLE"
	TypeAlterTable         = "ALTER TABLE"
	TypeVacuum             = "VACUUM"
	TypeDrop               = "DROP"
	TypeAnalyze            = "ANALYZE"
	TypeBegin              = "BEGIN"
	TypeCommit             = "COMMIT"
	TypeRollback           = "ROLLBACK"
	TypeSavepoint          = "SAVEPOINT"
	TypeRelease            = "RELEASE"
	TypeOther              = "OTHER"
)

type Statement struct {
	source       string
	hasDistinct  bool
	hasReturning bool
	typ          string
	parameters   []string
	columns      []string
	ddl          bool
	hasIfExists  bool
	hasModifier  bool
}

func ParseStatement(ctx context.Context, sql string) (*Statement, error) {
	// PgWire Begin statement has a different syntax
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "BEGIN") {
		return &Statement{
			source: sql,
			typ:    TypeBegin,
		}, nil
	}

	statements, ok := cache.Get(sql)
	if !ok {
		var err error
		statements, err = parse(ctx, sql)
		if err != nil {
			return nil, err
		}
		cache.Add(sql, statements)
	}
	if len(statements) > 1 {
		return nil, fmt.Errorf("multiple SQL statements are not allowed: %w", ErrInvalidSQL)
	}
	stmt := statements[0]
	stmt.source = sql
	return stmt, nil
}

func Parse(ctx context.Context, sql string) ([]*Statement, error) {
	if stmts, ok := cache.Get(sql); ok {
		return stmts, nil
	}
	statements, err := parse(ctx, sql)
	if err != nil {
		return nil, err
	}
	cache.Add(sql, statements)
	return statements, nil
}

func (s *Statement) Source() string {
	return s.source
}

func (s *Statement) Type() string {
	return s.typ
}

func (s *Statement) HasDistinct() bool {
	return s.hasDistinct
}

func (s *Statement) Parameters() []string {
	return s.parameters
}

func (s *Statement) Columns() []string {
	return s.columns
}

func (s *Statement) IsExplain() bool {
	return s.typ == TypeExplain
}

func (s *Statement) HasReturning() bool {
	return s.hasReturning
}

func (s *Statement) IsSelect() bool {
	return s.typ == TypeSelect
}

func (s *Statement) IsInsert() bool {
	return s.typ == TypeInsert
}

func (s *Statement) IsUpdate() bool {
	return s.typ == TypeUpdate
}

func (s *Statement) IsDelete() bool {
	return s.typ == TypeDelete
}

func (s *Statement) IsCreateTable() bool {
	return s.typ == TypeCreateTable
}

func (s *Statement) DDL() bool {
	return s.ddl
}

func (s *Statement) Begin() bool {
	return s.typ == TypeBegin
}

func (s *Statement) Commit() bool {
	return s.typ == TypeCommit
}

func (s *Statement) Rollback() bool {
	return s.typ == TypeRollback
}

func (s *Statement) sourceWithIfExists() string {
	if s.hasIfExists || !s.ddl || s.typ == TypeAlterTable {
		return s.source
	}
	var ifExistsExpression string
	if s.typ == TypeDrop {
		ifExistsExpression = "IF EXISTS"
	} else {
		ifExistsExpression = "IF NOT EXISTS"
	}
	if s.hasModifier {
		parts := strings.SplitN(s.source, " ", 4)
		parts = append(parts[0:3], ifExistsExpression, parts[3])
		return strings.Join(parts, " ")
	}
	parts := strings.SplitN(s.source, " ", 3)
	parts = append(parts[0:2], ifExistsExpression, parts[2])
	return strings.Join(parts, " ")
}

func parse(ctx context.Context, source string) ([]*Statement, error) {
	slog.DebugContext(ctx, "Parse", "sql", source)
	input := antlr.NewInputStream(source)
	lexer := parser.NewSQLiteLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := parser.NewSQLiteParser(stream)
	p.RemoveErrorListeners()
	listener := sqlListener{
		input: input,
	}
	p.AddParseListener(&listener)
	var errorListener errorListener
	p.AddErrorListener(&errorListener)
	p.Parse()
	if errorListener.err != nil {
		slog.ErrorContext(ctx, "Parse error", "error", errorListener.err, "sql", source)
		return nil, errorListener.err
	}

	return listener.statements, nil
}

type errorListener struct {
	antlr.DefaultErrorListener
	err error
}

func (d *errorListener) SyntaxError(_ antlr.Recognizer, _ any, line, column int, msg string, _ antlr.RecognitionException) {
	if msg != "" {
		d.err = fmt.Errorf("%d:%d: %s: %w", line, column, msg, ErrInvalidSQL)
	}
}

type sqlListener struct {
	parser.BaseSQLiteParserListener
	input       *antlr.InputStream
	statements  []*Statement
	current     int
	sourceIndex int
}

func (s *sqlListener) ExitExpr(ctx *parser.ExprContext) {
	if ctx.BIND_PARAMETER() != nil {
		if !slices.Contains(s.statement().parameters, ctx.GetText()) || ctx.GetText() == "?" {
			s.statement().parameters = append(s.statement().parameters, ctx.GetText())
		}
	}
}

func (s *sqlListener) ExitSelect_stmt(c *parser.Select_stmtContext) {
	if c.AllSelect_core() != nil && len(c.AllSelect_core()) > 0 {
		mainSelect := c.AllSelect_core()[0]
		if mainSelect.DISTINCT_() != nil {
			s.statement().hasDistinct = true
		} else {
			s.statement().hasDistinct = false
		}

		s.statement().columns = make([]string, 0)
		for _, col := range mainSelect.AllResult_column() {
			if col.Column_alias() != nil {
				s.statement().columns = append(s.statement().columns, col.Column_alias().GetText())
				continue
			}
			s.statement().columns = append(s.statement().columns, col.GetText())
		}
	}
}

func (s *sqlListener) EnterSql_stmt(c *parser.Sql_stmtContext) {
	s.statements = append(s.statements, &Statement{})
}

func (s *sqlListener) statement() *Statement {
	return s.statements[s.current]
}

func (s *sqlListener) ExitSql_stmt(c *parser.Sql_stmtContext) {
	defer func() {
		s.current++
	}()

	s.statement().source = s.input.GetText(s.sourceIndex, s.input.Index())
	s.sourceIndex = s.input.Index() + 1

	if c.EXPLAIN_() != nil {
		s.statement().typ = TypeExplain
		s.statement().columns = []string{"id", "parent", "notused", "detail"}
		return
	}
	switch {
	case c.Select_stmt() != nil:
		s.statement().typ = TypeSelect
	case c.Insert_stmt() != nil:
		s.statement().typ = TypeInsert
		s.statement().hasReturning = c.Insert_stmt().Returning_clause() != nil
	case c.Update_stmt() != nil:
		s.statement().typ = TypeUpdate
		s.statement().hasReturning = c.Update_stmt().Returning_clause() != nil
	case c.Delete_stmt() != nil:
		s.statement().typ = TypeDelete
		s.statement().hasReturning = c.Delete_stmt().Returning_clause() != nil
	case c.Begin_stmt() != nil:
		s.statement().typ = TypeBegin
	case c.Commit_stmt() != nil:
		s.statement().typ = TypeCommit
	case c.Rollback_stmt() != nil:
		s.statement().typ = TypeRollback
	case c.Create_table_stmt() != nil:
		s.statement().typ = TypeCreateTable
	case c.Create_index_stmt() != nil:
		s.statement().typ = TypeCreateIndex
	case c.Create_trigger_stmt() != nil:
		s.statement().typ = TypeCreateTrigger
	case c.Create_view_stmt() != nil:
		s.statement().typ = TypeCreateView
	case c.Create_virtual_table_stmt() != nil:
		s.statement().typ = TypeCreateVirtualTable
	case c.Alter_table_stmt() != nil:
		s.statement().typ = TypeAlterTable
	case c.Vacuum_stmt() != nil:
		s.statement().typ = TypeVacuum
	case c.Drop_stmt() != nil:
		s.statement().typ = TypeDrop
	case c.Analyze_stmt() != nil:
		s.statement().typ = TypeAnalyze
	case c.Savepoint_stmt() != nil:
		s.statement().typ = TypeSavepoint
	case c.Release_stmt() != nil:
		s.statement().typ = TypeRelease
	default:
		s.statement().typ = TypeOther
	}
}

func (s *sqlListener) ExitReturning_clause(ctx *parser.Returning_clauseContext) {
	if len(ctx.AllResult_column()) > 0 {
		s.statement().columns = make([]string, 0)
	}
	for _, col := range ctx.AllResult_column() {
		if col.Column_alias() != nil {
			s.statement().columns = append(s.statement().columns, col.Column_alias().GetText())
			continue
		}
		s.statement().columns = append(s.statement().columns, col.GetText())
	}
}

func (s *sqlListener) ExitCreate_table_stmt(ctx *parser.Create_table_stmtContext) {
	s.statement().hasIfExists = ctx.IF_() != nil && ctx.NOT_() != nil && ctx.EXISTS_() != nil
	s.statement().ddl = true
}

func (s *sqlListener) ExitCreate_index_stmt(ctx *parser.Create_index_stmtContext) {
	s.statement().hasIfExists = ctx.IF_() != nil && ctx.NOT_() != nil && ctx.EXISTS_() != nil
	s.statement().hasModifier = ctx.UNIQUE_() != nil
	s.statement().ddl = true
}

func (s *sqlListener) ExitCreate_trigger_stmt(ctx *parser.Create_trigger_stmtContext) {
	s.statement().hasIfExists = ctx.IF_() != nil && ctx.NOT_() != nil && ctx.EXISTS_() != nil
	s.statement().hasModifier = ctx.TEMPORARY_() != nil || ctx.TEMP_() != nil
	s.statement().ddl = true
}

func (s *sqlListener) ExitCreate_view_stmt(ctx *parser.Create_view_stmtContext) {
	s.statement().hasIfExists = ctx.IF_() != nil && ctx.NOT_() != nil && ctx.EXISTS_() != nil
	s.statement().hasModifier = ctx.TEMPORARY_() != nil || ctx.TEMP_() != nil
	s.statement().ddl = true
}

func (s *sqlListener) ExitCreate_virtual_table_stmt(ctx *parser.Create_virtual_table_stmtContext) {
	s.statement().hasIfExists = ctx.IF_() != nil && ctx.NOT_() != nil && ctx.EXISTS_() != nil
	s.statement().hasModifier = true
	s.statement().ddl = true
}

func (s *sqlListener) ExitAlter_table_stmt(ctx *parser.Alter_table_stmtContext) {
	s.statement().ddl = true
}

func (s *sqlListener) ExitDrop_stmt(ctx *parser.Drop_stmtContext) {
	s.statement().hasIfExists = ctx.IF_() != nil && ctx.EXISTS_() != nil
	s.statement().ddl = true
}

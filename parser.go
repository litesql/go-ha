package ha

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/rqlite/sql"
)

var (
	ErrInvalidSQL            = fmt.Errorf("invalid SQL")
	cache, _                 = lru.New[string, []*Statement](256)
	supportedProjectionFuncs = []string{"COUNT", "SUM", "TOTAL", "MAX", "MIN"}
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
	TypePragma             = "PRAGMA"
	TypeOther              = "OTHER"
)

type Statement struct {
	source              string
	hasDistinct         bool
	hasReturning        bool
	typ                 string
	parameters          []string
	columns             []string
	orderBy             []string
	limit               int
	ddl                 bool
	hasIfExists         bool
	hasModifier         bool
	modifiesDatabase    bool
	selectDepth         int
	projectionFunctions map[int]string
}

func UnverifiedStatement(source string, hasDistinct bool,
	hasReturning bool, typ string, parameters []string, columns []string,
	ddl bool, hasIfExists bool, hasModifier bool,
	modifiesDatabase bool) *Statement {
	return &Statement{
		source:           source,
		hasDistinct:      hasDistinct,
		hasReturning:     hasReturning,
		typ:              typ,
		parameters:       parameters,
		columns:          columns,
		ddl:              ddl,
		hasIfExists:      hasIfExists,
		hasModifier:      hasModifier,
		modifiesDatabase: modifiesDatabase,
	}
}

func ParseStatement(ctx context.Context, source string) (*Statement, error) {
	// PgWire Begin statement has a different syntax
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(source)), "BEGIN") {
		return &Statement{
			source:           source,
			typ:              TypeBegin,
			modifiesDatabase: true,
		}, nil
	}
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(source)), "ATTACH ") {
		return &Statement{
			source:           source,
			typ:              TypeOther,
			modifiesDatabase: false,
		}, nil
	}
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(source)), "DETACH ") {
		return &Statement{
			source:           source,
			typ:              TypeOther,
			modifiesDatabase: false,
		}, nil
	}
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(source)), "VACUUM") {
		return &Statement{
			source:           source,
			typ:              TypeOther,
			modifiesDatabase: true,
		}, nil
	}

	statements, ok := cache.Get(source)
	if !ok {
		var err error
		statements, err = parse(ctx, source)
		if err != nil {
			return nil, err
		}
		cache.Add(source, statements)
	}
	if len(statements) > 1 {
		return nil, fmt.Errorf("multiple SQL statements are not allowed: %w", ErrInvalidSQL)
	}
	result := statements[0]
	result.source = source
	return result, nil
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

func (s *Statement) Visit(n sql.Node) (w sql.Visitor, node sql.Node, err error) {
	switch n.(type) {
	case *sql.SelectStatement:
		if s.selectDepth == 0 {
			s.projectionFunctions = make(map[int]string)
			s.limit = -1
		}
		s.selectDepth++
	}
	return s, n, nil
}

func (s *Statement) VisitEnd(n sql.Node) (sql.Node, error) {
	switch n := n.(type) {
	case *sql.SelectStatement:
		s.selectDepth--
		if s.selectDepth == 0 {
			s.typ = TypeSelect
			s.hasDistinct = n.Distinct.IsValid()
			s.columns = resultColumnsToString(n.Columns)
			s.projectionFunctions = make(map[int]string)
			for i, col := range n.Columns {
				if col.Expr == nil {
					continue
				}
				text := col.Expr.String()
				index := strings.LastIndex(text, "(")
				if index > 0 {
					fn := strings.ToUpper(text[0:index])
					if slices.Contains(supportedProjectionFuncs, fn) {
						s.projectionFunctions[i] = fn
					}
				}
			}
			s.orderBy = make([]string, 0)
			for _, orderTerm := range n.OrderingTerms {
				order := strings.ReplaceAll(orderTerm.X.String(), "\"", "")
				if orderTerm.Desc.IsValid() {
					order += " DESC"
				}
				s.orderBy = append(s.orderBy, order)
			}
			if n.Limit.IsValid() {
				limit, err := strconv.Atoi(n.LimitExpr.String())
				if err == nil {
					s.limit = limit
				}
			}
		}
	case *sql.BindExpr:
		s.parameters = append(s.parameters, n.Name)
	case *sql.ExplainStatement:
		s.columns = []string{"id", "parent", "notused", "detail"}
		s.modifiesDatabase = false
		s.typ = TypeExplain
	case *sql.InsertStatement:
		s.modifiesDatabase = true
		s.typ = TypeInsert
		if n.ReturningClause != nil {
			s.columns = resultColumnsToString(n.ReturningClause.Columns)
			s.hasReturning = true
		}
	case *sql.DeleteStatement:
		s.modifiesDatabase = true
		s.typ = TypeDelete
		if n.ReturningClause != nil {
			s.columns = resultColumnsToString(n.ReturningClause.Columns)
			s.hasReturning = true
		}
	case *sql.UpdateStatement:
		s.modifiesDatabase = true
		s.typ = TypeUpdate
		if n.ReturningClause != nil {
			s.columns = resultColumnsToString(n.ReturningClause.Columns)
			s.hasReturning = true
		}
	case *sql.BeginStatement:
		s.modifiesDatabase = true
		s.typ = TypeBegin
	case *sql.CommitStatement:
		s.modifiesDatabase = true
		s.typ = TypeCommit
	case *sql.RollbackStatement:
		s.modifiesDatabase = true
		s.typ = TypeRollback
	case *sql.CreateTableStatement:
		s.modifiesDatabase = true
		s.ddl = true
		s.hasIfExists = n.IfNotExists.IsValid()
		s.typ = TypeCreateTable
	case *sql.CreateIndexStatement:
		s.modifiesDatabase = true
		s.ddl = true
		s.hasIfExists = n.IfNotExists.IsValid()
		s.hasModifier = n.Unique.IsValid()
		s.typ = TypeCreateIndex
	case *sql.CreateTriggerStatement:
		s.modifiesDatabase = true
		s.ddl = true
		s.hasModifier = n.Temp.IsValid()
		s.hasIfExists = n.IfNotExists.IsValid()
		s.typ = TypeCreateTrigger
	case *sql.CreateViewStatement:
		s.modifiesDatabase = true
		s.ddl = true
		//FIXME add TEMPORARY keyword to the parser
		s.hasIfExists = n.IfNotExists.IsValid()
		s.typ = TypeCreateView
	case *sql.CreateVirtualTableStatement:
		s.modifiesDatabase = true
		s.hasIfExists = n.IfNotExists.IsValid()
		s.hasModifier = true
		s.typ = TypeCreateVirtualTable
	case *sql.AlterTableStatement:
		s.modifiesDatabase = true
		s.ddl = true
		s.typ = TypeAlterTable
	case *sql.AnalyzeStatement:
		s.modifiesDatabase = true
		s.typ = TypeAnalyze
	case *sql.DropTableStatement:
		s.modifiesDatabase = true
		s.typ = TypeDrop
	case *sql.DropIndexStatement:
		s.modifiesDatabase = true
		s.typ = TypeDrop
	case *sql.DropTriggerStatement:
		s.modifiesDatabase = true
		s.typ = TypeDrop
	case *sql.DropViewStatement:
		s.modifiesDatabase = true
		s.typ = TypeDrop
	case *sql.PragmaStatement:
		s.modifiesDatabase = false
		if n.Expr != nil {
			if strings.Contains(n.Expr.String(), "=") || strings.Contains(n.Expr.String(), "(") {
				s.modifiesDatabase = true
			}
		}
		s.typ = TypePragma
	case *sql.SavepointStatement:
		s.modifiesDatabase = true
		s.typ = TypeSavepoint
	case *sql.ReleaseStatement:
		s.modifiesDatabase = true
		s.typ = TypeRelease
	default:
		s.typ = TypeOther
	}
	return n, nil
}

func resultColumnsToString(resultColumns []*sql.ResultColumn) []string {
	columns := make([]string, len(resultColumns))
	for i, col := range resultColumns {
		if col.Alias != nil {
			columns[i] = col.Alias.Name
			continue
		}
		if col.Star.IsValid() {
			columns[i] = "*"
			continue
		}
		columns[i] = strings.ReplaceAll(col.Expr.String(), `"`, "")
	}
	return columns
}

func (s *Statement) Source() string {
	return strings.TrimSuffix(s.source, ";") + ";"
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

func (s *Statement) OrderBy() []string {
	return s.orderBy
}

func (s *Statement) Limit() int {
	return s.limit
}

func (s *Statement) ProjectionFunctions() map[int]string {
	return s.projectionFunctions
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

func (s *Statement) SourceWithIfExists() string {
	if s.hasIfExists || !s.ddl || s.typ == TypeAlterTable {
		return s.Source()
	}
	var ifExistsExpression string
	if s.typ == TypeDrop {
		ifExistsExpression = "IF EXISTS"
	} else {
		ifExistsExpression = "IF NOT EXISTS"
	}
	if s.hasModifier {
		parts := strings.SplitN(s.Source(), " ", 4)
		parts = append(parts[0:3], ifExistsExpression, parts[3])
		return strings.Join(parts, " ")
	}
	parts := strings.SplitN(s.Source(), " ", 3)
	parts = append(parts[0:2], ifExistsExpression, parts[2])
	return strings.Join(parts, " ")
}

func (s *Statement) ModifiesDatabase() bool {
	return s.modifiesDatabase
}

func (s *Statement) equals(other *Statement) bool {
	if s == nil || other == nil {
		return s == other
	}

	if slices.Compare(s.columns, other.columns) != 0 {
		return false
	}

	if slices.Compare(s.parameters, other.parameters) != 0 {
		return false
	}

	if s.hasDistinct != other.hasDistinct {
		return false
	}

	return true
}

func parse(ctx context.Context, source string) ([]*Statement, error) {
	slog.DebugContext(ctx, "Parse", "sql", source)
	p := sql.NewParser(strings.NewReader(source))
	parsed, err := p.ParseStatements()
	if err != nil {
		return nil, err
	}
	var statements []*Statement
	for _, query := range parsed {
		stmt := &Statement{}
		node, err := sql.Walk(stmt, query)
		if err != nil {
			return nil, err
		}
		stmt.source = node.String()
		statements = append(statements, stmt)
	}
	return statements, nil
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

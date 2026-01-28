package ha

import (
	"context"
	"slices"
	"testing"
)

func TestStatement(t *testing.T) {
	tests := map[string]struct {
		sql       string
		hasError  bool
		statement *Statement
	}{
		"select": {
			sql:       "SELECT *, u.*, name as n  FROM `user`  u",
			hasError:  false,
			statement: &Statement{hasDistinct: false, columns: []string{"*", "u.*", "n"}},
		},
		"bind named parameter": {
			sql:       "SELECT * FROM user WHERE id = :id",
			hasError:  false,
			statement: &Statement{hasDistinct: false, columns: []string{"*"}, parameters: []string{":id"}},
		},
		"bind positional parameter": {
			sql:       "SELECT * FROM user WHERE id = ? OR id = ?",
			hasError:  false,
			statement: &Statement{hasDistinct: false, columns: []string{"*"}, parameters: []string{"?", "?"}},
		},
		"bind positional ? parameter": {
			sql:       "SELECT * FROM user WHERE id = ?1 OR id = ?2",
			hasError:  false,
			statement: &Statement{hasDistinct: false, columns: []string{"*"}, parameters: []string{"?1", "?2"}},
		},
		"explain": {
			sql:       "EXPLAIN QUERY PLAN SELECT name FROM user WHERE id IN (SELECT DISTINCT user_id FROM orders)",
			hasError:  false,
			statement: &Statement{hasDistinct: false, columns: []string{"id", "parent", "notused", "detail"}},
		},
		"select distinct": {
			sql:       "SELECT DISTINCT name as test FROM user",
			hasError:  false,
			statement: &Statement{hasDistinct: true, columns: []string{"test"}},
		},
		"select distinct on subquery": {
			sql:       "SELECT name FROM user WHERE id IN (SELECT DISTINCT user_id FROM orders)",
			hasError:  false,
			statement: &Statement{hasDistinct: false, columns: []string{"name"}},
		},
		"returning": {
			sql:       "UPDATE user SET name = 'WW' WHERE id = $1 RETURNING id, name",
			hasError:  false,
			statement: &Statement{hasDistinct: false, columns: []string{"id", "name"}, parameters: []string{"$1"}},
		},
		"malformed sql": {
			sql:      "SELEC FROM user",
			hasError: true,
		},
		"single statement": {
			sql:      "SELECT * FROM user where name = ';';",
			hasError: false,
		},
		"multiple statements": {
			sql:      "SELECT * FROM user where name = ';'; DELETE FROM user WHERE id = 1;",
			hasError: true,
		},

		"dbeaver metadata query": {
			sql:      "select sql from sqlite_schema where lower(name) = lower('Genre')",
			hasError: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			stmt, err := ParseStatement(context.TODO(), tc.sql)
			if tc.hasError && err == nil {
				t.Fatalf("expected error but got none")
			}
			if !tc.hasError && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.statement != nil && !tc.statement.equals(stmt) {
				t.Fatalf("expected statement to be %+v but got %+v", tc.statement, stmt)
			}
		})
	}
}

func TestStatementCommit(t *testing.T) {
	sql := "COMMIT"
	stmt, err := ParseStatement(context.TODO(), sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectStmt := &Statement{source: sql}
	if stmt == nil {
		t.Fatalf("expected statement to be not nil")
		return
	}
	if !stmt.equals(expectStmt) {
		t.Fatalf("expected statement to be %v but got %v", expectStmt, stmt)
	}
	if !stmt.Commit() {
		t.Fatalf("expected statement to be commit")
	}
	if stmt.Rollback() {
		t.Fatalf("expected statement to not be rollback")
	}
	if stmt.Begin() {
		t.Fatalf("expected statement to not be begin")
	}
	if stmt.IsInsert() {
		t.Fatalf("expected statement to not be insert")
	}
	if stmt.IsSelect() {
		t.Fatalf("expected statement to not be select")
	}
	if stmt.IsUpdate() {
		t.Fatalf("expected statement to not be update")
	}
	if stmt.IsDelete() {
		t.Fatalf("expected statement to not be delete")
	}
	if stmt.IsCreateTable() {
		t.Fatalf("expected statement to not be create table")
	}
	if stmt.IsExplain() {
		t.Fatalf("expected statement to not be explain")
	}
}

func TestStatementRollback(t *testing.T) {
	sql := "ROLLBACK"
	stmt, err := ParseStatement(context.TODO(), sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectStmt := &Statement{source: sql}
	if stmt == nil {
		t.Fatalf("expected statement to be not nil")
		return
	}
	if !stmt.equals(expectStmt) {
		t.Fatalf("expected statement to be %v but got %v", expectStmt, stmt)
	}
	if !stmt.Rollback() {
		t.Fatalf("expected statement to be rollback")
	}
	if stmt.Commit() {
		t.Fatalf("expected statement to not be commit")
	}
	if stmt.Begin() {
		t.Fatalf("expected statement to not be begin")
	}
	if stmt.IsInsert() {
		t.Fatalf("expected statement to not be insert")
	}
	if stmt.IsSelect() {
		t.Fatalf("expected statement to not be select")
	}
	if stmt.IsUpdate() {
		t.Fatalf("expected statement to not be update")
	}
	if stmt.IsDelete() {
		t.Fatalf("expected statement to not be delete")
	}
	if stmt.IsCreateTable() {
		t.Fatalf("expected statement to not be create table")
	}
	if stmt.IsExplain() {
		t.Fatalf("expected statement to not be explain")
	}
}

func TestStatementBegin(t *testing.T) {
	sql := "BEGIN"
	stmt, err := ParseStatement(context.TODO(), sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectStmt := &Statement{source: sql}
	if stmt == nil {
		t.Fatalf("expected statement to be not nil")
		return
	}
	if !stmt.equals(expectStmt) {
		t.Fatalf("expected statement to be %v but got %v", expectStmt, stmt)
	}
	if !stmt.Begin() {
		t.Fatalf("expected statement to be begin")
	}
	if stmt.Rollback() {
		t.Fatalf("expected statement to not be rollback")
	}
	if stmt.Commit() {
		t.Fatalf("expected statement to not be commit")
	}
	if stmt.IsInsert() {
		t.Fatalf("expected statement to not be insert")
	}
	if stmt.IsSelect() {
		t.Fatalf("expected statement to not be select")
	}
	if stmt.IsUpdate() {
		t.Fatalf("expected statement to not be update")
	}
	if stmt.IsDelete() {
		t.Fatalf("expected statement to not be delete")
	}
	if stmt.IsCreateTable() {
		t.Fatalf("expected statement to not be create table")
	}
	if stmt.IsExplain() {
		t.Fatalf("expected statement to not be explain")
	}
}

func TestStatementExplain(t *testing.T) {
	sql := "EXPLAIN QUERY PLAN SELECT * FROM user"
	stmt, err := ParseStatement(context.TODO(), sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectStmt := &Statement{source: sql, columns: []string{"id", "parent", "notused", "detail"}}
	if stmt == nil {
		t.Fatalf("expected statement to be not nil")
		return
	}
	if !stmt.equals(expectStmt) {
		t.Fatalf("expected statement to be %v but got %v", expectStmt, stmt)
	}
	if !stmt.IsExplain() {
		t.Fatalf("expected statement to be explain")
	}
	if stmt.IsSelect() {
		t.Fatalf("expected statement to not be select")
	}
	if stmt.Begin() {
		t.Fatalf("expected statement to not be begin")
	}
	if stmt.Commit() {
		t.Fatalf("expected statement to not be commit")
	}
	if stmt.Rollback() {
		t.Fatalf("expected statement to not be rollback")
	}
	if stmt.IsInsert() {
		t.Fatalf("expected statement to not be insert")
	}
	if stmt.IsUpdate() {
		t.Fatalf("expected statement to not be update")
	}
	if stmt.IsDelete() {
		t.Fatalf("expected statement to not be delete")
	}
	if stmt.IsCreateTable() {
		t.Fatalf("expected statement to not be create table")
	}
}

func TestStatementModifiesDatabase(t *testing.T) {
	tests := map[string]struct {
		sql              string
		modifiesDatabase bool
	}{
		"select": {
			sql:              "SELECT * FROM user",
			modifiesDatabase: false,
		},
		"insert": {
			sql:              "INSERT INTO user (id, name) VALUES (1, 'WW')",
			modifiesDatabase: true,
		},
		"update": {
			sql:              "UPDATE user SET name = 'WW' WHERE id = 1",
			modifiesDatabase: true,
		},
		"delete": {
			sql:              "DELETE FROM user WHERE id = 1",
			modifiesDatabase: true,
		},
		"create table": {
			sql:              "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)",
			modifiesDatabase: true,
		},
		"begin": {
			sql:              "BEGIN",
			modifiesDatabase: true,
		},
		"commit": {
			sql:              "COMMIT",
			modifiesDatabase: true,
		},
		"rollback": {
			sql:              "ROLLBACK",
			modifiesDatabase: true,
		},
		"pragma": {
			sql:              "PRAGMA foreign_keys = 'ON'",
			modifiesDatabase: true,
		},
		"vacuum": {
			sql:              "VACUUM",
			modifiesDatabase: false,
		},
		"explain": {
			sql:              "EXPLAIN QUERY PLAN SELECT * FROM user",
			modifiesDatabase: false,
		},
		"cte": {
			sql:              "WITH cte AS (SELECT * FROM user) SELECT * FROM cte",
			modifiesDatabase: false,
		},
		"cte insert": {
			sql:              "WITH cte AS (SELECT * FROM user) INSERT INTO cte (id, name) VALUES (1, 'WW')",
			modifiesDatabase: true,
		},
		"alter table": {
			sql:              "ALTER TABLE user ADD COLUMN age INTEGER",
			modifiesDatabase: true,
		},
		"analyze": {
			sql:              "ANALYZE",
			modifiesDatabase: true,
		},
		"attach": {
			sql:              "ATTACH DATABASE 'test.db' AS testdb",
			modifiesDatabase: false,
		},
		"detach": {
			sql:              "DETACH DATABASE testdb",
			modifiesDatabase: false,
		},
		"create index": {
			sql:              "CREATE INDEX idx_name ON user(name)",
			modifiesDatabase: true,
		},
		"drop index": {
			sql:              "DROP INDEX idx_name",
			modifiesDatabase: true,
		},
		"create view": {
			sql:              "CREATE VIEW v_user AS SELECT * FROM user",
			modifiesDatabase: true,
		},
		"drop view": {
			sql:              "DROP VIEW v_user",
			modifiesDatabase: true,
		},
		"create trigger": {
			sql:              "CREATE TRIGGER trg_user AFTER INSERT ON user BEGIN UPDATE user SET name = 'triggered' WHERE id = NEW.id; END;",
			modifiesDatabase: true,
		},
		"drop trigger": {
			sql:              "DROP TRIGGER trg_user",
			modifiesDatabase: true,
		},
		"savepoint": {
			sql:              "SAVEPOINT sp1",
			modifiesDatabase: true,
		},
		"release savepoint": {
			sql:              "RELEASE sp1",
			modifiesDatabase: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			stmt, err := ParseStatement(context.TODO(), tc.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if stmt == nil {
				t.Fatalf("expected statement to be not nil")
				return
			}
			if stmt.ModifiesDatabase() != tc.modifiesDatabase {
				t.Fatalf("expected modifiesDatabase to be %v but got %v", tc.modifiesDatabase, stmt.ModifiesDatabase())
			}
		})
	}
}

func TestReturning(t *testing.T) {
	tests := map[string]struct {
		sql     string
		columns []string
	}{
		"all": {
			sql:     "INSERT INTO user(name) VALUES('name') RETURNING *",
			columns: []string{"*"},
		},
		"simple": {
			sql:     "INSERT INTO user(name) VALUES('name') RETURNING id",
			columns: []string{"id"},
		},
		"multiple": {
			sql:     "INSERT INTO user(name) VALUES('name') RETURNING id, created_at",
			columns: []string{"id", "created_at"},
		},
		"simple alias": {
			sql:     "INSERT INTO user(name) VALUES('name') RETURNING id identify",
			columns: []string{"identify"},
		},
		"simple mix alias": {
			sql:     "INSERT INTO user(name) VALUES('name') RETURNING id as identify, created_at",
			columns: []string{"identify", "created_at"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			stmt, err := ParseStatement(context.TODO(), tc.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if stmt == nil {
				t.Fatalf("expected statement to be not nil")
				return
			}
			if !slices.Equal(stmt.Columns(), tc.columns) {
				t.Fatalf("expected columns to be %v but got %v", tc.columns, stmt.Columns())
			}
		})
	}
}

func TestOrderBy(t *testing.T) {
	tests := map[string]struct {
		sql     string
		orderBy []string
	}{
		"zero": {
			sql:     "SELECT * FROM user",
			orderBy: []string{},
		},
		"not main select": {
			sql:     "SELECT u.* FROM user u WHERE u.ID IN (SELECT id FROM other_users ORDER BY name, id LIMIT 1)",
			orderBy: []string{},
		},
		"main select too": {
			sql:     "SELECT u.* FROM user u WHERE u.ID IN (SELECT id FROM other_users ORDER BY name, id LIMIT 1) ORDER BY id",
			orderBy: []string{"id"},
		},
		"one": {
			sql:     "SELECT * FROM user ORDER BY 1",
			orderBy: []string{"1"},
		},
		"simple": {
			sql:     "SELECT * FROM user ORDER BY name",
			orderBy: []string{"name"},
		},
		"multiple": {
			sql:     "SELECT * FROM user ORDER BY name, id",
			orderBy: []string{"name", "id"},
		},
		"simple desc": {
			sql:     "SELECT count(tet), name, sum(text) FROM user ORDER BY name, id desc",
			orderBy: []string{"name", "id DESC"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			stmt, err := ParseStatement(context.TODO(), tc.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if stmt == nil {
				t.Fatalf("expected statement to be not nil")
				return
			}
			if !slices.Equal(stmt.OrderBy(), tc.orderBy) {
				t.Fatalf("expected orderBy to be %v but got %v", tc.orderBy, stmt.OrderBy())
			}
		})
	}
}

func TestAggregateFunctions(t *testing.T) {
	tests := map[string]struct {
		sql                string
		aggregateFunctions map[int]string
	}{
		"zero": {
			sql:                "SELECT * FROM user",
			aggregateFunctions: map[int]string{},
		},
		"simple": {
			sql: "SELECT count(*) FROM user",
			aggregateFunctions: map[int]string{
				0: "COUNT",
			},
		},
		"multiple": {
			sql: "SELECT id, count(*), sum(salary) FROM user",
			aggregateFunctions: map[int]string{
				1: "COUNT",
				2: "SUM",
			},
		},
		"subquery": {
			sql:                "SELECT id FROM user WHERE salary = (SELECT MAX(salary) FROM user) LIMIT 6 OFFSET 2",
			aggregateFunctions: map[int]string{},
		},
		"avg": {
			sql:                "SELECT avg(salary) FROM employee",
			aggregateFunctions: map[int]string{1: "AVG"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			stmt, err := ParseStatement(context.TODO(), tc.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if stmt == nil {
				t.Fatalf("expected statement to be not nil")
				return
			}
			stmt.RewriteQueryToAggregate()
			if len(stmt.AggregateFunctions()) != len(tc.aggregateFunctions) {
				t.Fatalf("expected aggregate functions to be %v but got %v", tc.aggregateFunctions, stmt.AggregateFunctions())
			}
		})
	}
}

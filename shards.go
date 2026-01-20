package ha

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	sqlparser "github.com/rqlite/sql"
)

type driverConnFunc func(*sql.Conn) (driver.QueryerContext, error)

func CrossShardQuery(ctx context.Context, stmt *Statement, args []driver.NamedValue, queryRouter *regexp.Regexp, driverConn driverConnFunc) (driver.Rows, error) {
	slog.Debug("Executing cross-shard query", "query", stmt.Source(), "queryRouter", queryRouter.String())
	dbs := make([]*sql.DB, 0)
	for _, dsn := range ListDSN() {
		if !queryRouter.MatchString(dsn) {
			continue
		}
		connector, _ := LookupConnector(dsn)
		if connector == nil {
			continue
		}
		dbs = append(dbs, connector.DB())
	}

	if len(dbs) == 0 {
		return nil, fmt.Errorf("no databases available to execute query based on queryRouter=%s", queryRouter.String())
	}
	rewrittenQuery, aggregateFunctions, newTempDerivedColumns, err := stmt.RewriteQueryToAggregate()
	if err != nil {
		return nil, fmt.Errorf("failed to rewrite query: %w", err)
	}
	//TODO create a worker pool
	chErr := make(chan error, len(dbs))
	chRows := make(chan driver.Rows, len(dbs))
	var errs error
	go func() {
		var wg sync.WaitGroup
		for _, db := range dbs {
			wg.Go(func() {
				rows, err := queryDB(ctx, db, rewrittenQuery, args, driverConn)
				if err != nil {
					chErr <- err
					return
				}
				chRows <- rows
			})
		}
		wg.Wait()
		close(chErr)
		close(chRows)
	}()

	var wg sync.WaitGroup
	wg.Go(func() {
		for err := range chErr {
			if !errors.Is(err, sql.ErrNoRows) {
				errs = errors.Join(errs, err)
			}
		}
	})

	if (!stmt.HasDistinct() && len(stmt.OrderBy()) == 0 && len(aggregateFunctions) == 0) || len(dbs) < 2 {
		chValues := make(chan []driver.Value)
		result := newStreamResults(chValues)
		var nextErrs error
		chFirstResponse := make(chan struct{})
		var hasValues bool
		go func() {
			defer close(chValues)
			for rows := range chRows {
				size := len(rows.Columns())
				for {
					values := make([]driver.Value, size)
					err := rows.Next(values)
					if err != nil {
						if err == io.EOF {
							break
						}
						nextErrs = errors.Join(nextErrs, err)
						break
					}
					if !hasValues {
						hasValues = true
						result.setColumns(rows.Columns())
						close(chFirstResponse)
					}
					chValues <- values
				}
				rows.Close()
			}
			if !hasValues {
				close(chFirstResponse)
			}
		}()
		<-chFirstResponse
		if !hasValues {
			wg.Wait()
			errs = errors.Join(errs, nextErrs)
			if errs != nil {
				return nil, errs
			}
			return nil, sql.ErrNoRows
		}
		return result, nil
	}
	result := newBufferedResults(stmt.HasDistinct(), newTempDerivedColumns)
	var nextErrs error
	var shardsResultsCount int
	for rows := range chRows {
		result.setColumns(rows.Columns())
		size := len(rows.Columns())
		for {
			row := make([]driver.Value, size)
			err := rows.Next(row)
			if err != nil {
				if err == io.EOF {
					break
				}
				nextErrs = errors.Join(nextErrs, err)
				break
			}
			result.append(row)
			shardsResultsCount++
		}
		rows.Close()
	}
	wg.Wait()
	errs = errors.Join(errs, nextErrs)
	if result.empty() && errs != nil {
		return nil, errs
	}

	if err := result.mergeAndSort(shardsResultsCount, aggregateFunctions, stmt.OrderBy(), stmt.Limit()); err != nil {
		return nil, err
	}

	return result, nil
}

func queryDB(ctx context.Context, db *sql.DB, query string, args []driver.NamedValue, driverConn driverConnFunc) (driver.Rows, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	sqlConn, err := driverConn(conn)
	if err != nil {
		return nil, err
	}
	return sqlConn.QueryContext(ctx, query, args)
}

func newStreamResults(results chan []driver.Value) *streamResults {
	return &streamResults{
		results: results,
	}
}

type streamResults struct {
	columns    []string
	results    chan []driver.Value
	hasResults bool
}

func (r *streamResults) setColumns(columns []string) {
	r.columns = columns
}

func (r *streamResults) Columns() []string {
	return r.columns
}

func (r *streamResults) Close() error {
	return nil
}

func (r *streamResults) Next(dest []driver.Value) error {
	src, ok := <-r.results
	if !ok {
		return io.EOF
	}
	r.hasResults = true
	copy(dest, src)
	return nil
}

type bufferedResults struct {
	distinct    bool
	columns     []string
	values      [][]driver.Value
	index       int
	tempColumns map[int]int
}

func newBufferedResults(distinct bool, tempColumns map[int]int) *bufferedResults {
	return &bufferedResults{
		distinct:    distinct,
		tempColumns: tempColumns,
	}
}

func (r *bufferedResults) setColumns(columns []string) {
	r.columns = columns
}

func (r *bufferedResults) Columns() []string {
	return r.columns
}

func (r *bufferedResults) Close() error {
	r.values = nil
	return nil
}

func (r *bufferedResults) Next(dest []driver.Value) error {
	if r.values == nil || r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func (r *bufferedResults) append(row []driver.Value) {
	if !r.distinct {
		r.values = append(r.values, row)
		return
	}
	var exists bool
	for _, val := range r.values {
		if slices.Equal(val, row) {
			exists = true
			break
		}
	}
	if !exists {
		r.values = append(r.values, row)
	}
}

func (r *bufferedResults) empty() bool {
	return len(r.values) == 0
}

func (r *bufferedResults) mergeAndSort(shards int, funcs map[int]*sqlparser.Call, orderBy []string, limit int) error {
	if len(r.tempColumns) > 0 {
		r.columns = r.columns[0 : len(r.columns)-2*len(r.tempColumns)]
	}

	if shards < 2 {
		return nil
	}

	if len(funcs) > 0 {
		r.values = r.merge(funcs)
	}

	if len(orderBy) > 0 && len(r.values) > 1 {
		if err := r.validOrderByClause(orderBy); err != nil {
			return err
		}

		slices.SortFunc(r.values, r.sortFunc(orderBy))
	}

	if limit >= 0 && len(r.values) > limit {
		r.values = r.values[0:limit]
	}

	return nil
}

func (r *bufferedResults) merge(funcs map[int]*sqlparser.Call) [][]driver.Value {
	if len(r.columns) == len(funcs) {
		return r.values
	}

	funcsColumnIndexes := make([]int, 0)
	for i := range funcs {
		funcsColumnIndexes = append(funcsColumnIndexes, i)
	}

	groupByColumns := make([]int, 0)
	for i := range len(r.columns) {
		if slices.Contains(funcsColumnIndexes, i) {
			continue
		}
		groupByColumns = append(groupByColumns, i)
	}

	strategies := make(map[int]aggregateStrategy)
	for i, typ := range funcs {
		strategies[i] = agregateStrategyFactory(typ)
	}

	groupByValues := make(map[string][]driver.Value)
	for _, newRow := range r.values {
		key := rowKey(newRow, groupByColumns)
		if row, ok := groupByValues[key]; ok {
			for i, aggregateStrategy := range strategies {
				agg := row[i]
				row[i] = aggregateStrategy.apply(agg, newRow[i])
			}
			groupByValues[key] = row
		} else {
			groupByValues[key] = newRow
		}
	}

	if len(r.tempColumns) > 0 {
		//calculate AVG values
		for _, row := range groupByValues {
			for i, countCol := range r.tempColumns {
				var count, sum float64
				switch v := row[countCol].(type) {
				case int64:
					count = float64(v)
				case float64:
					count = v
				}
				switch v := row[countCol+1].(type) {
				case int64:
					sum = float64(v)
				case float64:
					sum = v
				}
				if count == 0 {
					row[i] = 0
				} else {
					row[i] = sum / count
				}
			}
		}
	}

	values := make([][]driver.Value, len(groupByValues))
	count := 0
	for _, row := range groupByValues {
		if l := len(r.tempColumns); l > 0 {
			values[count] = row[0 : len(row)-2*l]
		} else {
			values[count] = row
		}
		count++
	}

	return values
}

type aggregateStrategy interface {
	apply(agg driver.Value, new driver.Value) driver.Value
}

type sumStrategy struct{}

func (s *sumStrategy) apply(agg driver.Value, new driver.Value) driver.Value {
	if agg == nil {
		return new
	}
	if new == nil {
		return agg
	}
	switch total := agg.(type) {
	case int64:
		newValue, _ := new.(int64)
		return total + newValue
	case float64:
		newValue, _ := new.(float64)
		return total + newValue
	default:
		panic("invalid COUNT/SUM/TOTAL aggregation type:" + fmt.Sprintf("%T", agg))
	}
}

type minStrategy struct{}

func (s *minStrategy) apply(agg driver.Value, new driver.Value) driver.Value {
	if agg == nil {
		return new
	}
	if new == nil {
		return agg
	}
	switch x := agg.(type) {
	case int64:
		return min(x, new.(int64))
	case float64:
		return min(x, new.(float64))
	case string:
		return min(x, new.(string))
	case time.Time:
		y, _ := new.(time.Time)
		res := x.Compare(y)
		if res < 0 {
			return x
		}
		return y
	case []byte:
		y, _ := new.([]byte)
		res := bytes.Compare(x, y)
		if res < 0 {
			return x
		}
		return y
	default:
		panic("invalid MIN aggregation type:" + fmt.Sprintf("%T", agg))
	}
}

type maxStrategy struct{}

func (s *maxStrategy) apply(agg driver.Value, new driver.Value) driver.Value {
	if agg == nil {
		return new
	}
	if new == nil {
		return agg
	}
	switch x := agg.(type) {
	case int64:
		return max(x, new.(int64))
	case float64:
		return max(x, new.(float64))
	case string:
		return max(x, new.(string))
	case time.Time:
		y, _ := new.(time.Time)
		res := x.Compare(y)
		if res > 0 {
			return x
		}
		return y
	case []byte:
		y, _ := new.([]byte)
		res := bytes.Compare(x, y)
		if res > 0 {
			return x
		}
		return y
	default:
		panic("invalid MAX aggregation type:" + fmt.Sprintf("%T", agg))
	}
}

type avgStrategy struct{}

func (s *avgStrategy) apply(agg driver.Value, new driver.Value) driver.Value {
	return nil // will be calculated at the end
}

type stringAggStrategy struct {
	sep string
}

func (s *stringAggStrategy) apply(agg driver.Value, new driver.Value) driver.Value {
	if agg == nil {
		return new
	}
	if new == nil {
		return agg
	}
	switch total := agg.(type) {
	case string:
		newValue, _ := new.(string)
		return total + s.sep + newValue
	default:
		panic("invalid STRING_AGG/GROUP_CONCAT aggregation type:" + fmt.Sprintf("%T", agg))
	}
}

type distinctStringAggStrategy struct {
	sep string
}

func (s *distinctStringAggStrategy) apply(agg driver.Value, new driver.Value) driver.Value {
	if agg == nil {
		return new
	}
	if new == nil {
		return agg
	}
	switch total := agg.(type) {
	case string:
		newValue, _ := new.(string)
		values := strings.Split(total, s.sep)
		if slices.Contains(values, newValue) {
			return agg
		}
		return total + s.sep + newValue
	default:
		panic("invalid STRING_AGG/GROUP_CONCAT aggregation type:" + fmt.Sprintf("%T", agg))
	}
}

func agregateStrategyFactory(call *sqlparser.Call) aggregateStrategy {
	switch strings.ToUpper(call.Name.Name) {
	case "COUNT", "SUM", "TOTAL":
		return &sumStrategy{}
	case "MIN":
		return &minStrategy{}
	case "MAX":
		return &maxStrategy{}
	case "AVG":
		return &avgStrategy{}
	case "STRING_AGG", "GROUP_CONCAT":
		sep := ","
		if len(call.Args) > 1 {
			sep = call.Args[1].String()
		}
		if call.Distinct.IsValid() {
			return &distinctStringAggStrategy{
				sep: sep,
			}
		}
		return &stringAggStrategy{
			sep: sep,
		}
	}
	panic("usupported aggregate function: " + call.Name.Name)
}

func rowKey(row []driver.Value, keyColumns []int) string {
	key := make([]string, len(keyColumns))
	for i, rowIndex := range keyColumns {
		key[i] = fmt.Sprint(row[rowIndex])
	}
	return strings.Join(key, "-_-")
}

func (r *bufferedResults) sliceIndexByNameOrOrder(column string) int {
	for i, col := range r.columns {
		if strings.EqualFold(column, col) {
			return i
		}
	}

	if i, err := strconv.Atoi(column); err == nil {
		return i - 1
	}
	return -1
}

func (r *bufferedResults) validOrderByClause(orderBy []string) error {
	for _, column := range orderBy {
		var exists bool
		column := strings.TrimSuffix(column, " DESC")
		for _, col := range r.columns {
			if strings.EqualFold(column, col) {
				exists = true
				break
			}
		}
		if !exists {
			if i, err := strconv.Atoi(column); err == nil && i >= 1 && len(column) >= i {
				exists = true
			}
		}
		if !exists {
			return fmt.Errorf("invalid orderBy clause: %s", column)
		}
	}
	return nil
}

type orderClause struct {
	order int
	desc  bool
}

func (r *bufferedResults) sortFunc(orderBy []string) func(a []driver.Value, b []driver.Value) int {
	clauses := make([]orderClause, 0)
	for _, ob := range orderBy {
		column, desc := strings.CutSuffix(ob, " DESC")
		clauses = append(clauses, orderClause{
			order: r.sliceIndexByNameOrOrder(column),
			desc:  desc,
		})
	}
	return func(a, b []driver.Value) int {
		for _, term := range clauses {
			var result int
			aValue := a[term.order]
			bValue := b[term.order]
			switch x := aValue.(type) {
			case int64:
				if y, ok := bValue.(int64); ok {
					result = cmp.Compare(x, y)
				}
			case float64:
				if y, ok := bValue.(float64); ok {
					result = cmp.Compare(x, y)
				}
			case string:
				if y, ok := bValue.(string); ok {
					result = cmp.Compare(x, y)
				}
			case time.Time:
				if y, ok := bValue.(time.Time); ok {
					result = x.Compare(y)
				}
			case []byte:
				if y, ok := bValue.([]byte); ok {
					result = bytes.Compare(x, y)
				}
			}
			if result != 0 {
				if term.desc {
					return result * -1
				}
				return result
			}

		}
		return 0
	}
}

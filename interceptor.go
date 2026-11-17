package ha

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	rqlitesql "github.com/rqlite/sql"
)

// DefaultLWWTableName is the default local metadata table used by
// LastWriterWinsInterceptor to persist per-row version timestamps and tombstones.
const DefaultLWWTableName = "ha_lww_versions"

// LocalChangeSetObserver is an optional interface that ChangeSetInterceptor
// implementations may implement to observe locally committed ChangeSets on the
// originating node (for example, to record local row timestamps for
// multi-master conflict resolution before remote changesets arrive).
type LocalChangeSetObserver interface {
	OnLocalChangeSet(cs *ChangeSet)
}

type selfChangeSetRecorder interface {
	recordSelfChangeSet(ctx context.Context, cs *ChangeSet, conn *sql.Conn) error
}

func notifyLocalChangeSet(interceptor ChangeSetInterceptor, cs *ChangeSet) {
	if interceptor == nil || cs == nil || len(cs.Changes) == 0 {
		return
	}
	if observer, ok := interceptor.(LocalChangeSetObserver); ok {
		observer.OnLocalChangeSet(cs)
	}
}

func isInternalTable(table string) bool {
	return strings.EqualFold(table, controlTableName) ||
		strings.EqualFold(table, DefaultLWWTableName) ||
		strings.EqualFold(table, OutboxTable) ||
		strings.EqualFold(table, TwoPhaseCommitDecisionTable)
}

// NoopInterceptor is a no-op ChangeSetInterceptor that allows all changesets.
// It can be used directly or embedded in custom interceptors that only need
// to override BeforeApply or AfterApply.
type NoopInterceptor struct{}

// NewNoopInterceptor returns a new NoopInterceptor.
func NewNoopInterceptor() *NoopInterceptor {
	return &NoopInterceptor{}
}

func (*NoopInterceptor) BeforeApply(*ChangeSet, *sql.Conn) (bool, error) {
	return false, nil
}

func (*NoopInterceptor) AfterApply(*ChangeSet, *sql.Conn, error) error {
	return nil
}

// ChangeSetInterceptorFuncs adapts plain functions to the ChangeSetInterceptor interface.
type ChangeSetInterceptorFuncs struct {
	BeforeApplyFn func(cs *ChangeSet, conn *sql.Conn) (skip bool, err error)
	AfterApplyFn  func(cs *ChangeSet, conn *sql.Conn, applyErr error) error
}

func (f ChangeSetInterceptorFuncs) BeforeApply(cs *ChangeSet, conn *sql.Conn) (bool, error) {
	if f.BeforeApplyFn == nil {
		return false, nil
	}
	return f.BeforeApplyFn(cs, conn)
}

func (f ChangeSetInterceptorFuncs) AfterApply(cs *ChangeSet, conn *sql.Conn, applyErr error) error {
	if f.AfterApplyFn == nil {
		return nil
	}
	return f.AfterApplyFn(cs, conn, applyErr)
}

// CompositeInterceptor chains multiple ChangeSetInterceptors together.
// BeforeApply executes interceptors in order and stops early if any interceptor
// returns skip=true or a non-nil error. AfterApply executes in reverse order
// for all interceptors whose BeforeApply ran.
type CompositeInterceptor struct {
	mu           sync.Mutex
	interceptors []ChangeSetInterceptor
	executed     map[*ChangeSet]int
}

// NewCompositeInterceptor creates a CompositeInterceptor from the given interceptors.
// Nil entries are ignored.
func NewCompositeInterceptor(interceptors ...ChangeSetInterceptor) *CompositeInterceptor {
	filtered := make([]ChangeSetInterceptor, 0, len(interceptors))
	for _, interceptor := range interceptors {
		if interceptor != nil {
			filtered = append(filtered, interceptor)
		}
	}
	return &CompositeInterceptor{
		interceptors: filtered,
		executed:     make(map[*ChangeSet]int),
	}
}

func (c *CompositeInterceptor) BeforeApply(cs *ChangeSet, conn *sql.Conn) (bool, error) {
	count := 0
	for _, interceptor := range c.interceptors {
		count++
		skip, err := interceptor.BeforeApply(cs, conn)
		if err != nil || skip {
			c.recordExecuted(cs, count)
			return skip, err
		}
	}
	c.recordExecuted(cs, count)
	return false, nil
}

func (c *CompositeInterceptor) AfterApply(cs *ChangeSet, conn *sql.Conn, applyErr error) error {
	count := c.popExecuted(cs)
	var joined error
	for i := count - 1; i >= 0; i-- {
		joined = errors.Join(joined, c.interceptors[i].AfterApply(cs, conn, applyErr))
	}
	return joined
}

func (c *CompositeInterceptor) OnLocalChangeSet(cs *ChangeSet) {
	for _, interceptor := range c.interceptors {
		if observer, ok := interceptor.(LocalChangeSetObserver); ok {
			observer.OnLocalChangeSet(cs)
		}
	}
}

func (c *CompositeInterceptor) recordSelfChangeSet(ctx context.Context, cs *ChangeSet, conn *sql.Conn) error {
	var joined error
	for _, interceptor := range c.interceptors {
		if recorder, ok := interceptor.(selfChangeSetRecorder); ok {
			joined = errors.Join(joined, recorder.recordSelfChangeSet(ctx, cs, conn))
		}
	}
	return joined
}

func (c *CompositeInterceptor) recordExecuted(cs *ChangeSet, count int) {
	if cs == nil {
		return
	}
	c.mu.Lock()
	c.executed[cs] = count
	c.mu.Unlock()
}

func (c *CompositeInterceptor) popExecuted(cs *ChangeSet) int {
	if cs == nil {
		return len(c.interceptors)
	}
	c.mu.Lock()
	count, ok := c.executed[cs]
	if ok {
		delete(c.executed, cs)
	}
	c.mu.Unlock()
	if !ok {
		return len(c.interceptors)
	}
	return count
}

// ChangeFilterFunc decides whether an individual Change within a ChangeSet should be applied.
type ChangeFilterFunc func(cs *ChangeSet, change Change) bool

// FilterInterceptor filters Changes in a ChangeSet using a predicate function.
// If all changes in a non-empty ChangeSet are filtered out, BeforeApply returns skip=true.
type FilterInterceptor struct {
	NoopInterceptor
	keep ChangeFilterFunc
}

// NewFilterInterceptor creates a FilterInterceptor that retains only changes where keep returns true.
func NewFilterInterceptor(keep ChangeFilterFunc) *FilterInterceptor {
	return &FilterInterceptor{keep: keep}
}

func (f *FilterInterceptor) BeforeApply(cs *ChangeSet, _ *sql.Conn) (bool, error) {
	if cs == nil || f.keep == nil || len(cs.Changes) == 0 {
		return false, nil
	}
	kept := make([]Change, 0, len(cs.Changes))
	for _, change := range cs.Changes {
		if f.keep(cs, change) {
			kept = append(kept, change)
		}
	}
	cs.Changes = kept
	return len(kept) == 0, nil
}

// NewAllowTablesInterceptor returns an interceptor that only applies changes targeting the specified tables.
func NewAllowTablesInterceptor(tables ...string) *FilterInterceptor {
	allowed := normalizeNameSet(tables)
	return NewFilterInterceptor(func(_ *ChangeSet, change Change) bool {
		if change.Table != "" {
			_, ok := allowed[strings.ToLower(change.Table)]
			return ok
		}
		if strings.EqualFold(change.Operation, "SQL") {
			ddlTables := extractSQLStatementTables(change.Command)
			if len(ddlTables) == 0 {
				return false
			}
			for _, tbl := range ddlTables {
				if _, ok := allowed[tbl]; !ok {
					return false
				}
			}
			return true
		}
		return false
	})
}

// NewIgnoreTablesInterceptor returns an interceptor that drops changes targeting the specified tables.
func NewIgnoreTablesInterceptor(tables ...string) *FilterInterceptor {
	ignored := normalizeNameSet(tables)
	return NewFilterInterceptor(func(_ *ChangeSet, change Change) bool {
		if change.Table != "" {
			_, skip := ignored[strings.ToLower(change.Table)]
			return !skip
		}
		if strings.EqualFold(change.Operation, "SQL") {
			ddlTables := extractSQLStatementTables(change.Command)
			if len(ddlTables) == 0 {
				return true
			}
			for _, tbl := range ddlTables {
				if _, skip := ignored[tbl]; skip {
					return false
				}
			}
		}
		return true
	})
}

// NewAllowOperationsInterceptor returns an interceptor that only applies changes whose Operation
// matches one of the specified operations (e.g. "INSERT", "UPDATE", "DELETE", "SQL").
func NewAllowOperationsInterceptor(operations ...string) *FilterInterceptor {
	allowed := normalizeNameSet(operations)
	return NewFilterInterceptor(func(_ *ChangeSet, change Change) bool {
		_, ok := allowed[strings.ToLower(change.Operation)]
		return ok
	})
}

// NewIgnoreOperationsInterceptor returns an interceptor that drops changes whose Operation
// matches any of the specified operations (e.g. "DELETE", "SQL").
func NewIgnoreOperationsInterceptor(operations ...string) *FilterInterceptor {
	ignored := normalizeNameSet(operations)
	return NewFilterInterceptor(func(_ *ChangeSet, change Change) bool {
		_, skip := ignored[strings.ToLower(change.Operation)]
		return !skip
	})
}

// LWWTieBreaker breaks ties when an incoming change and the local state have the exact same timestamp/version.
// Returning true means the incoming change wins; returning false keeps the local state.
type LWWTieBreaker func(incomingNode, localNode string) bool

// DefaultLWWTieBreaker deterministically breaks timestamp ties by comparing node names lexicographically.
// For identical node names (e.g. idempotent replay or consecutive writes in the same nanosecond), incoming wins.
func DefaultLWWTieBreaker(incomingNode, localNode string) bool {
	return incomingNode >= localNode
}

// LWWConflictHandler is called whenever an incoming Change is skipped due to a newer local version.
type LWWConflictHandler func(cs *ChangeSet, change Change, localTsNs int64, localNode string)

// LWWOption configures a LastWriterWinsInterceptor.
type LWWOption func(*LastWriterWinsInterceptor)

// WithLWWTableName sets the local SQLite table name used to store per-row LWW metadata.
func WithLWWTableName(name string) LWWOption {
	return func(i *LastWriterWinsInterceptor) {
		if strings.TrimSpace(name) != "" {
			i.tableName = strings.TrimSpace(name)
		}
	}
}

// WithLWWTieBreaker sets a custom tie-breaker function for equal timestamps.
func WithLWWTieBreaker(fn LWWTieBreaker) LWWOption {
	return func(i *LastWriterWinsInterceptor) {
		if fn != nil {
			i.tieBreaker = fn
		}
	}
}

// WithLWWTables restricts LWW conflict resolution to the given tables.
// Changes to other tables pass through unmodified.
func WithLWWTables(tables ...string) LWWOption {
	return func(i *LastWriterWinsInterceptor) {
		i.allowTables = normalizeNameSet(tables)
	}
}

// WithLWWIgnoreTables excludes the given tables from LWW conflict resolution.
func WithLWWIgnoreTables(tables ...string) LWWOption {
	return func(i *LastWriterWinsInterceptor) {
		i.ignoreTables = normalizeNameSet(tables)
	}
}

// WithLWWAtomicTransaction controls whether a conflict on any single row skips
// the entire ChangeSet transaction (true) or only drops the stale row Changes (false, default).
func WithLWWAtomicTransaction(atomic bool) LWWOption {
	return func(i *LastWriterWinsInterceptor) {
		i.atomicTx = atomic
	}
}

// WithLWWOnConflict registers a callback invoked whenever an incoming Change loses LWW resolution.
func WithLWWOnConflict(fn LWWConflictHandler) LWWOption {
	return func(i *LastWriterWinsInterceptor) {
		i.onConflict = fn
	}
}

type lwwRowID struct {
	db    string
	table string
	key   string
}

type lwwVersion struct {
	tsNs int64
	node string
}

type lwwRecord struct {
	id  lwwRowID
	ver lwwVersion
}

// LastWriterWinsInterceptor implements schema-less Last-Writer-Wins (LWW) conflict
// resolution using Change.TsNs (falling back to ChangeSet.Timestamp) and a
// deterministic node-name tie-breaker.
//
// Per-row timestamps and delete tombstones are persisted in a local control table
// (DefaultLWWTableName by default) and local commits on the origin node are tracked
// automatically via LocalChangeSetObserver so multi-master clusters converge cleanly
// without requiring timestamp columns on application tables.
type LastWriterWinsInterceptor struct {
	tableName    string
	tieBreaker   LWWTieBreaker
	allowTables  map[string]struct{}
	ignoreTables map[string]struct{}
	atomicTx     bool
	onConflict   LWWConflictHandler

	mu           sync.Mutex
	pendingLocal map[lwwRowID]lwwVersion
	staged       map[*ChangeSet][]lwwRecord
}

// NewLastWriterWinsInterceptor creates a new LastWriterWinsInterceptor with the given options.
func NewLastWriterWinsInterceptor(opts ...LWWOption) *LastWriterWinsInterceptor {
	interceptor := &LastWriterWinsInterceptor{
		tableName:    DefaultLWWTableName,
		tieBreaker:   DefaultLWWTieBreaker,
		pendingLocal: make(map[lwwRowID]lwwVersion),
		staged:       make(map[*ChangeSet][]lwwRecord),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(interceptor)
		}
	}
	return interceptor
}

// OnLocalChangeSet records locally committed row versions so concurrent remote
// changesets with older timestamps cannot overwrite newer local writes.
func (i *LastWriterWinsInterceptor) OnLocalChangeSet(cs *ChangeSet) {
	if cs == nil || len(cs.Changes) == 0 {
		return
	}
	records := i.extractChangeSetRecords(cs)
	if len(records) == 0 {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, rec := range records {
		if cur, exists := i.pendingLocal[rec.id]; !exists || i.wins(rec.ver, cur) {
			i.pendingLocal[rec.id] = rec.ver
		}
	}
}

func (i *LastWriterWinsInterceptor) recordSelfChangeSet(ctx context.Context, cs *ChangeSet, conn *sql.Conn) error {
	if conn == nil {
		i.OnLocalChangeSet(cs)
		return nil
	}
	ctx = ContextLocalDB(ctx, true)
	if err := i.ensureTable(ctx, conn); err != nil {
		i.OnLocalChangeSet(cs)
		return err
	}
	if err := i.flushPendingLocal(ctx, conn); err != nil {
		i.OnLocalChangeSet(cs)
		return err
	}
	for _, rec := range i.extractChangeSetRecords(cs) {
		if err := i.upsertIfNewer(ctx, conn, rec); err != nil {
			return err
		}
	}
	return nil
}

func (i *LastWriterWinsInterceptor) BeforeApply(cs *ChangeSet, conn *sql.Conn) (bool, error) {
	if cs == nil || len(cs.Changes) == 0 || conn == nil {
		return false, nil
	}
	ctx := ContextLocalDB(context.Background(), true)
	if err := i.ensureTable(ctx, conn); err != nil {
		return false, err
	}
	if err := i.flushPendingLocal(ctx, conn); err != nil {
		return false, err
	}

	batchVersions := make(map[lwwRowID]lwwVersion)
	stagedRecords := make([]lwwRecord, 0, len(cs.Changes))
	keptChanges := make([]Change, 0, len(cs.Changes))
	filteredAny := false

	for _, change := range cs.Changes {
		if !i.managesChange(change) {
			keptChanges = append(keptChanges, change)
			continue
		}
		keys, ok := extractChangeRowKeys(change)
		if !ok {
			keptChanges = append(keptChanges, change)
			continue
		}

		incomingVer := lwwVersion{
			tsNs: effectiveChangeTimestamp(cs, change),
			node: cs.Node,
		}
		dbName := normalizeDBName(change.Database)
		tblName := strings.ToLower(change.Table)

		changeWins := true
		for _, key := range keys {
			rowID := lwwRowID{db: dbName, table: tblName, key: key}
			localVer, exists, err := i.lookupVersion(ctx, conn, batchVersions, rowID)
			if err != nil {
				return false, err
			}
			if exists && !i.wins(incomingVer, localVer) {
				changeWins = false
				if i.onConflict != nil {
					i.onConflict(cs, change, localVer.tsNs, localVer.node)
				}
				break
			}
		}

		if !changeWins {
			if i.atomicTx {
				i.clearStaged(cs)
				return true, nil
			}
			filteredAny = true
			continue
		}

		keptChanges = append(keptChanges, change)
		for _, key := range keys {
			rowID := lwwRowID{db: dbName, table: tblName, key: key}
			batchVersions[rowID] = incomingVer
			stagedRecords = append(stagedRecords, lwwRecord{id: rowID, ver: incomingVer})
		}
	}

	cs.Changes = keptChanges
	if len(keptChanges) == 0 && filteredAny {
		i.clearStaged(cs)
		return true, nil
	}

	i.mu.Lock()
	if len(stagedRecords) > 0 {
		i.staged[cs] = stagedRecords
	} else {
		delete(i.staged, cs)
	}
	i.mu.Unlock()

	return false, nil
}

func (i *LastWriterWinsInterceptor) AfterApply(cs *ChangeSet, conn *sql.Conn, applyErr error) error {
	records := i.popStaged(cs)
	if applyErr != nil || len(records) == 0 || conn == nil {
		return nil
	}
	ctx := ContextLocalDB(context.Background(), true)
	for _, rec := range records {
		if err := i.upsertRecord(ctx, conn, rec); err != nil {
			return err
		}
	}
	return nil
}

func (i *LastWriterWinsInterceptor) wins(incoming, local lwwVersion) bool {
	if incoming.tsNs != local.tsNs {
		return incoming.tsNs > local.tsNs
	}
	return i.tieBreaker(incoming.node, local.node)
}

func (i *LastWriterWinsInterceptor) managesChange(change Change) bool {
	if change.Table == "" || isInternalTable(change.Table) || strings.EqualFold(change.Table, i.tableName) {
		return false
	}
	switch strings.ToUpper(change.Operation) {
	case "INSERT", "UPDATE", "DELETE":
	default:
		return false
	}
	tbl := strings.ToLower(change.Table)
	if _, ignored := i.ignoreTables[tbl]; ignored {
		return false
	}
	if len(i.allowTables) > 0 {
		if _, allowed := i.allowTables[tbl]; !allowed {
			return false
		}
	}
	return true
}

func (i *LastWriterWinsInterceptor) extractChangeSetRecords(cs *ChangeSet) []lwwRecord {
	var records []lwwRecord
	for _, change := range cs.Changes {
		if !i.managesChange(change) {
			continue
		}
		keys, ok := extractChangeRowKeys(change)
		if !ok {
			continue
		}
		ver := lwwVersion{
			tsNs: effectiveChangeTimestamp(cs, change),
			node: cs.Node,
		}
		dbName := normalizeDBName(change.Database)
		tblName := strings.ToLower(change.Table)
		for _, key := range keys {
			records = append(records, lwwRecord{
				id:  lwwRowID{db: dbName, table: tblName, key: key},
				ver: ver,
			})
		}
	}
	return records
}

func (i *LastWriterWinsInterceptor) ensureTable(ctx context.Context, conn *sql.Conn) error {
	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (db_name TEXT NOT NULL, table_name TEXT NOT NULL, row_key TEXT NOT NULL, ts_ns INTEGER NOT NULL, node TEXT NOT NULL, PRIMARY KEY (db_name, table_name, row_key))`,
		quoteSQLiteIdent(i.tableName),
	)
	_, err := conn.ExecContext(ctx, query)
	return err
}

func (i *LastWriterWinsInterceptor) flushPendingLocal(ctx context.Context, conn *sql.Conn) error {
	i.mu.Lock()
	if len(i.pendingLocal) == 0 {
		i.mu.Unlock()
		return nil
	}
	snapshot := make([]lwwRecord, 0, len(i.pendingLocal))
	for id, ver := range i.pendingLocal {
		snapshot = append(snapshot, lwwRecord{id: id, ver: ver})
	}
	i.pendingLocal = make(map[lwwRowID]lwwVersion)
	i.mu.Unlock()

	for idx, rec := range snapshot {
		if err := i.upsertIfNewer(ctx, conn, rec); err != nil {
			// Restore unflushed records so nothing is lost on transient errors.
			i.mu.Lock()
			for _, rem := range snapshot[idx:] {
				if cur, exists := i.pendingLocal[rem.id]; !exists || i.wins(rem.ver, cur) {
					i.pendingLocal[rem.id] = rem.ver
				}
			}
			i.mu.Unlock()
			return err
		}
	}
	return nil
}

func (i *LastWriterWinsInterceptor) lookupVersion(ctx context.Context, conn *sql.Conn, batch map[lwwRowID]lwwVersion, id lwwRowID) (lwwVersion, bool, error) {
	if ver, ok := batch[id]; ok {
		return ver, true, nil
	}
	return i.queryStoredVersion(ctx, conn, id)
}

func (i *LastWriterWinsInterceptor) queryStoredVersion(ctx context.Context, conn *sql.Conn, id lwwRowID) (lwwVersion, bool, error) {
	query := fmt.Sprintf(
		`SELECT ts_ns, node FROM %s WHERE db_name = ? AND table_name = ? AND row_key = ?`,
		quoteSQLiteIdent(i.tableName),
	)
	var ver lwwVersion
	err := conn.QueryRowContext(ctx, query, id.db, id.table, id.key).Scan(&ver.tsNs, &ver.node)
	if errors.Is(err, sql.ErrNoRows) {
		return lwwVersion{}, false, nil
	}
	if err != nil {
		return lwwVersion{}, false, err
	}
	return ver, true, nil
}

func (i *LastWriterWinsInterceptor) upsertIfNewer(ctx context.Context, conn *sql.Conn, rec lwwRecord) error {
	cur, exists, err := i.queryStoredVersion(ctx, conn, rec.id)
	if err != nil {
		return err
	}
	if exists && !i.wins(rec.ver, cur) {
		return nil
	}
	return i.upsertRecord(ctx, conn, rec)
}

func (i *LastWriterWinsInterceptor) upsertRecord(ctx context.Context, conn *sql.Conn, rec lwwRecord) error {
	query := fmt.Sprintf(
		`REPLACE INTO %s (db_name, table_name, row_key, ts_ns, node) VALUES (?, ?, ?, ?, ?)`,
		quoteSQLiteIdent(i.tableName),
	)
	_, err := conn.ExecContext(ctx, query, rec.id.db, rec.id.table, rec.id.key, rec.ver.tsNs, rec.ver.node)
	return err
}

func (i *LastWriterWinsInterceptor) clearStaged(cs *ChangeSet) {
	if cs == nil {
		return
	}
	i.mu.Lock()
	delete(i.staged, cs)
	i.mu.Unlock()
}

func (i *LastWriterWinsInterceptor) popStaged(cs *ChangeSet) []lwwRecord {
	if cs == nil {
		return nil
	}
	i.mu.Lock()
	records := i.staged[cs]
	delete(i.staged, cs)
	i.mu.Unlock()
	return records
}

// ColumnLWWConflictHandler is invoked whenever an incoming Change loses column-based LWW resolution.
type ColumnLWWConflictHandler func(cs *ChangeSet, change Change, localValue, incomingValue any)

// ColumnLWWOption configures a ColumnLWWInterceptor.
type ColumnLWWOption func(*ColumnLWWInterceptor)

// WithTableVersionColumn overrides the version/timestamp column name for a specific table.
func WithTableVersionColumn(table, column string) ColumnLWWOption {
	return func(i *ColumnLWWInterceptor) {
		if strings.TrimSpace(table) != "" && strings.TrimSpace(column) != "" {
			i.tableColumns[strings.ToLower(strings.TrimSpace(table))] = strings.TrimSpace(column)
		}
	}
}

// WithNodeTieBreakerColumn specifies an optional column storing the writer's node identifier
// to break ties when two concurrent writes carry the exact same version/timestamp value.
// When unset, ties are broken deterministically by comparing row column values.
func WithNodeTieBreakerColumn(column string) ColumnLWWOption {
	return func(i *ColumnLWWInterceptor) {
		i.nodeColumn = strings.TrimSpace(column)
	}
}

// WithColumnLWWTieBreaker sets the node tie-breaker used together with WithNodeTieBreakerColumn.
func WithColumnLWWTieBreaker(fn LWWTieBreaker) ColumnLWWOption {
	return func(i *ColumnLWWInterceptor) {
		if fn != nil {
			i.tieBreaker = fn
		}
	}
}

// WithColumnLWWTables restricts column-based LWW conflict resolution to the given tables.
func WithColumnLWWTables(tables ...string) ColumnLWWOption {
	return func(i *ColumnLWWInterceptor) {
		i.allowTables = normalizeNameSet(tables)
	}
}

// WithColumnLWWIgnoreTables excludes the given tables from column-based LWW conflict resolution.
func WithColumnLWWIgnoreTables(tables ...string) ColumnLWWOption {
	return func(i *ColumnLWWInterceptor) {
		i.ignoreTables = normalizeNameSet(tables)
	}
}

// WithColumnLWWAtomicTransaction controls whether a conflict on any single row skips
// the entire ChangeSet transaction (true) or only drops the stale row Changes (false, default).
func WithColumnLWWAtomicTransaction(atomic bool) ColumnLWWOption {
	return func(i *ColumnLWWInterceptor) {
		i.atomicTx = atomic
	}
}

// WithColumnLWWStrictMissingColumn returns an error in BeforeApply if a managed table
// does not contain the configured version column (default false: changes pass through).
func WithColumnLWWStrictMissingColumn(strict bool) ColumnLWWOption {
	return func(i *ColumnLWWInterceptor) {
		i.strictMissingColumn = strict
	}
}

// WithColumnLWWOnConflict registers a callback invoked whenever an incoming Change loses resolution.
func WithColumnLWWOnConflict(fn ColumnLWWConflictHandler) ColumnLWWOption {
	return func(i *ColumnLWWInterceptor) {
		i.onConflict = fn
	}
}

// ColumnLWWInterceptor implements Last-Writer-Wins / monotonic-version conflict
// resolution by comparing a column on the target table itself (such as "updated_at",
// "version", or an HLC string) against the current row stored in the local database.
//
// Supported column types include integers, floats, time.Time, RFC3339 / SQLite
// datetime strings, and lexicographically ordered strings (such as ISO-8601 UTC or HLCs).
// For DELETE changes, the delete applies only if the local row version has not advanced
// past the version observed in Change.OldValues, preventing concurrent updates from
// being lost while maintaining convergence across nodes.
type ColumnLWWInterceptor struct {
	NoopInterceptor
	defaultColumn       string
	tableColumns        map[string]string
	nodeColumn          string
	tieBreaker          LWWTieBreaker
	allowTables         map[string]struct{}
	ignoreTables        map[string]struct{}
	atomicTx            bool
	strictMissingColumn bool
	onConflict          ColumnLWWConflictHandler
}

// NewColumnLWWInterceptor creates a ColumnLWWInterceptor using column as the default
// version/timestamp column name (defaults to "updated_at" if empty).
func NewColumnLWWInterceptor(column string, opts ...ColumnLWWOption) *ColumnLWWInterceptor {
	col := strings.TrimSpace(column)
	if col == "" {
		col = "updated_at"
	}
	interceptor := &ColumnLWWInterceptor{
		defaultColumn: col,
		tableColumns:  make(map[string]string),
		tieBreaker:    DefaultLWWTieBreaker,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(interceptor)
		}
	}
	return interceptor
}

type columnBatchRow struct {
	deleted bool
	values  []any
}

func (i *ColumnLWWInterceptor) BeforeApply(cs *ChangeSet, conn *sql.Conn) (bool, error) {
	if cs == nil || len(cs.Changes) == 0 || conn == nil {
		return false, nil
	}
	ctx := ContextLocalDB(context.Background(), true)
	batchRows := make(map[lwwRowID]columnBatchRow)
	keptChanges := make([]Change, 0, len(cs.Changes))
	filteredAny := false

	for _, change := range cs.Changes {
		if !i.managesChange(change) {
			keptChanges = append(keptChanges, change)
			continue
		}
		verCol := i.versionColumnFor(change.Table)
		verIdx := findColumnIndex(change.Columns, verCol)
		if verIdx < 0 {
			if i.strictMissingColumn {
				return false, fmt.Errorf("version column %q not found in table %q", verCol, change.Table)
			}
			keptChanges = append(keptChanges, change)
			continue
		}

		wins, localVer, incomingVer, err := i.evaluateChange(ctx, conn, batchRows, change, verIdx)
		if err != nil {
			return false, err
		}
		if !wins {
			if i.onConflict != nil {
				i.onConflict(cs, change, localVer, incomingVer)
			}
			if i.atomicTx {
				return true, nil
			}
			filteredAny = true
			continue
		}
		keptChanges = append(keptChanges, change)
		i.recordBatchChange(batchRows, change)
	}

	cs.Changes = keptChanges
	if len(keptChanges) == 0 && filteredAny {
		return true, nil
	}
	return false, nil
}

func (i *ColumnLWWInterceptor) managesChange(change Change) bool {
	if change.Table == "" || isInternalTable(change.Table) {
		return false
	}
	switch strings.ToUpper(change.Operation) {
	case "INSERT", "UPDATE", "DELETE":
	default:
		return false
	}
	tbl := strings.ToLower(change.Table)
	if _, ignored := i.ignoreTables[tbl]; ignored {
		return false
	}
	if len(i.allowTables) > 0 {
		if _, allowed := i.allowTables[tbl]; !allowed {
			return false
		}
	}
	return true
}

func (i *ColumnLWWInterceptor) versionColumnFor(table string) string {
	if col, ok := i.tableColumns[strings.ToLower(table)]; ok {
		return col
	}
	return i.defaultColumn
}

func (i *ColumnLWWInterceptor) evaluateChange(ctx context.Context, conn *sql.Conn, batch map[lwwRowID]columnBatchRow, change Change, verIdx int) (wins bool, localVer any, incomingVer any, err error) {
	op := strings.ToUpper(change.Operation)
	switch op {
	case "INSERT":
		if verIdx >= len(change.NewValues) {
			return true, nil, nil, nil
		}
		incomingVer = change.NewValues[verIdx]
		whereCols, whereVals, ok := rowLookupPredicate(change, true)
		if !ok {
			return true, nil, incomingVer, nil
		}
		localRow, exists, err := i.lookupRow(ctx, conn, batch, change.Database, change.Table, change.Columns, whereCols, whereVals)
		if err != nil || !exists {
			return true, nil, incomingVer, err
		}
		localVer = localRow[verIdx]
		return i.incomingRowWins(change.Columns, change.NewValues, localRow, incomingVer, localVer), localVer, incomingVer, nil

	case "UPDATE":
		if verIdx >= len(change.NewValues) {
			return true, nil, nil, nil
		}
		incomingVer = change.NewValues[verIdx]
		oldCols, oldVals, okOld := rowLookupPredicate(change, false)
		if okOld {
			localRow, exists, err := i.lookupRow(ctx, conn, batch, change.Database, change.Table, change.Columns, oldCols, oldVals)
			if err != nil {
				return false, nil, incomingVer, err
			}
			if exists {
				localVer = localRow[verIdx]
				if !i.incomingRowWins(change.Columns, change.NewValues, localRow, incomingVer, localVer) {
					return false, localVer, incomingVer, nil
				}
			}
		}
		// If the primary key / rowid changed, also verify the destination row key.
		newCols, newVals, okNew := rowLookupPredicate(change, true)
		if okNew && (!okOld || !equalValueSlices(oldVals, newVals)) {
			localRow, exists, err := i.lookupRow(ctx, conn, batch, change.Database, change.Table, change.Columns, newCols, newVals)
			if err != nil {
				return false, nil, incomingVer, err
			}
			if exists {
				localVer = localRow[verIdx]
				if !i.incomingRowWins(change.Columns, change.NewValues, localRow, incomingVer, localVer) {
					return false, localVer, incomingVer, nil
				}
			}
		}
		return true, localVer, incomingVer, nil

	case "DELETE":
		if verIdx >= len(change.OldValues) {
			return true, nil, nil, nil
		}
		incomingVer = change.OldValues[verIdx]
		whereCols, whereVals, ok := rowLookupPredicate(change, false)
		if !ok {
			return true, nil, incomingVer, nil
		}
		localRow, exists, err := i.lookupRow(ctx, conn, batch, change.Database, change.Table, change.Columns, whereCols, whereVals)
		if err != nil || !exists {
			return true, nil, incomingVer, err
		}
		localVer = localRow[verIdx]
		// Apply DELETE iff the local row has not advanced past the version observed by DELETE.
		return compareVersionValues(incomingVer, localVer) >= 0, localVer, incomingVer, nil

	default:
		return true, nil, nil, nil
	}
}

func (i *ColumnLWWInterceptor) lookupRow(ctx context.Context, conn *sql.Conn, batch map[lwwRowID]columnBatchRow, database, table string, columns, whereCols []string, whereVals []any) ([]any, bool, error) {
	id := lwwRowID{
		db:    normalizeDBName(database),
		table: strings.ToLower(table),
		key:   formatPredicateKey(whereCols, whereVals),
	}
	if st, ok := batch[id]; ok {
		if st.deleted {
			return nil, false, nil
		}
		return st.values, true, nil
	}
	return i.queryLocalRow(ctx, conn, database, table, columns, whereCols, whereVals)
}

func (i *ColumnLWWInterceptor) recordBatchChange(batch map[lwwRowID]columnBatchRow, change Change) {
	dbName := normalizeDBName(change.Database)
	tblName := strings.ToLower(change.Table)
	switch strings.ToUpper(change.Operation) {
	case "INSERT":
		if key, ok := buildRowKey(change, true); ok {
			batch[lwwRowID{db: dbName, table: tblName, key: key}] = columnBatchRow{values: change.NewValues}
		}
	case "UPDATE":
		oldKey, okOld := buildRowKey(change, false)
		newKey, okNew := buildRowKey(change, true)
		if okOld && okNew && oldKey != newKey {
			batch[lwwRowID{db: dbName, table: tblName, key: oldKey}] = columnBatchRow{deleted: true}
		}
		if okNew {
			batch[lwwRowID{db: dbName, table: tblName, key: newKey}] = columnBatchRow{values: change.NewValues}
		}
	case "DELETE":
		if key, ok := buildRowKey(change, false); ok {
			batch[lwwRowID{db: dbName, table: tblName, key: key}] = columnBatchRow{deleted: true}
		}
	}
}

func (i *ColumnLWWInterceptor) incomingRowWins(columns []string, incomingRow, localRow []any, incomingVer, localVer any) bool {
	cmp := compareVersionValues(incomingVer, localVer)
	if cmp > 0 {
		return true
	}
	if cmp < 0 {
		return false
	}
	if i.nodeColumn != "" {
		if nodeIdx := findColumnIndex(columns, i.nodeColumn); nodeIdx >= 0 && nodeIdx < len(incomingRow) && nodeIdx < len(localRow) {
			return i.tieBreaker(scalarToString(incomingRow[nodeIdx]), scalarToString(localRow[nodeIdx]))
		}
	}
	// Deterministic symmetric tie-breaker over row payload values so all nodes converge identically.
	return compareRowPayloads(incomingRow, localRow) >= 0
}

func (i *ColumnLWWInterceptor) queryLocalRow(ctx context.Context, conn *sql.Conn, database, table string, columns, whereCols []string, whereVals []any) ([]any, bool, error) {
	if len(columns) == 0 || len(whereCols) == 0 {
		return nil, false, nil
	}
	quotedCols := make([]string, len(columns))
	for idx, col := range columns {
		quotedCols[idx] = quoteSQLiteIdent(col)
	}
	whereParts := make([]string, 0, len(whereCols))
	args := make([]any, 0, len(whereVals))
	for idx, col := range whereCols {
		if whereVals[idx] == nil {
			whereParts = append(whereParts, fmt.Sprintf("%s IS NULL", quoteSQLiteIdent(col)))
		} else {
			whereParts = append(whereParts, fmt.Sprintf("%s = ?", quoteSQLiteIdent(col)))
			args = append(args, whereVals[idx])
		}
	}
	target := quoteSQLiteTable(database, table)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT 1",
		strings.Join(quotedCols, ", "),
		target,
		strings.Join(whereParts, " AND "),
	)

	values := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for idx := range values {
		ptrs[idx] = &values[idx]
	}
	err := conn.QueryRowContext(ctx, query, args...).Scan(ptrs...)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, "no such table") {
			return nil, false, nil
		}
		if !i.strictMissingColumn && strings.Contains(errMsg, "no such column") {
			return nil, false, nil
		}
		return nil, false, err
	}
	for idx, v := range values {
		if b, ok := v.([]byte); ok {
			values[idx] = string(b)
		}
	}
	return values, true, nil
}

func effectiveChangeTimestamp(cs *ChangeSet, change Change) int64 {
	if change.TsNs > 0 {
		return change.TsNs
	}
	if cs != nil {
		return cs.Timestamp
	}
	return 0
}

func normalizeDBName(db string) string {
	if strings.TrimSpace(db) == "" {
		return "main"
	}
	return strings.ToLower(strings.TrimSpace(db))
}

func normalizeNameSet(names []string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		trimmed := strings.ToLower(strings.TrimSpace(name))
		if trimmed != "" {
			set[trimmed] = struct{}{}
		}
	}
	return set
}

func findColumnIndex(columns []string, target string) int {
	for idx, col := range columns {
		if strings.EqualFold(col, target) {
			return idx
		}
	}
	return -1
}

func quoteSQLiteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func quoteSQLiteTable(database, table string) string {
	if strings.TrimSpace(database) == "" {
		return quoteSQLiteIdent(table)
	}
	return quoteSQLiteIdent(database) + "." + quoteSQLiteIdent(table)
}

func extractChangeRowKeys(change Change) ([]string, bool) {
	switch strings.ToUpper(change.Operation) {
	case "INSERT":
		key, ok := buildRowKey(change, true)
		if !ok {
			return nil, false
		}
		return []string{key}, true
	case "DELETE":
		key, ok := buildRowKey(change, false)
		if !ok {
			return nil, false
		}
		return []string{key}, true
	case "UPDATE":
		oldKey, okOld := buildRowKey(change, false)
		newKey, okNew := buildRowKey(change, true)
		switch {
		case okOld && okNew:
			if oldKey == newKey {
				return []string{oldKey}, true
			}
			return []string{oldKey, newKey}, true
		case okNew:
			return []string{newKey}, true
		case okOld:
			return []string{oldKey}, true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}

func buildRowKey(change Change, useNew bool) (string, bool) {
	cols, vals, ok := rowLookupPredicate(change, useNew)
	if !ok {
		return "", false
	}
	return formatPredicateKey(cols, vals), true
}

func formatPredicateKey(cols []string, vals []any) string {
	var b strings.Builder
	for idx, col := range cols {
		if idx > 0 {
			b.WriteByte('|')
		}
		b.WriteString(strings.ToLower(col))
		b.WriteByte('=')
		b.WriteString(canonicalScalar(vals[idx]))
	}
	return b.String()
}

func rowLookupPredicate(change Change, useNew bool) ([]string, []any, bool) {
	if len(change.PKColumns) > 0 {
		values := change.OldValues
		if useNew {
			values = change.NewValues
		}
		if len(values) < len(change.Columns) || len(change.Columns) == 0 {
			return nil, nil, false
		}
		pkCols := make([]string, len(change.PKColumns))
		pkVals := make([]any, len(change.PKColumns))
		for idx, pkCol := range change.PKColumns {
			colIdx := findColumnIndex(change.Columns, pkCol)
			if colIdx < 0 || colIdx >= len(values) {
				return nil, nil, false
			}
			pkCols[idx] = change.Columns[colIdx]
			pkVals[idx] = normalizeNumericArg(values[colIdx])
		}
		return pkCols, pkVals, true
	}

	rowID := change.OldRowID
	if useNew {
		rowID = change.NewRowID
	}
	if rowID == 0 {
		return nil, nil, false
	}
	return []string{"rowid"}, []any{rowID}, true
}

func normalizeNumericArg(v any) any {
	switch f := v.(type) {
	case float64:
		if !math.IsNaN(f) && !math.IsInf(f, 0) && math.Trunc(f) == f && f >= -9007199254740992 && f <= 9007199254740992 {
			return int64(f)
		}
	case float32:
		f64 := float64(f)
		if !math.IsNaN(f64) && !math.IsInf(f64, 0) && math.Trunc(f64) == f64 {
			return int64(f64)
		}
	}
	return v
}

func canonicalScalar(v any) string {
	switch val := v.(type) {
	case nil:
		return "null"
	case bool:
		if val {
			return "b:1"
		}
		return "b:0"
	case int:
		return fmt.Sprintf("n:%d", val)
	case int8:
		return fmt.Sprintf("n:%d", val)
	case int16:
		return fmt.Sprintf("n:%d", val)
	case int32:
		return fmt.Sprintf("n:%d", val)
	case int64:
		return fmt.Sprintf("n:%d", val)
	case uint:
		return fmt.Sprintf("n:%d", val)
	case uint8:
		return fmt.Sprintf("n:%d", val)
	case uint16:
		return fmt.Sprintf("n:%d", val)
	case uint32:
		return fmt.Sprintf("n:%d", val)
	case uint64:
		return fmt.Sprintf("n:%d", val)
	case float32:
		return canonicalFloat(float64(val))
	case float64:
		return canonicalFloat(val)
	case string:
		return fmt.Sprintf("s:%q", val)
	case []byte:
		return fmt.Sprintf("x:%x", val)
	case time.Time:
		return fmt.Sprintf("t:%d", val.UTC().UnixNano())
	default:
		return fmt.Sprintf("v:%v", val)
	}
}

func canonicalFloat(f float64) string {
	if !math.IsNaN(f) && !math.IsInf(f, 0) && math.Trunc(f) == f && f >= -9007199254740992 && f <= 9007199254740992 {
		return fmt.Sprintf("n:%d", int64(f))
	}
	return fmt.Sprintf("f:%s", strconv.FormatFloat(f, 'g', -1, 64))
}

func equalValueSlices(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for idx := range a {
		if canonicalScalar(a[idx]) != canonicalScalar(b[idx]) {
			return false
		}
	}
	return true
}

func compareRowPayloads(a, b []any) int {
	minLen := min(len(a), len(b))
	for idx := range minLen {
		if cmp := compareVersionValues(a[idx], b[idx]); cmp != 0 {
			return cmp
		}
	}
	switch {
	case len(a) > len(b):
		return 1
	case len(a) < len(b):
		return -1
	default:
		return 0
	}
}

var commonTimestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	time.DateOnly,
}

func compareVersionValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	if ia, okA := asExactInt64(a); okA {
		if ib, okB := asExactInt64(b); okB {
			switch {
			case ia > ib:
				return 1
			case ia < ib:
				return -1
			default:
				return 0
			}
		}
	}

	if fa, okA := asFloat64(a); okA {
		if fb, okB := asFloat64(b); okB {
			switch {
			case fa > fb:
				return 1
			case fa < fb:
				return -1
			default:
				return 0
			}
		}
	}

	if ta, okA := asTime(a); okA {
		if tb, okB := asTime(b); okB {
			return ta.Compare(tb)
		}
	}

	return strings.Compare(scalarToString(a), scalarToString(b))
}

func asExactInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case uint:
		if uint64(val) <= math.MaxInt64 {
			return int64(val), true
		}
	case uint8:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		if val <= math.MaxInt64 {
			return int64(val), true
		}
	case float32:
		f := float64(val)
		if !math.IsNaN(f) && !math.IsInf(f, 0) && math.Trunc(f) == f {
			return int64(f), true
		}
	case float64:
		if !math.IsNaN(val) && !math.IsInf(val, 0) && math.Trunc(val) == val && val >= -9007199254740992 && val <= 9007199254740992 {
			return int64(val), true
		}
	case string:
		if n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64); err == nil {
			return n, true
		}
	case []byte:
		if n, err := strconv.ParseInt(strings.TrimSpace(string(val)), 10, 64); err == nil {
			return n, true
		}
	}
	return 0, false
}

func asFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, !math.IsNaN(val)
	case float32:
		f := float64(val)
		return f, !math.IsNaN(f)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		if i, ok := asExactInt64(val); ok {
			return float64(i), true
		}
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(val), 64); err == nil && !math.IsNaN(f) {
			return f, true
		}
	case []byte:
		if f, err := strconv.ParseFloat(strings.TrimSpace(string(val)), 64); err == nil && !math.IsNaN(f) {
			return f, true
		}
	}
	return 0, false
}

func asTime(v any) (time.Time, bool) {
	switch val := v.(type) {
	case time.Time:
		return val, true
	case string:
		return parseTimeString(val)
	case []byte:
		return parseTimeString(string(val))
	default:
		return time.Time{}, false
	}
}

func parseTimeString(s string) (time.Time, bool) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return time.Time{}, false
	}
	for _, layout := range commonTimestampLayouts {
		if t, err := time.Parse(layout, trimmed); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func scalarToString(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	case []byte:
		return string(val)
	case time.Time:
		return val.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(val)
	}
}

func extractSQLStatementTables(command string) []string {
	if strings.TrimSpace(command) == "" {
		return nil
	}
	stmts, err := Parse(context.Background(), command)
	if err != nil {
		return nil
	}
	var tables []string
	for _, stmt := range stmts {
		if stmt == nil || stmt.node == nil {
			continue
		}
		switch n := stmt.node.(type) {
		case *rqlitesql.CreateTableStatement:
			if n.Name != nil && n.Name.Name != "" {
				tables = append(tables, strings.ToLower(n.Name.Name))
			}
		case *rqlitesql.AlterTableStatement:
			if n.Name != nil && n.Name.Name != "" {
				tables = append(tables, strings.ToLower(n.Name.Name))
			}
			if n.NewName != nil && n.NewName.Name != "" {
				tables = append(tables, strings.ToLower(n.NewName.Name))
			}
		case *rqlitesql.DropTableStatement:
			if n.Name != nil && n.Name.Name != "" {
				tables = append(tables, strings.ToLower(n.Name.Name))
			}
		case *rqlitesql.CreateIndexStatement:
			if n.Table != nil && n.Table.Name != "" {
				tables = append(tables, strings.ToLower(n.Table.Name))
			}
		case *rqlitesql.InsertStatement:
			if n.Table != nil && n.Table.Name != "" {
				tables = append(tables, strings.ToLower(n.Table.Name))
			}
		case *rqlitesql.UpdateStatement:
			if n.Table != nil && n.Table.Name != nil && n.Table.Name.Name != "" {
				tables = append(tables, strings.ToLower(n.Table.Name.Name))
			}
		case *rqlitesql.DeleteStatement:
			if n.Table != nil && n.Table.Name != nil && n.Table.Name.Name != "" {
				tables = append(tables, strings.ToLower(n.Table.Name.Name))
			}
		}
	}
	return tables
}

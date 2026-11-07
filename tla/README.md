# Formal TLA+ Verification Suite for `go-ha` Strategies

This directory contains formal **TLA+ specifications** and **TLC model checker configurations** for the core replication, consensus, conflict resolution, and change-capture strategies implemented in `go-ha`.

## Overview of Specifications

| Specification | Config File(s) | `go-ha` Strategy / Subsystem Modeled | Source Files Mapped |
| :--- | :--- | :--- | :--- |
| [`TwoPhaseCommit.tla`](./TwoPhaseCommit.tla) | [`TwoPhaseCommit.cfg`](./TwoPhaseCommit.cfg) | Synchronous Two-Phase Commit (2PC), durable coordinator decisions (`ha_2pc_decisions`), worker idempotency (`ha_2pc_transactions`), stream disconnect rollback, and startup crash recovery | `publisher.go`, `sqlite_conn.go`, `connect/service.go`, `connector.go` |
| [`LeaderBasedReplication.tla`](./LeaderBasedReplication.tla) | [`LeaderBasedReplication.cfg`](./LeaderBasedReplication.cfg) | Async/Sync Leader-Follower replication, Static & NATS KV Dynamic Leader Election (TTL expiry & re-creation window), Read-Your-Writes (`ConsistentReaderFunc`), and follower write forwarding | `leader.go`, `http.go`, `sqlite_conn.go`, `subscriber.go`, `connect/service.go` |
| [`LeaderlessReplication.tla`](./LeaderlessReplication.tla) | [`LeaderlessReplication.cfg`](./LeaderlessReplication.cfg)<br>[`LeaderlessReplication_NoInterceptor.cfg`](./LeaderlessReplication_NoInterceptor.cfg) | Multi-Master / Leaderless replication, durable transactional Outbox (`ha_outbox`), SHA-256 content-hash deduplication, self-skip vs. `ChangeSetInterceptor` conflict resolution, and snapshot bootstrapping | `publisher.go`, `subscriber.go`, `changeset.go`, `snapshotter.go`, `sqlite_conn.go` |
| [`RowIdentifyStrategies.tla`](./RowIdentifyStrategies.tla) | [`RowIdentifyStrategies.cfg`](./RowIdentifyStrategies.cfg) | ChangeSet row identification strategies (`pk`, `rowid`, `full`), `UPDATE -> INSERT` zero-row fallback, and historical transaction reversal (`UNDO`, `UNDOE`, `UNDOT`) | `changeset.go`, `option.go`, `sqlite_hooks.go`, `sqlite_conn.go` |

---

## Running the Model Checker

Run the verification suite via [`verify.sh`](./verify.sh) (requires `java` 11+ and `curl` to auto-fetch `tla2tools.jar` on first run):

```bash
./verify.sh
```

To verify a single specification:

```bash
./verify.sh TwoPhaseCommit
```

To run the leaderless counterexample model demonstrating why a conflict-resolution `ChangeSetInterceptor` is necessary when concurrent multi-master writes touch the same row:

```bash
java -XX:-UseContainerSupport -XX:+UseParallelGC -cp .cache/tla2tools.jar \
  tlc2.TLC -cleanup -workers auto \
  -config LeaderlessReplication_NoInterceptor.cfg LeaderlessReplication.tla
```

---

## Detailed Strategy Models & Verified Properties

### 1. Two-Phase Commit & Crash Recovery (`TwoPhaseCommit.tla`)

Models `GrpcPublisher.Publish` (`publisher.go`), `conn.Commit` (`sqlite_conn.go`), and `ReplicationServer.Replicate` (`connect/service.go`):

1. **Phase 1 (Prepare)**:
   - Coordinator opens bidi gRPC streams to all workers (`ReplicationTopology`), sends `PREPARE`, and waits for `OK` or `ERROR`.
   - Each worker begins a SQLite transaction, checks `ha_2pc_transactions` for idempotency (`state = 'COMMITTED'`), executes `cs.Prepare`, and records `state = 'PREPARED'`.
   - If the gRPC stream closes before `COMMIT` arrives, `defer tx.Rollback()` in `connect/service.go` rolls back both the prepared SQL changes and the `'PREPARED'` record.
2. **Coordinator Local Commit & Durable Decision**:
   - Only if all workers vote `OK` does the coordinator invoke `RecordCommitDecision(ctx, tx.Tx, txID, csBytes)` inside the **same** local SQLite transaction (`sqlite_conn.go:258-281`) and commit locally.
   - If any worker fails/times out during `PREPARE` or if the local SQLite commit fails, the coordinator broadcasts `ABORT` and rolls back its local transaction.
3. **Phase 2 (Commit) & Startup Crash Recovery**:
   - Once the local transaction and `ha_2pc_decisions` row are committed, new local writes are blocked (`HasPendingDecisions`) if any worker fails to acknowledge `COMMIT`.
   - `RecoverPendingDecisions` (`publisher.go:503-576`) replays `COMMIT` with the stored `ChangeSet` bytes (`tx == nil` branch in `connect/service.go:809-844`) until all workers acknowledge, then deletes the `ha_2pc_decisions` row.

**Verified Properties**:
- `TypeOK`: Type safety across coordinator and worker state machines.
- `DecisionAtomicity`: The coordinator's local SQLite commit and `ha_2pc_decisions` record are strictly atomic.
- `NoCommittedWorkerWithoutCoordinatorDecision`: No worker ever commits a transaction unless the coordinator durably committed it.
- `WorkerIdempotencyTableConsistency`: Every worker's `ha_2pc_transactions` state matches its applied data state (exact-once execution across retries).
- `NoNewWriteWhileRecoveryPending`: New local writes cannot enter 2PC while `ha_2pc_decisions` is non-empty.
- `AbortedTransactionsClean`: Aborted transactions leave no committed state on the coordinator or any worker.
- `ClusterConsistencyWhenQuiescent`: Once recovery completes and no transaction is in-flight, all worker databases equal the coordinator database.
- `EventualCommitCompletion` (Temporal Liveness): Under weak fairness of protocol steps and strong fairness of recovery retries, every durably decided transaction eventually commits on all workers and clears `ha_2pc_decisions`.

---

### 2. Leader-Based Replication, Election & Read-Your-Writes (`LeaderBasedReplication.tla`)

Models `StaticLeaderElection` and `NatsLeaderElection` (`leader.go`), `ConsistentReaderFunc` (`http.go`), and `ReplicationInterceptor` (`sqlite_conn.go`):

1. **Leader Election (`leader.go`)**:
   - Models Graft leader election (`GraftElectLeader`), NATS KV redirect key lease renewal (`TickerLeaderRefreshKV` via `kv.PutString`), follower KV polling (`TickerFollowerRefreshSuccess` / `TickerFollowerRetryFailure`), and KV TTL expiration (`ExpireKVRedirect`).
   - Allows Graft to elect a new leader even before its NATS subscriber has caught up (`latestSeq[n] < Len(stream)`).
2. **Replication Stream & Follower Catch-up (`publisher.go`, `subscriber.go`)**:
   - Models the ordered replication stream (`stream`), per-node published sequence (`pubSeq[n]` = `c.PubSeq()`), and per-node applied subscriber sequence (`latestSeq[n]` = `c.LatestSeq()`).
3. **Read-Your-Writes (`http.go:82-146`)**:
   - Clients receive a monotonically increasing `_txseq` cookie on write (`c.PubSeq()`).
   - On subsequent reads with `_txseq`, `ConsistentReaderFunc` waits until `max(c.LatestSeq(), c.PubSeq()) >= txSeq` (`MaxNodeSeq(n) >= clientTxSeq`), ensuring both followers and newly elected leaders after failover have applied `txSeq` before serving reads.

**Verified Properties**:
- `TypeOK`: Structural type invariant.
- `AtMostOneActiveLeader`: Mutual exclusion of the NATS KV leader lease (`Cardinality(ActiveLeaders) <= 1`).
- `PrefixConsistency`: Every node's local database log is always a strict prefix of the global replication stream.
- `LeaderCompleteness`: Any active leader has applied all prior committed replication entries.
- `ReadYourWritesConsistency`: Every read served with `tx_seq` observes a database state at least as recent as `tx_seq`.
- `EventualReplicationCatchup` & `EventualLeaderTargetConvergence` (Temporal Liveness): All nodes eventually catch up to the replication stream and converge on the active leader target.

---

### 3. Leaderless Replication, Outbox, Deduplication & Snapshots (`LeaderlessReplication.tla`)

Models multi-master operation (`publisher.go`, `subscriber.go`, `changeset.go`, `snapshotter.go`):

1. **Durable Transactional Outbox (`publisher.go`)**:
   - When `EnableOutbox` is active or initial publish fails, the serialized `ChangeSet` is persisted in `ha_outbox` inside the local SQLite transaction and drained asynchronously to NATS JetStream.
2. **SHA-256 Content-Hash Deduplication (`publisher.go:108`, `subscriber.go:577`)**:
   - Identical `ChangeSet` payloads share a deterministic hash (`Nats-Msg-Id` & in-memory `dedupCache`), preventing duplicate delivery or re-application from outbox retries.
3. **Self-Skip vs. Conflict Interceptor (`subscriber.go:570`, `changeset.go:99`)**:
   - Every node skips messages originating from itself (`cs.Node == s.node && cs.ProcessID == processID`).
   - Compares `ConflictPolicy = "DefaultNoInterceptor"` against `ConflictPolicy = "InterceptorLWW"` (using `ChangeSetInterceptor` with version/timestamp comparison).
4. **Snapshot Bootstrap (`snapshotter.go`, `subscriber.go:DownloadSnapshot`)**:
   - A node resetting or joining downloads a consistent snapshot `(snapDB, snapSeq)` and resumes stream consumption from `snapSeq + 1`.

**Verified Properties**:
- `TypeOK`: Type safety across outbox, JetStream, deduplication caches, and node states.
- `OutboxDurability`: Every locally committed transaction exists either in the node's local `ha_outbox` or in the NATS JetStream log.
- `ExactOncePerOriginTx`: Content-hash deduplication and sequence tracking guarantee each origin transaction is applied at most once per node.
- `StrongEventualConvergenceWhenQuiescent`:
  - **Passes** under `LeaderlessReplication.cfg` (`ConflictPolicy = "InterceptorLWW"`): all nodes converge to identical database state once outboxes and streams drain.
  - **Produces Counterexample** under `LeaderlessReplication_NoInterceptor.cfg` (`ConflictPolicy = "DefaultNoInterceptor"`): proves that when two nodes concurrently update the same key without a conflict-resolving `ChangeSetInterceptor`, self-skipping causes Node 1 to end with Node 2's value while Node 2 ends with Node 1's value.

---

### 4. Row Identification, `UPDATE->INSERT` Fallback & Undo Strategies (`RowIdentifyStrategies.tla`)

Models `ChangeSet.Prepare` and `rowIdentifyStrategy` implementations (`changeset.go:139-156, 260-425`), plus historical undo (`sqlite_conn.go:512-662`):

1. **Row Identification Modes (`RowIdentify`)**:
   - `pk` (`pkIdentifyStrategy`): matches rows by primary key (`WHERE id = ?`).
   - `rowid` (`rowidIdentifyStrategy`): matches rows by SQLite `rowid` (`WHERE rowid = ?`).
   - `full` (`fullIdentifyStrategy`): matches rows by all columns (`WHERE id = ? AND val = ?`).
2. **`UPDATE -> INSERT` Zero-Row Fallback (`changeset.go:142-151`)**:
   - If an `UPDATE` statement affects `0` rows on a replica, `ChangeSet.Prepare` falls back to `ToSQL(change.AsInsert())`.
   - By contrast, `DELETE` has no fallback if `0` rows match.
3. **Undo Commands (`UNDO`, `UNDOE`, `UNDOT`)**:
   - `Invert()` swaps `INSERT <-> DELETE`, reverses `UPDATE` (`OldValues <-> NewValues`), and reverses change slice order.
   - `UNDO [n]` / `UNDOT <seq>` reverts the entire suffix of transactions back to a target sequence; `UNDOE <entity_id> [n]` reverts changes scoped to a single entity key without disturbing other keys.

**Verified Properties**:
- `TypeOK`: Type safety across all replica strategies and history logs.
- `AllStrategiesMatchOrigin`: Without external replica drift, `pk`, `rowid`, and `full` replicas remain 100% identical to the origin database across arbitrary `INSERT`, `UPDATE`, and `DELETE` sequences.
- `UpdateFallbackSelfHealingAndFullDeleteContrast`: Proves that if a non-PK column drifts on a replica:
  - An `UPDATE` self-heals on `pk`, `rowid`, and `full` (0 rows matched -> `INSERT` fallback via `REPLACE INTO` / `INSERT ... ON CONFLICT DO UPDATE`).
  - A `DELETE` succeeds on `pk` (`WHERE id = ?`) and `rowid` (`WHERE rowid = ?`), but silently matches 0 rows on `full` (`WHERE id = ? AND val = old_val`), leaving the drifted row behind.
- `UndoByAnySeqRestoresSnapshot`: Rolling back any suffix of committed history (`UNDO` / `UNDOT`) restores the exact historical database snapshot at that sequence.
- `EntityUndoIsolation`: Rolling back a single entity (`UNDOE`) restores that key's state while leaving all other keys untouched.

---

## Key Formal Verification Insights & Code Fixes

1. **Leaderless Concurrent Write Divergence without `ChangeSetInterceptor`** (`subscriber.go:570`):
   Because `NATSSubscriber.handler` ignores messages originating from the local node (`cs.Node == s.node && cs.ProcessID == processID`), concurrent writes to the same key on two nodes $A$ and $B$ are applied in opposite orders ($A$ applies $A$ locally then $B$ from NATS; $B$ applies $B$ locally then $A$ from NATS). Running TLC with `LeaderlessReplication_NoInterceptor.cfg` generates the minimal 6-step counterexample trace. Supplying a deterministic LWW/version `ChangeSetInterceptor` (`LeaderlessReplication.cfg`) restores strong eventual convergence.
2. **NATS KV Leader Lease Renewal & Failover Read-Your-Writes** (`leader.go:157-179`, `http.go:82-146`, `sqlite_conn.go:293-314`):
   - `StartLeaderElection` (`leader.go`) now refreshes `redirectKey` via `kv.PutString` on every 3-second tick while `node.State() == graft.LEADER`, resetting the 1-minute NATS KV TTL so the redirect key does not periodically expire while the leader is active.
   - `ConsistentReaderFunc` (`http.go`) now checks `max(c.LatestSeq(), c.PubSeq()) >= txSeq` instead of unconditionally bypassing the wait when `IsLeader()` is true, ensuring a newly elected leader after failover waits to catch up to `txSeq` before serving reads.
3. **SQL Placeholder Formatting in `rowidIdentifyStrategy.ToSQL`** (`changeset.go:405-420`):
   Fixed the `INSERT ... ON CONFLICT DO UPDATE SET` clause in `rowidIdentifyStrategy.ToSQL` to format each column with numbered positional parameters (`%s = ?%d`) and `` `rowid` = ?%d `` rather than `strings.Join(change.Columns, " = ?, ")`, which previously omitted `= ?` on the final column and mixed numbered and unnumbered SQLite placeholders.

--------------------------- MODULE TwoPhaseCommit ---------------------------
(***************************************************************************)
(* Formal TLA+ Specification of go-ha's Two-Phase Commit (2PC) Strategy    *)
(*                                                                         *)
(* Models the exact protocol implemented in:                               *)
(*   - publisher.go: TwoPhaseCommitPublisher, preparedTwoPhaseCommit,      *)
(*                   recoverLocalDecisions                                 *)
(*   - sqlite_conn.go: txLocal.Commit, execLocalTwoPhase, recoverTwoPhase  *)
(*   - changeset.go: ChangeSet.Prepare                                     *)
(*   - connect/service.go: Service.ChangeSet                               *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Workers,        \* Set of remote 2PC worker nodes (2pcPeers)
    Transactions    \* Set of transaction IDs to execute

VARIABLES
    coordPhase,     \* Coordinator protocol state for active transaction
    activeTx,       \* Currently active transaction ID on coordinator (or "None")
    coordDB,        \* Set of transaction IDs committed in coordinator's local SQLite DB
    decisions,      \* Set of transaction IDs in coordinator's ha_2pc_decisions table
    workerStreamTx, \* [w \in Workers -> TxID \cup {"None"}] open prepared tx on active gRPC stream
    workerTxTable,    \* [w \in Workers -> [Transactions -> {"NONE", "PREPARED", "COMMITTED"}]]
                      \* Models ha_2pc_transactions table on each worker
    workerDB,         \* [w \in Workers -> SUBSET Transactions] committed data on each worker
    workerApplyCount, \* [w \in Workers -> [Transactions -> Nat]] number of times tx committed on worker
    txHistory         \* [t \in Transactions -> {"IDLE", "ACTIVE", "COMMITTED_LOCAL", "COMPLETED", "ABORTED"}]

vars == <<coordPhase, activeTx, coordDB, decisions, workerStreamTx, workerTxTable, workerDB, workerApplyCount, txHistory>>

CoordPhases == {
    "IDLE",
    "RECOVERING",
    "LOCAL_EXEC",
    "PREPARING",
    "PREPARED_ALL",
    "DECISION_RECORDED",
    "COMMITTING_WORKERS",
    "RESOLVING_LOCAL",
    "ABORTING_WORKERS",
    "PENDING_RECOVERY"
}

WorkerTxStates == {"NONE", "PREPARED", "COMMITTED"}
TxHistoryStates == {"IDLE", "ACTIVE", "COMMITTED_LOCAL", "COMPLETED", "ABORTED"}

TypeOK ==
    /\ coordPhase \in CoordPhases
    /\ activeTx \in Transactions \cup {"None"}
    /\ coordDB \subseteq Transactions
    /\ decisions \subseteq Transactions
    /\ workerStreamTx \in [Workers -> Transactions \cup {"None"}]
    /\ workerTxTable \in [Workers -> [Transactions -> WorkerTxStates]]
    /\ workerDB \in [Workers -> SUBSET Transactions]
    /\ workerApplyCount \in [Workers -> [Transactions -> 0..2]]
    /\ txHistory \in [Transactions -> TxHistoryStates]

Init ==
    /\ coordPhase = "IDLE"
    /\ activeTx = "None"
    /\ coordDB = {}
    /\ decisions = {}
    /\ workerStreamTx = [w \in Workers |-> "None"]
    /\ workerTxTable = [w \in Workers |-> [t \in Transactions |-> "NONE"]]
    /\ workerDB = [w \in Workers |-> {}]
    /\ workerApplyCount = [w \in Workers |-> [t \in Transactions |-> 0]]
    /\ txHistory = [t \in Transactions |-> "IDLE"]

-----------------------------------------------------------------------------
(* Recovery Phase: recoverTwoPhase / RecoverTwoPhaseCommits (publisher.go) *)
(* Runs on startup (BindLocalDB) and before every new local transaction.   *)
(* Blocks new local writes until ha_2pc_decisions is empty.                *)
-----------------------------------------------------------------------------

StartRecovery ==
    /\ coordPhase \in {"IDLE", "PENDING_RECOVERY"}
    /\ decisions /= {}
    /\ coordPhase' = "RECOVERING"
    /\ UNCHANGED <<activeTx, coordDB, decisions, workerStreamTx, workerTxTable, workerDB, workerApplyCount, txHistory>>

(* In recoverLocalDecisions (publisher.go:539-569):                        *)
(* Coordinator queries worker STATUS; if not COMMITTED, sends COMMIT.      *)
(* On worker (connect/service.go:809-844): if tx == nil and state !=       *)
(* COMMITTED, replays worker.Prepare(ctx, db), updates state='COMMITTED',  *)
(* and commits worker SQLite transaction.                                  *)
RecoverWorker(t, w) ==
    /\ coordPhase = "RECOVERING"
    /\ t \in decisions
    /\ workerTxTable[w][t] /= "COMMITTED"
    /\ workerStreamTx' = [workerStreamTx EXCEPT ![w] = "None"]
    /\ workerTxTable' = [workerTxTable EXCEPT ![w][t] = "COMMITTED"]
    /\ workerDB' = [workerDB EXCEPT ![w] = @ \cup {t}]
    /\ workerApplyCount' = [workerApplyCount EXCEPT ![w][t] = @ + 1]
    /\ UNCHANGED <<coordPhase, activeTx, coordDB, decisions, txHistory>>

(* Also model idempotent COMMIT replay when worker already committed       *)
(* (connect/service.go:811-815: state == "COMMITTED" returns immediately). *)
RecoverWorkerAlreadyCommitted(t, w) ==
    /\ coordPhase = "RECOVERING"
    /\ t \in decisions
    /\ workerTxTable[w][t] = "COMMITTED"
    /\ UNCHANGED vars

(* Once all workers have state = COMMITTED for t, coordinator deletes t    *)
(* from ha_2pc_decisions (publisher.go:571-573).                           *)
CompleteRecoveryForTx(t) ==
    /\ coordPhase = "RECOVERING"
    /\ t \in decisions
    /\ \A w \in Workers : workerTxTable[w][t] = "COMMITTED"
    /\ decisions' = decisions \ {t}
    /\ txHistory' = [txHistory EXCEPT ![t] = "COMPLETED"]
    /\ coordPhase' = IF decisions \ {t} = {} THEN "IDLE" ELSE "RECOVERING"
    /\ UNCHANGED <<activeTx, coordDB, workerStreamTx, workerTxTable, workerDB, workerApplyCount>>

(* Recovery can fail transiently if a worker is unreachable, leaving the   *)
(* coordinator in PENDING_RECOVERY and blocking new writes.                *)
FailRecovery ==
    /\ coordPhase = "RECOVERING"
    /\ decisions /= {}
    /\ coordPhase' = "PENDING_RECOVERY"
    /\ UNCHANGED <<activeTx, coordDB, decisions, workerStreamTx, workerTxTable, workerDB, workerApplyCount, txHistory>>

-----------------------------------------------------------------------------
(* Normal Transaction Execution & Phase 1: Prepare (publisher.go:347-400)  *)
-----------------------------------------------------------------------------

(* BeginTx / execLocalTwoPhase requires recoverTwoPhase to succeed first   *)
(* (i.e., decisions = {}).                                                 *)
BeginLocalTx(t) ==
    /\ coordPhase = "IDLE"
    /\ decisions = {}
    /\ activeTx = "None"
    /\ txHistory[t] = "IDLE"
    /\ activeTx' = t
    /\ coordPhase' = "LOCAL_EXEC"
    /\ txHistory' = [txHistory EXCEPT ![t] = "ACTIVE"]
    /\ UNCHANGED <<coordDB, decisions, workerStreamTx, workerTxTable, workerDB, workerApplyCount>>

StartPrepare ==
    /\ coordPhase = "LOCAL_EXEC"
    /\ activeTx /= "None"
    /\ coordPhase' = "PREPARING"
    /\ UNCHANGED <<activeTx, coordDB, decisions, workerStreamTx, workerTxTable, workerDB, workerApplyCount, txHistory>>

(* Worker receives CHANGESET_REQUEST_TYPE_PREPARE (connect/service.go:764) *)
(* Begins SQLite tx, inserts (t, 'PREPARED') into ha_2pc_transactions,     *)
(* applies changes in uncommitted tx.                                      *)
WorkerPrepareSuccess(w) ==
    /\ coordPhase = "PREPARING"
    /\ activeTx /= "None"
    /\ workerStreamTx[w] = "None"
    /\ workerTxTable[w][activeTx] = "NONE"
    /\ workerStreamTx' = [workerStreamTx EXCEPT ![w] = activeTx]
    /\ workerTxTable' = [workerTxTable EXCEPT ![w][activeTx] = "PREPARED"]
    /\ UNCHANGED <<coordPhase, activeTx, coordDB, decisions, workerDB, workerApplyCount, txHistory>>

(* If any worker fails during PrepareTwoPhaseCommit, coordinator transitions*)
(* to ABORTING_WORKERS to abort already-prepared workers and rollback.     *)
PrepareFailed ==
    /\ coordPhase = "PREPARING"
    /\ activeTx /= "None"
    /\ coordPhase' = "ABORTING_WORKERS"
    /\ UNCHANGED <<activeTx, coordDB, decisions, workerStreamTx, workerTxTable, workerDB, workerApplyCount, txHistory>>

AllWorkersPrepared ==
    /\ coordPhase = "PREPARING"
    /\ activeTx /= "None"
    /\ \A w \in Workers :
        /\ workerStreamTx[w] = activeTx
        /\ workerTxTable[w][activeTx] = "PREPARED"
    /\ coordPhase' = "PREPARED_ALL"
    /\ UNCHANGED <<activeTx, coordDB, decisions, workerStreamTx, workerTxTable, workerDB, workerApplyCount, txHistory>>

-----------------------------------------------------------------------------
(* Coordinator Local Decision & Commit (sqlite_conn.go:774-810)            *)
-----------------------------------------------------------------------------

(* RecordCommitDecision writes to ha_2pc_decisions inside the coordinator  *)
(* local SQLite transaction tx.Tx (sqlite_conn.go:776-786).                *)
RecordDecisionSuccess ==
    /\ coordPhase = "PREPARED_ALL"
    /\ activeTx /= "None"
    /\ coordPhase' = "DECISION_RECORDED"
    /\ UNCHANGED <<activeTx, coordDB, decisions, workerStreamTx, workerTxTable, workerDB, workerApplyCount, txHistory>>

(* If RecordCommitDecision fails, coordinator aborts prepared workers and  *)
(* rolls back local tx (sqlite_conn.go:788-794).                           *)
RecordDecisionFailure ==
    /\ coordPhase = "PREPARED_ALL"
    /\ activeTx /= "None"
    /\ coordPhase' = "ABORTING_WORKERS"
    /\ UNCHANGED <<activeTx, coordDB, decisions, workerStreamTx, workerTxTable, workerDB, workerApplyCount, txHistory>>

(* tx.Tx.Commit() commits BOTH the local data changes AND the row in       *)
(* ha_2pc_decisions atomically in SQLite (sqlite_conn.go:795).             *)
CommitLocalSuccess ==
    /\ coordPhase = "DECISION_RECORDED"
    /\ activeTx /= "None"
    /\ coordDB' = coordDB \cup {activeTx}
    /\ decisions' = decisions \cup {activeTx}
    /\ txHistory' = [txHistory EXCEPT ![activeTx] = "COMMITTED_LOCAL"]
    /\ coordPhase' = "COMMITTING_WORKERS"
    /\ UNCHANGED <<activeTx, workerStreamTx, workerTxTable, workerDB, workerApplyCount>>

(* If tx.Tx.Commit() returns an error, SQLite either rolled back both or   *)
(* committed both; coordinator calls ResolveCommit (sqlite_conn.go:797-810)*)
CommitLocalAmbiguousFail ==
    /\ coordPhase = "DECISION_RECORDED"
    /\ activeTx /= "None"
    /\ coordPhase' = "RESOLVING_LOCAL"
    /\ UNCHANGED <<activeTx, coordDB, decisions, workerStreamTx, workerTxTable, workerDB, workerApplyCount, txHistory>>

CommitLocalAmbiguousCommitted ==
    /\ coordPhase = "DECISION_RECORDED"
    /\ activeTx /= "None"
    /\ coordDB' = coordDB \cup {activeTx}
    /\ decisions' = decisions \cup {activeTx}
    /\ txHistory' = [txHistory EXCEPT ![activeTx] = "COMMITTED_LOCAL"]
    /\ coordPhase' = "RESOLVING_LOCAL"
    /\ UNCHANGED <<activeTx, workerStreamTx, workerTxTable, workerDB, workerApplyCount>>

(* ResolveCommit checks SELECT 1 FROM ha_2pc_decisions (publisher.go:460)  *)
ResolveCommitFound ==
    /\ coordPhase = "RESOLVING_LOCAL"
    /\ activeTx \in decisions
    /\ coordPhase' = "COMMITTING_WORKERS"
    /\ UNCHANGED <<activeTx, coordDB, decisions, workerStreamTx, workerTxTable, workerDB, workerApplyCount, txHistory>>

ResolveCommitNotFound ==
    /\ coordPhase = "RESOLVING_LOCAL"
    /\ activeTx \notin decisions
    /\ coordPhase' = "ABORTING_WORKERS"
    /\ UNCHANGED <<activeTx, coordDB, decisions, workerStreamTx, workerTxTable, workerDB, workerApplyCount, txHistory>>

-----------------------------------------------------------------------------
(* Phase 2: Commit or Abort Workers (publisher.go:424-458, service.go:808) *)
-----------------------------------------------------------------------------

(* Worker receives CHANGESET_REQUEST_TYPE_COMMIT on active stream:         *)
(* Updates ha_2pc_transactions to 'COMMITTED' and commits SQLite tx.       *)
WorkerCommitSuccess(w) ==
    /\ coordPhase = "COMMITTING_WORKERS"
    /\ activeTx /= "None"
    /\ workerTxTable[w][activeTx] /= "COMMITTED"
    /\ workerStreamTx' = [workerStreamTx EXCEPT ![w] = "None"]
    /\ workerTxTable' = [workerTxTable EXCEPT ![w][activeTx] = "COMMITTED"]
    /\ workerDB' = [workerDB EXCEPT ![w] = @ \cup {activeTx}]
    /\ workerApplyCount' = [workerApplyCount EXCEPT ![w][activeTx] = @ + 1]
    /\ UNCHANGED <<coordPhase, activeTx, coordDB, decisions, txHistory>>

(* All workers committed: coordinator deletes decision from                *)
(* ha_2pc_decisions and finishes (publisher.go:445-451).                   *)
FinalizeCommit ==
    /\ coordPhase = "COMMITTING_WORKERS"
    /\ activeTx /= "None"
    /\ \A w \in Workers : workerTxTable[w][activeTx] = "COMMITTED"
    /\ decisions' = decisions \ {activeTx}
    /\ txHistory' = [txHistory EXCEPT ![activeTx] = "COMPLETED"]
    /\ activeTx' = "None"
    /\ coordPhase' = "IDLE"
    /\ UNCHANGED <<coordDB, workerStreamTx, workerTxTable, workerDB, workerApplyCount>>

(* If a worker stream fails during Phase 2 Commit:                         *)
(* tx.close() closes streams; any worker whose stream closes while still   *)
(* PREPARED rolls back its uncommitted SQLite tx (connect/service.go:673), *)
(* reverting its ha_2pc_transactions entry to NONE.                        *)
(* Coordinator keeps decision in ha_2pc_decisions and returns              *)
(* ErrTwoPhaseCommitPending (sqlite_conn.go:817-819).                      *)
WorkerCommitStreamFailure ==
    /\ coordPhase = "COMMITTING_WORKERS"
    /\ activeTx /= "None"
    /\ \E w \in Workers : workerTxTable[w][activeTx] /= "COMMITTED"
    /\ workerStreamTx' = [w \in Workers |-> "None"]
    /\ workerTxTable' = [w \in Workers |->
        [workerTxTable[w] EXCEPT ![activeTx] =
            IF @ = "PREPARED" THEN "NONE" ELSE @]]
    /\ activeTx' = "None"
    /\ coordPhase' = "PENDING_RECOVERY"
    /\ UNCHANGED <<coordDB, decisions, workerDB, workerApplyCount, txHistory>>

(* Abort Prepared Workers: coordinator sends ABORT and closes streams,     *)
(* rolling back any PREPARED worker transactions (publisher.go:478-491).   *)
AbortWorkersAndRollbackLocal ==
    /\ coordPhase = "ABORTING_WORKERS"
    /\ activeTx /= "None"
    /\ workerStreamTx' = [w \in Workers |-> "None"]
    /\ workerTxTable' = [w \in Workers |->
        [workerTxTable[w] EXCEPT ![activeTx] =
            IF @ = "PREPARED" THEN "NONE" ELSE @]]
    /\ txHistory' = [txHistory EXCEPT ![activeTx] = "ABORTED"]
    /\ activeTx' = "None"
    /\ coordPhase' = "IDLE"
    /\ UNCHANGED <<coordDB, decisions, workerDB, workerApplyCount>>

(* Terminal stuttering step once all bounded transactions have finished.   *)
(* Ensures TLC still checks for deadlocks in any non-terminal state.       *)
Terminated ==
    /\ coordPhase = "IDLE"
    /\ decisions = {}
    /\ \A t \in Transactions : txHistory[t] \in {"COMPLETED", "ABORTED"}
    /\ UNCHANGED vars

Next ==
    \/ StartRecovery
    \/ \E t \in Transactions, w \in Workers : RecoverWorker(t, w)
    \/ \E t \in Transactions, w \in Workers : RecoverWorkerAlreadyCommitted(t, w)
    \/ \E t \in Transactions : CompleteRecoveryForTx(t)
    \/ FailRecovery
    \/ \E t \in Transactions : BeginLocalTx(t)
    \/ StartPrepare
    \/ \E w \in Workers : WorkerPrepareSuccess(w)
    \/ PrepareFailed
    \/ AllWorkersPrepared
    \/ RecordDecisionSuccess
    \/ RecordDecisionFailure
    \/ CommitLocalSuccess
    \/ CommitLocalAmbiguousFail
    \/ CommitLocalAmbiguousCommitted
    \/ ResolveCommitFound
    \/ ResolveCommitNotFound
    \/ \E w \in Workers : WorkerCommitSuccess(w)
    \/ FinalizeCommit
    \/ WorkerCommitStreamFailure
    \/ AbortWorkersAndRollbackLocal
    \/ Terminated

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety Invariants                                                       *)
-----------------------------------------------------------------------------

(* 1. Atomic Local Decision Invariant:                                     *)
(* A transaction is in coordinator's local DB iff it is either in the      *)
(* durable ha_2pc_decisions table (awaiting completion/recovery) or has    *)
(* already completed on all workers.                                       *)
DecisionAtomicity ==
    \A t \in Transactions :
        (t \in coordDB) <=> (t \in decisions \/ txHistory[t] = "COMPLETED")

(* 2. No Unilateral Worker Commit Invariant:                               *)
(* A worker never commits a transaction unless the coordinator has         *)
(* durably committed it locally (and recorded the decision).               *)
NoCommittedWorkerWithoutCoordinatorDecision ==
    \A w \in Workers, t \in Transactions :
        (t \in workerDB[w] \/ workerTxTable[w][t] = "COMMITTED") => (t \in coordDB)

(* 3. Worker Table & Exact-Once Idempotency Invariant:                     *)
(* A worker's ha_2pc_transactions table marks a transaction COMMITTED iff  *)
(* that transaction's changes are committed in the worker's database, and  *)
(* no transaction is ever committed more than once on any worker.          *)
WorkerIdempotencyTableConsistency ==
    \A w \in Workers, t \in Transactions :
        /\ (workerTxTable[w][t] = "COMMITTED") <=> (t \in workerDB[w])
        /\ (workerTxTable[w][t] = "COMMITTED") <=> (workerApplyCount[w][t] = 1)
        /\ (workerTxTable[w][t] /= "COMMITTED") <=> (workerApplyCount[w][t] = 0)

(* 4. Recovery Gate Invariant (TwoPhaseCommitRecoveryFailureBlocksNewWrites)*)
(* Coordinator never executes a new local transaction while a prior        *)
(* transaction's decision is still pending in ha_2pc_decisions.            *)
NoNewWriteWhileRecoveryPending ==
    (coordPhase \in {"LOCAL_EXEC", "PREPARING", "PREPARED_ALL", "DECISION_RECORDED"})
        => (decisions = {})

(* 5. Aborted Transactions Never Leak Invariant:                           *)
(* An aborted transaction is never present in the coordinator DB,          *)
(* ha_2pc_decisions, or any worker DB.                                     *)
AbortedTransactionsClean ==
    \A t \in Transactions :
        (txHistory[t] = "ABORTED") =>
            /\ t \notin coordDB
            /\ t \notin decisions
            /\ \A w \in Workers : t \notin workerDB[w] /\ workerTxTable[w][t] = "NONE"

(* 6. Global Cluster Consistency When Quiescent:                           *)
(* Whenever no transaction is in flight and ha_2pc_decisions is empty,     *)
(* every worker's committed database state is identical to the             *)
(* coordinator's committed database state.                                 *)
ClusterConsistencyWhenQuiescent ==
    (coordPhase = "IDLE" /\ decisions = {}) =>
        \A w \in Workers : workerDB[w] = coordDB

-----------------------------------------------------------------------------
(* Liveness / Temporal Properties (under weak fairness of recovery/commit) *)
-----------------------------------------------------------------------------

Fairness ==
    /\ WF_vars(StartRecovery)
    /\ \A t \in Transactions, w \in Workers : SF_vars(RecoverWorker(t, w))
    /\ \A t \in Transactions : SF_vars(CompleteRecoveryForTx(t))
    /\ \A w \in Workers : WF_vars(WorkerCommitSuccess(w))
    /\ WF_vars(FinalizeCommit)
    /\ WF_vars(ResolveCommitFound)
    /\ WF_vars(ResolveCommitNotFound)
    /\ WF_vars(AbortWorkersAndRollbackLocal)

FairSpec == Spec /\ Fairness

(* Every transaction that commits locally on the coordinator eventually    *)
(* commits on all workers and clears its pending decision.                 *)
EventualCommitCompletion ==
    \A t \in Transactions :
        (t \in coordDB) ~> (txHistory[t] = "COMPLETED" /\ \A w \in Workers : t \in workerDB[w])

=============================================================================

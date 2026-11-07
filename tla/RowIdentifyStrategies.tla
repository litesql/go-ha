----------------------- MODULE RowIdentifyStrategies -----------------------
(***************************************************************************)
(* Formal TLA+ Specification of go-ha's Row Identification Strategies      *)
(* (pk, rowid, full), UPDATE->INSERT Fallback, and Transaction Undo        *)
(* (UndoFilterNone, UndoFilterEntity, UndoFilterTransaction).              *)
(*                                                                         *)
(* Models the exact logic implemented in:                                  *)
(*   - changeset.go: pkIdentifyStrategy, rowidIdentifyStrategy,            *)
(*                   fullIdentifyStrategy, ChangeSet.Prepare (lines 139-156*)
(*                   UPDATE -> INSERT fallback), Change.Reverse            *)
(*   - subscriber.go: NATSSubscriber.undo, DBSubscriber.undo,              *)
(*                    filterEntityChanges, reverseChanges                  *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Keys,           \* Set of primary keys / rowids (e.g. {1, 2})
    Values,         \* Set of non-key column values (e.g. {10, 20})
    MaxHistory      \* Maximum number of committed transactions to record

VARIABLES
    originDB,       \* [k \in Keys -> Values \cup {NullVal}] origin table state
    replicaPK,      \* [k \in Keys -> Values \cup {NullVal}] replica using "pk" strategy
    replicaRowid,   \* [k \in Keys -> Values \cup {NullVal}] replica using "rowid" strategy
    replicaFull,    \* [k \in Keys -> Values \cup {NullVal}] replica using "full" strategy
    history,        \* Seq(Change) committed change log for Undo verification
    snapshots,      \* Seq([Keys -> Values \cup {NullVal}]) origin DB state before each tx
    undoneState,    \* Table state after executing UndoBySeq(1, UndoFilterNone)
    entityUndone    \* [k \in Keys -> [Keys -> Values \cup {NullVal}]] state after UNDOE for k

vars == <<originDB, replicaPK, replicaRowid, replicaFull,
          history, snapshots, undoneState, entityUndone>>

NullVal == 0

-----------------------------------------------------------------------------
(* Change Representation & Reverse Operator (changeset.go:510-598)         *)
-----------------------------------------------------------------------------

ChangeSetOp == {"INSERT", "UPDATE", "DELETE"}

(* changeset.go:572-598:                                                   *)
(* Reverse swaps INSERT <-> DELETE and swaps OldValues <-> NewValues,      *)
(* OldRowID <-> NewRowID.                                                  *)
ReverseChange(c) ==
    [op     |-> CASE c.op = "INSERT" -> "DELETE"
                  [] c.op = "DELETE" -> "INSERT"
                  [] OTHER           -> "UPDATE",
     key    |-> c.key,
     oldVal |-> c.newVal,
     newVal |-> c.oldVal]

-----------------------------------------------------------------------------
(* Strategy Application Operators (changeset.go:123-157, 312-425)          *)
-----------------------------------------------------------------------------

(* 1. pkIdentifyStrategy (changeset.go:357-395) + Prepare fallback:        *)
(*    - INSERT: REPLACE INTO table (pk, val) VALUES (key, newVal)          *)
(*    - UPDATE: UPDATE table SET val = newVal WHERE pk = key               *)
(*      If RowsAffected() == 0 (table[key] = NullVal), fallback in         *)
(*      ChangeSet.Prepare (lines 139-156) converts op to INSERT (REPLACE). *)
(*    - DELETE: DELETE FROM table WHERE pk = key                           *)
ApplyPK(db, c) ==
    CASE c.op = "INSERT" -> [db EXCEPT ![c.key] = c.newVal]
      [] c.op = "UPDATE" ->
            IF db[c.key] /= NullVal
            THEN [db EXCEPT ![c.key] = c.newVal]      \* WHERE pk = key matched
            ELSE [db EXCEPT ![c.key] = c.newVal]      \* 0 rows -> fallback INSERT (REPLACE)
      [] c.op = "DELETE" -> [db EXCEPT ![c.key] = NullVal]

(* 2. rowidIdentifyStrategy (changeset.go:397-425) + Prepare fallback:     *)
(*    Matches rows by rowid (modeled as c.key for integer primary key /    *)
(*    stable rowid).                                                       *)
ApplyRowid(db, c) ==
    CASE c.op = "INSERT" -> [db EXCEPT ![c.key] = c.newVal]
      [] c.op = "UPDATE" ->
            IF db[c.key] /= NullVal
            THEN [db EXCEPT ![c.key] = c.newVal]
            ELSE [db EXCEPT ![c.key] = c.newVal]
      [] c.op = "DELETE" -> [db EXCEPT ![c.key] = NullVal]

(* 3. fullIdentifyStrategy (changeset.go:312-355) + Prepare fallback:      *)
(*    - INSERT: REPLACE INTO table (pk, val) VALUES (key, newVal)          *)
(*    - UPDATE: UPDATE table SET val = newVal WHERE pk = key AND val = old *)
(*      If db[key] /= oldVal, WHERE matches 0 rows! Then ChangeSet.Prepare *)
(*      (lines 139-156) falls back to INSERT (REPLACE INTO by PK), which   *)
(*      still sets db[key] = newVal when the table has a Primary Key!      *)
(*    - DELETE: DELETE FROM table WHERE pk = key AND val = oldVal          *)
(*      Note: DELETE has NO fallback in ChangeSet.Prepare! If db[key]      *)
(*      differs from c.oldVal, DELETE matches 0 rows.                      *)
ApplyFull(db, c) ==
    CASE c.op = "INSERT" -> [db EXCEPT ![c.key] = c.newVal]
      [] c.op = "UPDATE" ->
            IF db[c.key] = c.oldVal
            THEN [db EXCEPT ![c.key] = c.newVal]      \* WHERE pk=k AND val=oldVal matched
            ELSE [db EXCEPT ![c.key] = c.newVal]      \* 0 rows -> fallback INSERT (REPLACE)
      [] c.op = "DELETE" ->
            IF db[c.key] = c.oldVal
            THEN [db EXCEPT ![c.key] = NullVal]
            ELSE db                                   \* 0 rows matched, no fallback

(* Fold a sequence of changes over a database state using ApplyPK          *)
RECURSIVE ApplySeqPK(_, _)
ApplySeqPK(db, seq) ==
    IF seq = <<>>
    THEN db
    ELSE ApplySeqPK(ApplyPK(db, Head(seq)), Tail(seq))

(* subscriber.go:532-533, 864-865:                                         *)
(* undoChangeSet.Changes = reverseChanges(undoChangeSet.Changes)           *)
(* slices.Reverse(undoChangeSet.Changes)                                   *)
RECURSIVE ReverseAndInvert(_)
ReverseAndInvert(seq) ==
    IF seq = <<>>
    THEN <<>>
    ELSE Append(ReverseAndInvert(Tail(seq)), ReverseChange(Head(seq)))

(* subscriber.go:869-883: filterEntityChanges filters changes by entity key*)
RECURSIVE FilterByEntity(_, _)
FilterByEntity(seq, targetKey) ==
    IF seq = <<>>
    THEN <<>>
    ELSE IF Head(seq).key = targetKey
         THEN <<Head(seq)>> \o FilterByEntity(Tail(seq), targetKey)
         ELSE FilterByEntity(Tail(seq), targetKey)

(* Subsequence from startIdx..Len(seq) for UndoBySeq(startSeq)             *)
SuffixFrom(seq, startIdx) ==
    SubSeq(seq, startIdx, Len(seq))

-----------------------------------------------------------------------------
(* Initial State & Transitions                                             *)
-----------------------------------------------------------------------------

EmptyDB == [k \in Keys |-> NullVal]

TypeOK ==
    /\ originDB \in [Keys -> Values \cup {NullVal}]
    /\ replicaPK \in [Keys -> Values \cup {NullVal}]
    /\ replicaRowid \in [Keys -> Values \cup {NullVal}]
    /\ replicaFull \in [Keys -> Values \cup {NullVal}]
    /\ Len(history) <= MaxHistory
    /\ Len(snapshots) = Len(history)
    /\ undoneState \in [Keys -> Values \cup {NullVal}]
    /\ entityUndone \in [Keys -> [Keys -> Values \cup {NullVal}]]

Init ==
    /\ originDB = EmptyDB
    /\ replicaPK = EmptyDB
    /\ replicaRowid = EmptyDB
    /\ replicaFull = EmptyDB
    /\ history = <<>>
    /\ snapshots = <<>>
    /\ undoneState = EmptyDB
    /\ entityUndone = [k \in Keys |-> EmptyDB]

UpdateDerivedUndoStates(newOrigin, newHist) ==
    /\ undoneState' = ApplySeqPK(newOrigin, ReverseAndInvert(newHist))
    /\ entityUndone' = [k \in Keys |->
        ApplySeqPK(newOrigin, ReverseAndInvert(FilterByEntity(newHist, k)))]

(* Execute INSERT on key k when row does not currently exist on origin     *)
ExecInsert(k, v) ==
    /\ Len(history) < MaxHistory
    /\ originDB[k] = NullVal
    /\ LET change  == [op |-> "INSERT", key |-> k, oldVal |-> NullVal, newVal |-> v]
           newOrig == [originDB EXCEPT ![k] = v]
           newHist == Append(history, change)
       IN /\ snapshots' = Append(snapshots, originDB)
          /\ originDB' = newOrig
          /\ replicaPK' = ApplyPK(replicaPK, change)
          /\ replicaRowid' = ApplyRowid(replicaRowid, change)
          /\ replicaFull' = ApplyFull(replicaFull, change)
          /\ history' = newHist
          /\ UpdateDerivedUndoStates(newOrig, newHist)

(* Execute UPDATE on key k when row currently exists on origin             *)
ExecUpdate(k, v) ==
    /\ Len(history) < MaxHistory
    /\ originDB[k] /= NullVal
    /\ originDB[k] /= v
    /\ LET change  == [op |-> "UPDATE", key |-> k, oldVal |-> originDB[k], newVal |-> v]
           newOrig == [originDB EXCEPT ![k] = v]
           newHist == Append(history, change)
       IN /\ snapshots' = Append(snapshots, originDB)
          /\ originDB' = newOrig
          /\ replicaPK' = ApplyPK(replicaPK, change)
          /\ replicaRowid' = ApplyRowid(replicaRowid, change)
          /\ replicaFull' = ApplyFull(replicaFull, change)
          /\ history' = newHist
          /\ UpdateDerivedUndoStates(newOrig, newHist)

(* Execute DELETE on key k when row currently exists on origin             *)
ExecDelete(k) ==
    /\ Len(history) < MaxHistory
    /\ originDB[k] /= NullVal
    /\ LET change  == [op |-> "DELETE", key |-> k, oldVal |-> originDB[k], newVal |-> NullVal]
           newOrig == [originDB EXCEPT ![k] = NullVal]
           newHist == Append(history, change)
       IN /\ snapshots' = Append(snapshots, originDB)
          /\ originDB' = newOrig
          /\ replicaPK' = ApplyPK(replicaPK, change)
          /\ replicaRowid' = ApplyRowid(replicaRowid, change)
          /\ replicaFull' = ApplyFull(replicaFull, change)
          /\ history' = newHist
          /\ UpdateDerivedUndoStates(newOrig, newHist)

Terminated ==
    /\ Len(history) = MaxHistory
    /\ UNCHANGED vars

Next ==
    \/ \E k \in Keys, v \in Values : ExecInsert(k, v)
    \/ \E k \in Keys, v \in Values : ExecUpdate(k, v)
    \/ \E k \in Keys : ExecDelete(k)
    \/ Terminated

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety Invariants                                                       *)
-----------------------------------------------------------------------------

(* 1. Sequential Replication Convergence Across All 3 RowIdentify Modes:   *)
(* Under ordered replication of valid SQLite operations, all three         *)
(* strategies (pk, rowid, full) maintain state identical to originDB.      *)
AllStrategiesMatchOrigin ==
    /\ replicaPK = originDB
    /\ replicaRowid = originDB
    /\ replicaFull = originDB

(* 2. PK, Rowid & Full UPDATE Fallback Self-Healing Property               *)
(* (changeset.go:139-156, 405-420):                                        *)
(* Even if a replica's row was missing or had a stale value (tested on     *)
(* current originDB and mismatched states), applying an UPDATE change via  *)
(* ApplyPK, ApplyRowid, or ApplyFull (with the 0-rows-affected -> INSERT   *)
(* fallback) heals the row to `c.newVal`, whereas a DELETE under `full`    *)
(* strategy on a mismatched value leaves the stale value intact!           *)
UpdateFallbackSelfHealingAndFullDeleteContrast ==
    \A k \in Keys, v1 \in Values, v2 \in Values :
        LET uChange == [op |-> "UPDATE", key |-> k, oldVal |-> v1, newVal |-> v2]
            dChange == [op |-> "DELETE", key |-> k, oldVal |-> v1, newVal |-> NullVal]
            staleDB == [originDB EXCEPT ![k] = v2]
        IN /\ ApplyPK(originDB, uChange)[k] = v2
           /\ ApplyRowid(originDB, uChange)[k] = v2
           /\ ApplyFull(originDB, uChange)[k] = v2
           /\ ApplyPK(staleDB, dChange)[k] = NullVal
           /\ ApplyRowid(staleDB, dChange)[k] = NullVal
           /\ (v1 /= v2 => ApplyFull(staleDB, dChange)[k] = v2)

(* 3. Partial & Full Undo Invertibility Invariant (UndoBySeq(startSeq)):   *)
(* For ANY sequence number startSeq \in 1..Len(history), reversing and     *)
(* inverting the suffix history[startSeq..Len(history)] restores the       *)
(* database to the exact snapshot taken immediately before startSeq!       *)
UndoByAnySeqRestoresSnapshot ==
    /\ undoneState = EmptyDB
    /\ \A i \in 1..Len(history) :
        ApplySeqPK(originDB, ReverseAndInvert(SuffixFrom(history, i))) = snapshots[i]

(* 4. Entity Undo Isolation Invariant (UNDOE / UndoFilterEntity):          *)
(* Filtering undo by entity k (filterEntityChanges) from any startSeq      *)
(* restores entity k to its snapshot state at startSeq while leaving every *)
(* other entity m /= k untouched at its current committed value!           *)
EntityUndoIsolation ==
    \A k \in Keys :
        /\ entityUndone[k][k] = EmptyDB[k]
        /\ \A m \in Keys \ {k} : entityUndone[k][m] = originDB[m]
        /\ \A i \in 1..Len(history) :
            LET afterEntityUndo == ApplySeqPK(originDB, ReverseAndInvert(FilterByEntity(SuffixFrom(history, i), k)))
            IN /\ afterEntityUndo[k] = snapshots[i][k]
               /\ \A m \in Keys \ {k} : afterEntityUndo[m] = originDB[m]

=============================================================================

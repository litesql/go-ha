----------------------- MODULE LeaderlessReplication -----------------------
(***************************************************************************)
(* Formal TLA+ Specification of go-ha's Leaderless Replication Strategy,   *)
(* Outbox Asynchronous Publisher, Content-Hash Deduplication, Snapshotting,*)
(* and Conflict Resolution (ChangeSetInterceptor vs Default Self-Skip).    *)
(*                                                                         *)
(* Models the exact protocol implemented in:                               *)
(*   - publisher.go: NATSPublisher (SHA-256 MsgID dedup), OutboxPublisher  *)
(*   - subscriber.go: NATSSubscriber (ha_stats received_seq, self-skip)    *)
(*   - changeset.go: ChangeSet.Apply, ChangeSetInterceptor                 *)
(*   - snapshotter.go: NATSSnapshotter (TakeSnapshot, LatestSnapshot)      *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Nodes,              \* Set of cluster nodes
    MaxTx,              \* Maximum number of transactions to originate
    ConflictPolicy      \* "InterceptorLWW" or "DefaultNoInterceptor"

VARIABLES
    nextTxID,           \* Counter for unique transaction/message IDs (1..MaxTx+1)
    nodeVal,            \* [n \in Nodes -> 0..MaxTx] current row value on node n
    nodeVer,            \* [n \in Nodes -> 0..MaxTx] logical version/timestamp of row on node n
    outbox,             \* [n \in Nodes -> Seq(Msg)] ha_outbox table per node
    stream,             \* Seq(Msg) in NATS JetStream replication stream
    receivedSeq,        \* [n \in Nodes -> 0..MaxTx] ha_stats.received_seq per node
    snapshotSeq,        \* Latest snapshot sequence in NATS Object Store
    snapshotVal,        \* Row value captured in latest snapshot
    snapshotVer         \* Row version captured in latest snapshot

vars == <<nextTxID, nodeVal, nodeVer, outbox, stream,
          receivedSeq, snapshotSeq, snapshotVal, snapshotVer>>

MsgType == [id : 1..MaxTx, origin : Nodes, val : 1..MaxTx]

TypeOK ==
    /\ nextTxID \in 1..(MaxTx + 1)
    /\ nodeVal \in [Nodes -> 0..MaxTx]
    /\ nodeVer \in [Nodes -> 0..MaxTx]
    /\ \A n \in Nodes : Len(outbox[n]) <= MaxTx
    /\ Len(stream) <= MaxTx
    /\ receivedSeq \in [Nodes -> 0..MaxTx]
    /\ snapshotSeq \in 0..MaxTx
    /\ snapshotVal \in 0..MaxTx
    /\ snapshotVer \in 0..MaxTx

Init ==
    /\ nextTxID = 1
    /\ nodeVal = [n \in Nodes |-> 0]
    /\ nodeVer = [n \in Nodes |-> 0]
    /\ outbox = [n \in Nodes |-> <<>>]
    /\ stream = <<>>
    /\ receivedSeq = [n \in Nodes |-> 0]
    /\ snapshotSeq = 0
    /\ snapshotVal = 0
    /\ snapshotVer = 0

-----------------------------------------------------------------------------
(* Local Write & Outbox Persistence (publisher.go:741-851, sqlite_conn.go) *)
-----------------------------------------------------------------------------

(* Node n executes a local write transaction:                              *)
(* Atomically updates local SQLite table and inserts changeset into        *)
(* ha_outbox (preparedOutboxCommit.RecordCommitDecision + tx.Tx.Commit()). *)
LocalWriteWithOutbox(n) ==
    /\ nextTxID <= MaxTx
    /\ LET txID == nextTxID
           msg  == [id |-> txID, origin |-> n, val |-> txID]
       IN /\ nextTxID' = nextTxID + 1
          /\ nodeVal' = [nodeVal EXCEPT ![n] = txID]
          /\ nodeVer' = [nodeVer EXCEPT ![n] = txID]
          /\ outbox' = [outbox EXCEPT ![n] = Append(@, msg)]
          /\ UNCHANGED <<stream, receivedSeq, snapshotSeq, snapshotVal, snapshotVer>>

-----------------------------------------------------------------------------
(* Outbox Relay & NATS JetStream Deduplication (publisher.go:138, 785-815) *)
-----------------------------------------------------------------------------

InStream(msgID) ==
    \E i \in 1..Len(stream) : stream[i].id = msgID

(* Outbox relay publishes head of ha_outbox to NATS JetStream with         *)
(* SHA-256 content-derived MsgID (publisher.go:138-139).                   *)
(* If the message was already published (e.g. relay crashed before DELETE  *)
(* FROM ha_outbox), JetStream deduplicates it (pubAck.Duplicate = true).   *)
OutboxRelayPublishAndKeep(n) ==
    /\ Len(outbox[n]) > 0
    /\ LET msg == Head(outbox[n])
       IN /\ stream' = IF InStream(msg.id) THEN stream ELSE Append(stream, msg)
          /\ UNCHANGED <<nextTxID, nodeVal, nodeVer, outbox,
                         receivedSeq, snapshotSeq, snapshotVal, snapshotVer>>

(* Normal Outbox relay completion: publishes to JetStream (deduplicated    *)
(* if already published) and deletes row from ha_outbox (publisher.go:811).*)
OutboxRelayPublishAndDelete(n) ==
    /\ Len(outbox[n]) > 0
    /\ LET msg == Head(outbox[n])
       IN /\ stream' = IF InStream(msg.id) THEN stream ELSE Append(stream, msg)
          /\ outbox' = [outbox EXCEPT ![n] = Tail(@)]
          /\ UNCHANGED <<nextTxID, nodeVal, nodeVer,
                         receivedSeq, snapshotSeq, snapshotVal, snapshotVer>>

-----------------------------------------------------------------------------
(* NATS Subscriber Consumption & Conflict Resolution (subscriber.go:552)   *)
-----------------------------------------------------------------------------

(* NATSSubscriber.handler (subscriber.go:552-626):                         *)
(* Consumes stream[receivedSeq[n] + 1].                                    *)
(* - Self-Origin Skip (subscriber.go:570):                                 *)
(*   if cs.Node == s.node && cs.ProcessID == processID, skips Apply() and  *)
(*   only updates ha_stats.received_seq.                                   *)
(* - Remote Apply (changeset.go:99-166):                                   *)
(*   * Under "InterceptorLWW", ChangeSetInterceptor.BeforeApply skips      *)
(*     changesets whose version/timestamp is older than local state.       *)
(*   * Under "DefaultNoInterceptor", ChangeSet.Prepare unconditionally     *)
(*     applies the remote changeset.                                       *)
SubscriberConsume(n) ==
    /\ receivedSeq[n] < Len(stream)
    /\ LET nextSeq == receivedSeq[n] + 1
           msg     == stream[nextSeq]
       IN /\ IF msg.origin = n
             THEN /\ UNCHANGED <<nodeVal, nodeVer>>
             ELSE IF ConflictPolicy = "InterceptorLWW"
                  THEN IF msg.val > nodeVer[n]
                       THEN /\ nodeVal' = [nodeVal EXCEPT ![n] = msg.val]
                            /\ nodeVer' = [nodeVer EXCEPT ![n] = msg.val]
                       ELSE /\ UNCHANGED <<nodeVal, nodeVer>>
                  ELSE /\ nodeVal' = [nodeVal EXCEPT ![n] = msg.val]
                       /\ nodeVer' = [nodeVer EXCEPT ![n] = msg.val]
          /\ receivedSeq' = [receivedSeq EXCEPT ![n] = nextSeq]
          /\ UNCHANGED <<nextTxID, outbox, stream, snapshotSeq, snapshotVal, snapshotVer>>

-----------------------------------------------------------------------------
(* Snapshotter (snapshotter.go:149-264)                                    *)
-----------------------------------------------------------------------------

(* Node n takes a snapshot at its current receivedSeq[n] when it has no    *)
(* un-echoed local writes in flight, advancing snapshotSeq.                *)
TakeSnapshot(n) ==
    /\ receivedSeq[n] > snapshotSeq
    /\ outbox[n] = <<>>
    /\ \A i \in (receivedSeq[n] + 1)..Len(stream) : stream[i].origin /= n
    /\ snapshotSeq' = receivedSeq[n]
    /\ snapshotVal' = nodeVal[n]
    /\ snapshotVer' = nodeVer[n]
    /\ UNCHANGED <<nextTxID, nodeVal, nodeVer, outbox, stream, receivedSeq>>

(* A lagging replica with no pending local writes restores from the        *)
(* latest snapshot in NATS Object Store and resumes consuming after        *)
(* snapshotSeq.                                                            *)
RestoreFromSnapshot(n) ==
    /\ snapshotSeq > receivedSeq[n]
    /\ outbox[n] = <<>>
    /\ nodeVer[n] <= snapshotVer
    /\ nodeVal' = [nodeVal EXCEPT ![n] = snapshotVal]
    /\ nodeVer' = [nodeVer EXCEPT ![n] = snapshotVer]
    /\ receivedSeq' = [receivedSeq EXCEPT ![n] = snapshotSeq]
    /\ UNCHANGED <<nextTxID, outbox, stream, snapshotSeq, snapshotVal, snapshotVer>>

(* Terminal stuttering step once all transactions are published & consumed.*)
Terminated ==
    /\ nextTxID > MaxTx
    /\ \A n \in Nodes :
        /\ outbox[n] = <<>>
        /\ receivedSeq[n] = Len(stream)
    /\ UNCHANGED vars

Next ==
    \/ \E n \in Nodes : LocalWriteWithOutbox(n)
    \/ \E n \in Nodes : OutboxRelayPublishAndKeep(n)
    \/ \E n \in Nodes : OutboxRelayPublishAndDelete(n)
    \/ \E n \in Nodes : SubscriberConsume(n)
    \/ \E n \in Nodes : TakeSnapshot(n)
    \/ \E n \in Nodes : RestoreFromSnapshot(n)
    \/ Terminated

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety Invariants                                                       *)
-----------------------------------------------------------------------------

(* 1. JetStream Deduplication Invariant (publisher.go:138-139):            *)
(* Even when OutboxPublisher retries publishing the same changeset         *)
(* (OutboxRelayPublishAndKeep), NATS JetStream never stores duplicate      *)
(* messages with the same content-derived MsgID.                           *)
NoDuplicateMessagesInStream ==
    \A i, j \in 1..Len(stream) :
        (stream[i].id = stream[j].id) => (i = j)

(* 2. Outbox Bounded & Ordered Invariant:                                  *)
(* Every node's receivedSeq never exceeds the length of the JetStream log. *)
SubscriberSeqWithinStreamBounds ==
    \A n \in Nodes :
        /\ receivedSeq[n] <= Len(stream)
        /\ snapshotSeq <= Len(stream)

(* 3. Strong Eventual Consistency (SEC) when Quiescent:                    *)
(* When all outboxes are drained and all subscribers have caught up to     *)
(* Len(stream), every node in the cluster converges to the exact same      *)
(* database state!                                                         *)
(* Note: Holds under "InterceptorLWW" (or single-writer/causal writes),    *)
(* whereas "DefaultNoInterceptor" violates this due to self-skip on        *)
(* concurrent writes to the same key!                                      *)
StrongEventualConvergenceWhenQuiescent ==
    (\A n \in Nodes : outbox[n] = <<>> /\ receivedSeq[n] = Len(stream))
        => (\A n1, n2 \in Nodes : nodeVal[n1] = nodeVal[n2])

-----------------------------------------------------------------------------
(* Liveness / Temporal Properties                                          *)
-----------------------------------------------------------------------------

Fairness ==
    /\ \A n \in Nodes : WF_vars(OutboxRelayPublishAndDelete(n))
    /\ \A n \in Nodes : WF_vars(SubscriberConsume(n))

FairSpec == Spec /\ Fairness

(* Every locally committed outbox entry is eventually published to the     *)
(* NATS JetStream stream and consumed by all nodes in the cluster.         *)
EventualOutboxDrainAndDelivery ==
    \A n \in Nodes :
        (Len(outbox[n]) > 0) ~> (Len(outbox[n]) = 0 /\ receivedSeq[n] = Len(stream))

=============================================================================

----------------------- MODULE LeaderBasedReplication -----------------------
(***************************************************************************)
(* Formal TLA+ Specification of go-ha's Leader-Based Replication Strategy  *)
(*                                                                         *)
(* Models the exact protocol implemented in:                               *)
(*   - leader.go: StaticLeader, DynamicLeader, StartLeaderElection         *)
(*   - sqlite_conn.go: ExecContext, QueryContext (RYW loop),               *)
(*                     redirectExecToGrpc, redirectQuery                   *)
(*   - http.go: ForwardToLeaderFunc, ConsistentReaderFunc, TXCookieName    *)
(*   - publisher.go / subscriber.go: NATSPublisher, NATSSubscriber         *)
(*   - connect/service.go: Service.Query (execUpdate, execQuery)           *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Nodes,          \* Set of cluster nodes
    MaxWrites,      \* Maximum number of writes to model
    MaxFailovers    \* Maximum number of leader failovers / KV TTL expirations

VARIABLES
    graftLeader,    \* Current Graft (Raft) leader node, or "None"
    kvRedirect,     \* Current value of "redirect" key in NATS KV bucket, or "None"
    target,         \* [n \in Nodes -> Nodes \cup {"None"}] cached d.target per node
    retries,        \* [n \in Nodes -> 0..3] KV operation failure retry count per node
    stream,         \* Sequence of [seq: Nat, origin: Nodes] in NATS JetStream
    nodeApplied,    \* [n \in Nodes -> SUBSET (1..MaxWrites)] write seqs in node's SQLite DB
    latestSeq,      \* [n \in Nodes -> 0..MaxWrites] NATSSubscriber.LatestSeq() (ha_stats)
    pubSeq,         \* [n \in Nodes -> 0..MaxWrites] NATSPublisher.Sequence() (c.PubSeq())
    clientTxSeq,    \* Session watermark (c.txseq / _txseq cookie) from last write
    faultCount      \* Number of leader failovers / KV TTL expirations injected

vars == <<graftLeader, kvRedirect, target, retries, stream,
          nodeApplied, latestSeq, pubSeq, clientTxSeq, faultCount>>

-----------------------------------------------------------------------------
(* Helper Operators matching leader.go & http.go                           *)
-----------------------------------------------------------------------------

(* leader.go:52-57:                                                        *)
(* func (d *DynamicLeader) IsLeader() bool {                               *)
(*     return d.node.State() == graft.LEADER && target == d.localTarget    *)
(* }                                                                       *)
IsLeader(n) ==
    /\ graftLeader = n
    /\ target[n] = n

(* leader.go:65-67:                                                        *)
(* func (d *DynamicLeader) IsReady() bool {                                *)
(*     return d.RedirectTarget() != ""                                     *)
(* }                                                                       *)
IsReady(n) ==
    target[n] /= "None"

(* http.go:114:                                                            *)
(* max(c.LatestSeq(), c.PubSeq()) >= txSeq                                 *)
MaxNodeSeq(n) ==
    IF latestSeq[n] >= pubSeq[n] THEN latestSeq[n] ELSE pubSeq[n]

StreamSeqs == 1..Len(stream)

TypeOK ==
    /\ graftLeader \in Nodes \cup {"None"}
    /\ kvRedirect \in Nodes \cup {"None"}
    /\ target \in [Nodes -> Nodes \cup {"None"}]
    /\ retries \in [Nodes -> 0..3]
    /\ Len(stream) <= MaxWrites
    /\ \A i \in 1..Len(stream) :
        /\ stream[i].seq = i
        /\ stream[i].origin \in Nodes
    /\ nodeApplied \in [Nodes -> SUBSET (1..MaxWrites)]
    /\ latestSeq \in [Nodes -> 0..MaxWrites]
    /\ pubSeq \in [Nodes -> 0..MaxWrites]
    /\ clientTxSeq \in 0..MaxWrites
    /\ faultCount \in 0..MaxFailovers

Init ==
    /\ \E n \in Nodes :
        /\ graftLeader = n
        /\ kvRedirect = n
        /\ target = [m \in Nodes |-> n]
    /\ retries = [n \in Nodes |-> 0]
    /\ stream = <<>>
    /\ nodeApplied = [n \in Nodes |-> {}]
    /\ latestSeq = [n \in Nodes |-> 0]
    /\ pubSeq = [n \in Nodes |-> 0]
    /\ clientTxSeq = 0
    /\ faultCount = 0

-----------------------------------------------------------------------------
(* Leader Election & NATS KV Redirect Target Sync (leader.go:127-190)      *)
-----------------------------------------------------------------------------

(* Graft elects node n (sc.To == graft.LEADER, leader.go:133-142).         *)
(* Node n puts redirectKey = n into NATS KV and sets target[n] = n.        *)
(* Note: Graft election can occur even BEFORE node n's NATS subscriber has *)
(* finished consuming all prior stream entries (latestSeq[n] < Len(stream))*)
GraftElectLeader(n) ==
    /\ faultCount < MaxFailovers
    /\ graftLeader /= n
    /\ graftLeader' = n
    /\ kvRedirect' = n
    /\ target' = [target EXCEPT ![n] = n]
    /\ retries' = [retries EXCEPT ![n] = 0]
    /\ faultCount' = faultCount + 1
    /\ UNCHANGED <<stream, nodeApplied, latestSeq, pubSeq, clientTxSeq>>

(* NATS KV bucket TTL (1 minute) expires the redirect key if leader fails  *)
(* to refresh within the TTL window (leader.go:114).                       *)
ExpireKVRedirect ==
    /\ faultCount < MaxFailovers
    /\ kvRedirect /= "None"
    /\ kvRedirect' = "None"
    /\ faultCount' = faultCount + 1
    /\ UNCHANGED <<graftLeader, target, retries, stream, nodeApplied, latestSeq, pubSeq, clientTxSeq>>

(* 3-second ticker on leader n: refreshes redirectKey in NATS KV via       *)
(* kv.PutString(ctx, redirectKey, redirectTarget) (leader.go:158-171).     *)
TickerLeaderRefreshKV(n) ==
    /\ graftLeader = n
    /\ (kvRedirect /= n \/ target[n] /= n \/ retries[n] /= 0)
    /\ kvRedirect' = n
    /\ target' = [target EXCEPT ![n] = n]
    /\ retries' = [retries EXCEPT ![n] = 0]
    /\ UNCHANGED <<graftLeader, stream, nodeApplied, latestSeq, pubSeq, clientTxSeq, faultCount>>

(* 3-second ticker on follower n when kv.Get(redirectKey) succeeds         *)
(* (leader.go:172-184).                                                    *)
TickerFollowerRefreshSuccess(n) ==
    /\ graftLeader /= n
    /\ kvRedirect /= "None"
    /\ (target[n] /= kvRedirect \/ retries[n] /= 0)
    /\ target' = [target EXCEPT ![n] = kvRedirect]
    /\ retries' = [retries EXCEPT ![n] = 0]
    /\ UNCHANGED <<graftLeader, kvRedirect, stream, nodeApplied, latestSeq, pubSeq, clientTxSeq, faultCount>>

(* 3-second ticker on follower n when kv.Get fails: increments retries;    *)
(* after 3 retries, clears target[n] = "None" (leader.go:174-181).         *)
TickerFollowerRetryFailure(n) ==
    /\ kvRedirect = "None"
    /\ graftLeader /= n
    /\ target[n] /= "None"
    /\ IF retries[n] + 1 >= 3
       THEN /\ target' = [target EXCEPT ![n] = "None"]
            /\ retries' = [retries EXCEPT ![n] = 0]
       ELSE /\ target' = target
            /\ retries' = [retries EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<graftLeader, kvRedirect, stream, nodeApplied, latestSeq, pubSeq, clientTxSeq, faultCount>>

-----------------------------------------------------------------------------
(* Write Execution & Redirection (sqlite_conn.go:196, connect/service.go)  *)
-----------------------------------------------------------------------------

(* Client sends write to node n.                                           *)
(* - If IsLeader(n), n executes the write locally, publishes to NATS       *)
(*   JetStream (updating pubSeq[n] = newSeq), and returns txseq = newSeq.  *)
(* - If ~IsLeader(n) and target[n] = l with IsLeader(l), n redirects the   *)
(*   write over gRPC/HTTP to leader l, which executes it and returns       *)
(*   txseq = newSeq to n and the client (sqlite_conn.go:482-484).          *)
ClientWrite(n) ==
    /\ Len(stream) < MaxWrites
    /\ LET leaderNode == IF IsLeader(n) THEN n ELSE target[n]
           newSeq == Len(stream) + 1
       IN /\ leaderNode \in Nodes
          /\ IsLeader(leaderNode)
          /\ latestSeq[leaderNode] = Len(stream)
          /\ stream' = Append(stream, [seq |-> newSeq, origin |-> leaderNode])
          /\ nodeApplied' = [nodeApplied EXCEPT ![leaderNode] = @ \cup {newSeq}]
          /\ pubSeq' = [pubSeq EXCEPT ![leaderNode] = newSeq]
          /\ clientTxSeq' = newSeq
          /\ UNCHANGED <<graftLeader, kvRedirect, target, retries, latestSeq, faultCount>>

-----------------------------------------------------------------------------
(* NATS JetStream Subscriber Replication (subscriber.go:552-638)           *)
-----------------------------------------------------------------------------

(* Node n's NATSSubscriber consumes the next message (latestSeq[n] + 1)    *)
(* in stream order (MaxAckPending: 1).                                     *)
(* - If msg.origin = n (self-originated, subscriber.go:570), skips         *)
(*   re-applying to nodeApplied[n] and updates ha_stats (latestSeq[n]).    *)
(* - Otherwise, applies msg.seq to nodeApplied[n] and updates latestSeq[n].*)
SubscriberConsume(n) ==
    /\ latestSeq[n] < Len(stream)
    /\ LET nextSeq == latestSeq[n] + 1
           entry == stream[nextSeq]
       IN /\ nodeApplied' =
               IF entry.origin = n
               THEN nodeApplied
               ELSE [nodeApplied EXCEPT ![n] = @ \cup {nextSeq}]
          /\ latestSeq' = [latestSeq EXCEPT ![n] = nextSeq]
          /\ UNCHANGED <<graftLeader, kvRedirect, target, retries, stream, pubSeq, clientTxSeq, faultCount>>

(* Terminal stuttering step once all writes are replicated and synced.     *)
Terminated ==
    /\ Len(stream) = MaxWrites
    /\ \A n \in Nodes :
        /\ latestSeq[n] = MaxWrites
        /\ target[n] = graftLeader
    /\ kvRedirect = graftLeader
    /\ UNCHANGED vars

Next ==
    \/ \E n \in Nodes : GraftElectLeader(n)
    \/ ExpireKVRedirect
    \/ \E n \in Nodes : TickerLeaderRefreshKV(n)
    \/ \E n \in Nodes : TickerFollowerRefreshSuccess(n)
    \/ \E n \in Nodes : TickerFollowerRetryFailure(n)
    \/ \E n \in Nodes : ClientWrite(n)
    \/ \E n \in Nodes : SubscriberConsume(n)
    \/ Terminated

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety Invariants                                                       *)
-----------------------------------------------------------------------------

(* 1. Single Active Leader Invariant:                                      *)
(* At most one node in the cluster can satisfy IsLeader(n) at any time.    *)
AtMostOneActiveLeader ==
    \A n1, n2 \in Nodes :
        (IsLeader(n1) /\ IsLeader(n2)) => (n1 = n2)

(* 2. Subscriber Prefix Consistency Invariant:                             *)
(* Every node's database contains at least the complete prefix             *)
(* 1..latestSeq[n] of the NATS JetStream replication stream, and never     *)
(* contains uncommitted sequences beyond Len(stream).                      *)
PrefixConsistency ==
    \A n \in Nodes :
        /\ (1..latestSeq[n]) \subseteq nodeApplied[n]
        /\ nodeApplied[n] \subseteq (1..Len(stream))

(* 3. Leader Completeness Invariant:                                       *)
(* Once an active leader has caught up to its published/subscribed stream  *)
(* watermark (MaxNodeSeq(n) = Len(stream)), its local SQLite database      *)
(* contains all committed writes 1..Len(stream).                           *)
LeaderCompleteness ==
    \A n \in Nodes :
        (IsLeader(n) /\ MaxNodeSeq(n) = Len(stream)) => (nodeApplied[n] = 1..Len(stream))

(* 4. Read-Your-Writes (RYW) Session Consistency Invariant (http.go:114):  *)
(* Whenever ConsistentReaderFunc serves a read on ANY node n (leader or    *)
(* follower) after checking max(c.LatestSeq(), c.PubSeq()) >= clientTxSeq, *)
(* node n's SQLite database guaranteedly includes all writes 1..clientTxSeq*)
(* even across arbitrary leader failovers before subscriber catch-up.      *)
ReadYourWritesConsistency ==
    \A n \in Nodes :
        (MaxNodeSeq(n) >= clientTxSeq) => ((1..clientTxSeq) \subseteq nodeApplied[n])

-----------------------------------------------------------------------------
(* Liveness / Temporal Properties                                          *)
-----------------------------------------------------------------------------

Fairness ==
    /\ \A n \in Nodes : WF_vars(SubscriberConsume(n))
    /\ \A n \in Nodes : SF_vars(TickerLeaderRefreshKV(n))
    /\ \A n \in Nodes : SF_vars(TickerFollowerRefreshSuccess(n))

FairSpec == Spec /\ Fairness

(* Every node eventually catches up to all published writes in the stream. *)
EventualReplicationCatchup ==
    \A w \in 1..MaxWrites :
        (Len(stream) >= w) ~> (\A n \in Nodes : w \in nodeApplied[n] /\ latestSeq[n] >= w)

(* After bounded KV expirations and leader changes, all nodes eventually   *)
(* converge their redirect target to the active Graft leader.              *)
EventualLeaderTargetConvergence ==
    <>[] (\A m \in Nodes : target[m] = graftLeader /\ kvRedirect = graftLeader)

=============================================================================

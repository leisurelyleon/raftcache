# Raft Implementation Notes

This documents precisely which parts of Raft `raftcache` implements, and what is
intentionally left out — so the scope is honest and a reader knows exactly what
they are looking at.

## Implemented

- **Leader election** with randomized (here, staggered) election timeouts and
  the **election restriction** (§5.4.1): a vote is granted only if the
  candidate's log is at least as up-to-date as the voter's.
- **Log replication** via AppendEntries, including the **log-matching** check on
  `(prevLogIndex, prevLogTerm)` and **conflict truncation** when an existing
  entry's term differs.
- **Commit advancement** following the **current-term commit rule** (§5.4.2): a
  leader advances the commit index by replica count only for an entry from its
  own current term; earlier entries commit indirectly once a current-term entry
  commits.
- **Step-down on higher term**: any message with a higher term causes a return
  to follower and a vote reset.
- **Safety verification**: the simulation asserts Election Safety and State
  Machine Safety under partitions and message loss.

## Intentionally out of scope

These are real parts of production Raft, deliberately omitted to keep a
portfolio core correct and focused rather than sprawling and fragile:

- **PreVote**: without it, a partitioned node can inflate its term and force a
  re-election upon rejoining. This affects *liveness/stability*, not *safety* —
  the safety properties still hold. Production systems add PreVote to avoid
  unnecessary leader churn.
- **Leader no-op on election**: a freshly elected leader does not append a no-op
  entry, so prior-term entries commit only once a new current-term entry does.
- **Log compaction / snapshotting**: the log grows unbounded; there is no
  snapshot+install mechanism.
- **Dynamic membership changes** (joint consensus): the cluster is fixed at
  construction.
- **Persistent durable storage**: state is in-memory; there is no write-ahead
  log to disk and thus no crash-recovery across process restarts.

## Why this scoping is the right call

Raft's hard, interesting, and bug-prone part is the *core safety algorithm* —
elections, log matching, and the commit rules — and that is what this project
implements correctly and tests hard. The omitted features are well-understood
extensions; implementing them halfway would add bugs without adding signal. A
correct subset, clearly documented, is more valuable than a buggy whole.

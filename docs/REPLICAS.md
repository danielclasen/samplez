# Running a replica set

What running replicas of the engine under one Raft log takes: starting
and joining, readiness, configuration, rollouts, the ops endpoints, the
replica's metrics and its timers. How replicated mode works is in
[ARCHITECTURE.md](ARCHITECTURE.md#replicated-mode); the engine's own
behaviour in [USAGE.md](USAGE.md).

## Starting a replica

A replica that joins a set is a learner until the leader promotes it,
and a learner reads not ready: it commits nothing. The voter count stays
odd, so a set starting from its first voter promotes no lone learner and
grows by two. On Kubernetes its StatefulSet needs `podManagementPolicy:
Parallel` (under `OrderedReady` the second ordinal waits for promotion,
never reads ready, and the third is never created), and replicas reach
each other through a headless Service with `publishNotReadyAddresses:
true`, so a replica that reads not ready still takes part in the log,
contact and resync.

A set agrees on its cluster id through a marker ConfigMap that ordinal 0
creates on an empty volume (`bootstrap::Kube`). Given the StatefulSet's
name (`Kube::owned_by`), the marker carries an owner reference to it and
the garbage collector deletes it after the StatefulSet; `--cascade=orphan`
keeps it. Reading the StatefulSet's uid takes the `get` on `statefulsets`
the rollout's target revision takes. The leader of a running set
recreates a missing marker under the set's cluster id, so a StatefulSet
created again on kept volumes goes on as the same set; an owned marker is
created only for the StatefulSet the process first read, and not while
it is being deleted. With no set running, an empty volume waits for a
marker; one created by hand with `data.cluster` from a member volume's
`cluster` file and `data.volume` 0 lets it join.

## Readiness

An audio replica of a set of more than one voter reads ready (`/readyz`)
once it has applied the log up to the commit it knows and is in contact
with a majority, and then:

- while it commits; or
- once it has committed since its process started, while another audio
  replica's report within `w + D` says that one commits, whatever its own
  state (`starting`, `withdrawn`, `diverged`, `resync` or
  `version_mismatch`).

A process that has not committed since it started reads ready only once
it commits, so a release that never commits stops a rolling update at its
first pod, while an old pod that withdraws, hands the baseline over,
resyncs or is rebuilt keeps its readiness. A rebuild or a return to
`starting` keeps that it committed; a restart does not. A replica in
`version_mismatch` below the baseline's versions, as a pod rolled back
is, reads ready while another commits, so a rollback goes on. No replica
reads ready while none commits. A withdrawn replica with no output
enabled, or with none, reads ready as one that commits, so a set whose
outputs are all disabled still serves its API. Out of contact the term
keeps its last value for `rollout::DEPARTED` (3 s); past that the replica
reads ready only while it commits. Before its first contact it reads not
ready.

An audio replica leaves `starting` once, in this order, its clock is
verified, the horizon does not hold it, its log has applied what its
restart and the commit it knows need, its engine core is rebuilt from
product state, the engine's pending generation is built, and the engine
has processed a block. `samplez_replica_start_held{reason}` reads 1
under the first of these that fails (`clock_unverified`, `horizon_held`,
`applying`, `rebuilding`, `generation_pending`, `not_processing`), and a
stay in `starting` a reason still holds after 30 s logs that reason once
at WARN with the values that decide it. A clock verifies once its kernel
TAI offset is the set's and it lies within `clock_guard` plus the
exchange's one-way delay of more than half of the audio replicas counted
(of a majority of the voters while none is counted). So a restart waits
in `starting` while its node's clock is further than that from its
peers', as a node's clock can be while its time service settles after a
reboot, and with two replicas counted, one whose node's clock is off
holds the other replicas' restarts as well.

A ready replica that commits nothing serves the product's API as one that
commits: it forwards writes to the leader and takes the leader's read
index. A ready audio replica takes writes whether or not a leader counts
it as a voter yet; reaching no leader, it answers a write as not applied,
unavailable for want of a leader, at the write timeout. A replica of a
product without contact takes writes only while a leader counts it as a
voter. A forward whose answer is lost is sent again under the same
command id and applied once.

## Configuration

Product state is shared by every replica, so every member, learners and
the witness included, needs equal configuration as far as it changes how
a command applies or a saved state loads (`Product::config_hash`). The
leader orders no client command while a member reports another hash
(`unavailable`, naming it), until it reports the set's hash or leaves,
and promotes no learner of another hash. A replica of another hash
refuses a snapshot before anything is written and exits with status 78
(`CONFIG_MISMATCH_EXIT`), so it restarts visibly; an embedder chooses
otherwise with `Store::on_config_mismatch`. The replica whose hash
differs from the others' is the one to fix, and it may be the sender. Each
replica's hash is in its contact metrics (`config_hash`). A store warns at
open when its snapshot was built under another hash: expected once after
a change made with every replica stopped, which is how such a change is
made; otherwise that replica holds another state and is rebuilt from an
empty volume.

The engine's `rampSamples` and `blockSize` equal the replica's
`Limits::ramp_samples` and `Limits::block` (`Replica::contact` refuses
audio whose engine reports others), and the product sets
`Timing::prune_after_blocks` to the engine's `pruneAfterBlocks`.

An output starts as configured (`enabled`). In replicated mode the
product's targets change an output only by an `Output` entry, which a
diff emits when the output's enabled state changes, or by a rebuild, so a
product's initial targets match the configured start. An install of a
product state whose outputs list leaves an output out starts it as
configured.

## Rollouts

Page hashes are compared and state is transferred only between replicas
of the same compatibility key
([Versions](ARCHITECTURE.md#versions-and-the-compatibility-key)). A
release that keeps the key rolls into a running set as peers. A rollout
that changes it hands the baseline over at a page and block boundary to a
replica at the StatefulSet's target revision, once that replica is ready
and has applied the latest `Baseline`.

A replica may take the baseline over when its versions are greater than
or equal to the baseline's (ordered by compute, then protocol, then
derivation version). A replica of lower versions, such as a pod rolled
back, is named only while it fills a stop as the previous baseline of a
hand-over that did not take, or once no live audio replica has carried
the baseline's versions for `DEPARTED`, and then only one with the
highest live versions. A partition that keeps every replica carrying the
baseline's versions from the leader for longer than `DEPARTED` counts as
their departure. A replica that reports no versions ranks below every one
that does.

A complete rollback stops the outputs from the last report of the last
replica with the higher versions until the replica named after it
commits: `DEPARTED`, the leader's next decision within a contact window,
the decision lead the `Baseline` is stamped with, and the named replica's
retarget before its commit start. The rollback test bounds that gap at
`DEPARTED` plus 2 s. Those pages are whole on no member; every page
before and after is.

A replica whose key differs from the baseline's is in `version_mismatch`
and computes nothing. Its contact metrics give its `versions` and, in
`mismatch`, whether it is awaiting the hand-over or below the baseline's
versions; `/metrics` has them as `samplez_replica_versions` and
`samplez_replica_version_mismatch{reason="awaiting_hand_over"}` or
`{reason="below_baseline_versions"}`. A replica of lower versions reads
below the baseline's versions until it is named: in a rolling update one
pod restart per replica, and past `DEPARTED` until the leader's next
decision.

The leader logs at info level each `Baseline` it decides, with its rule
(`HandOver`, `Named`, `Renamed`), replica, key, versions and order, and
its `at_index` and P once stamped; and, once per change of the reason,
why the hand-over passes over a replica of another key: `revision`,
`key`, `versions`, `failed`, `candidate`, `baseline applied` or
`withheld`. Every audio replica logs each `Baseline` it applies, with the
key it takes for it and its own, and each change of its state.

## Ops endpoints

The ops listener serves `/healthz`, `/readyz` and `/metrics`,
unauthenticated, on its own thread and runtime. `/healthz` reads the RT
heartbeat alone, fails once it has not advanced for 1 s, and takes no
lock of the replicated state. `/readyz` and `/metrics` read the
replicated state on a blocking pool of at most 4 threads and answer 503
with a body naming the timeout when the read has not returned within 1 s,
so neither can hold up `/healthz`.

A replica lock held for more than 10 s ends the process with SIGABRT,
after a report on stderr starting `samplez: a replica lock was held past
its limit, aborting` ([Locks](ARCHITECTURE.md#locks)).

## Metrics

Besides the engine's and the service's own, `/metrics` carries the
replica's. These are labelled `node` per peer and dropped when the peer
leaves the membership, so a peer removed and added again starts from 0:

| Metric | Meaning |
| --- | --- |
| `samplez_replica_peer_silent_transitions_total` | changes from heard to held silent: no report within `contactWindowMs` while in contact |
| `samplez_replica_peer_longest_silent_gap_seconds` | the longest time from the peer's last report heard, or contact regained, to its next report over a silent gap, on the monotonic clock |
| `samplez_replica_peer_report_timeouts_total` | reports with no answer within `contactWindowMs` |
| `samplez_replica_peer_dials_total` | connections dialed, the first included |
| `samplez_replica_peer_reports_sent_total` | reports handed to the connection pool |
| `samplez_replica_peer_report_bytes_total` | their encoded bytes |
| `samplez_replica_peer_report_encode_cpu_seconds_total` | the CPU time of encoding them |
| `samplez_replica_peer_reports_answered_total` | answered reports by round trip, cumulative over `le` (0.0005, 0.001, 0.002, 0.004, 0.008 s, `+Inf`) |
| `samplez_replica_peer_report_round_trip_seconds_total` | the sum of their round trips |
| `samplez_replica_peer_clock_offset_seconds` | the peer's clock minus this replica's, from the report exchange with the lowest round trip of the last 64: the offset clock verification reads |
| `samplez_replica_peer_clock_offset_delay_seconds` | that exchange's one-way delay; verification accepts an offset within `clock_guard` plus it |

A silence that ends in a loss of contact, or before a first report,
counts as a transition but adds no gap. The peer's contact metrics also
carry these as `silent_transitions` and `silent_gap_max`, and
`last_report_age`, the time since its last report received directly;
relayed copies and regaining contact leave that unchanged.

Every audio replica's start: `samplez_replica_start_held{reason}`
([Readiness](#readiness)), and `samplez_replica_clock_verified`, whether
its clock is verified.

The leader's:

| Metric | Meaning |
| --- | --- |
| `samplez_replica_disagreements_total{kind}` | page disagreements: `no_majority`, a page without a strict majority among two or more hashes of one key; `cross_key`, a page replicas of two keys both committed whole. From 0 on every process start, only while that replica leads |
| `samplez_replica_apply_lead_raised_by{node}` | the replica that holds the apply lead at its cap ([Stamping](ARCHITECTURE.md#stamping-and-the-apply-lead)) |
| `samplez_replica_decision_lead_samples` | the lead decisions and capped commands are stamped with |

The contact loop's own cost: `samplez_replica_contact_ticks_total` counts
the report periods it ran (periods missed are skipped, not caught up), and
`samplez_replica_contact_cpu_seconds_total{part}` the CPU time of its
threads: `tick` for a period's state and reports, `decisions` for the
leader's decisions, `decode` and `receive` for the peers' reports.

Resync outcomes: `samplez_replica_resync_rebuilt_for_flows_total`,
`samplez_replica_resync_rebuilt_for_key_total`,
`samplez_replica_resync_rebuilt_writing_nothing_total` and
`samplez_replica_resync_refused_total` ([Resync](ARCHITECTURE.md#resync)).

## Timing

`samplez_replica::RaftTiming` sets the timers of leader election, and
`raft_config_with` turns it into openraft's configuration (`raft_config`
is the defaults); `validate` refuses a timing unless `0 < heartbeat <
election_min < election_max`.

| Setting | Default | Meaning |
| --- | --- | --- |
| `election_min` | 300 ms | the least of the random part of a follower's election timeout |
| `election_max` | 600 ms | the most of it, and the leader lease that precedes it |
| `heartbeat` | 100 ms | how often a leader heartbeats, and how long one AppendEntries may take |

openraft 0.9.26 gives every AppendEntries the heartbeat interval to
complete and has no other timeout for it. A follower campaigns after
`election_max` plus a draw from `election_min` to `election_max` of
silence from its leader: 900 to 1200 ms at the defaults. A real loss of
the leader leaves the set without one for that silence plus a vote round;
entries stamped in that gap miss their apply deadline and their blocks
are withheld.

`samplez_replica::contact::Timing` sets contact's timers;
`Timing::defaults` gives:

| Field | Default | Meaning |
| --- | --- | --- |
| `report_period` | 2 ms | p, how often a replica reports to its peers |
| `contact_window` | 8 ms | w, `contactWindowMs` |
| `hold` | 10 ms | D, `holdMs` |
| `margin` | 576 samples | `readerMarginSamples` |
| `clock_guard` | 288 samples | how far a replica's clock may be from the set's majority |
| `max_apply_lead` | 50 ms | the cap on the stamping lead |
| `resync_budget` | 50 ms | a resync handing over later than this from asking for the core warns |
| `lead_minute` | 60 s | the minute of slack the apply lead falls on |
| `prune_after_blocks` | 50 | the engine's `pruneAfterBlocks` |

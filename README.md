# samplez

Real-time multichannel audio engine with MXL I/O, written in Rust.

## Building

`docker/builder` holds the build environment: Rust, clang and libclang for
bindgen, a Release build of libmxl (MXL v1.1.0) installed under
`/opt/mxl`, and the tools of the RT gates: a pinned nightly with
`rust-src` (`$SAMPLEZ_NIGHTLY`), Valgrind and the Gungraun runner. CI
builds and tests inside it.

```sh
docker build -t samplez-builder docker/builder
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/w" -w /w \
  samplez-builder cargo test --workspace
```

## samplez-hop

`samplez-hop --config <file>` runs the engine from a JSON configuration:
N MXL input flows through a static gain matrix (one row per input
channel, one column per output channel) to M MXL output flows, on
libmxl's TAI timeline at 48 kHz. It stops on SIGINT or SIGTERM and
releases its writers, which deletes the output flows.

```json
{
  "blockSize": 48,
  "readerMarginSamples": 576,
  "libmxl": "/opt/mxl/lib/libmxl.so",
  "domains": [{ "name": "a", "path": "/run/mxl/a" }],
  "inputs": [{ "name": "in", "domain": "a",
               "flowId": "5fbec3b1-1b0f-417d-9059-8b94a47197ed", "channels": 2 }],
  "outputs": [{ "name": "out", "domain": "a",
                "flowId": "6fbec3b1-1b0f-417d-9059-8b94a47197ed",
                "label": "hop out", "group": "hop", "role": "Audio", "channels": 1 }],
  "matrix": [[0.5], [0.5]],
  "rt": { "enabled": true, "priority": 70, "rtCpu": "auto" }
}
```

Further keys and their defaults: `maxCatchupBlocks` 2, `pruneAfterBlocks`
50, `rampSamples` 480, `holdMs` 10, `contactWindowMs` 8,
`historySlackMs` 50, `resyncBudgetMs` 50, per input
`offsetSamples` 0, per output `loudnessChannels` (one of `L`, `R`, `C`,
`Ls`, `Rs`, `LFE` per channel, weighted per ITU-R BS.1770-5 Table 3;
without it every channel weighs 1).

Each output's status carries its meters: sample peak, RMS and true peak
per channel over the last 100 ms, the largest true peak, and momentary,
short-term and integrated loudness and loudness range per EBU R 128.
RMS is referenced to a sine as in AES17: the root mean square times the
square root of 2, so a full-scale sine reads 1.0 (0 dB FS).
`Engine::reset_loudness` restarts the measurement at a sample index. A
meter record lost on the way from the RT thread, samples not metered
after an overrun, or an infinite or NaN sample mark integrated loudness
and loudness range invalid until the next reset.
After samples that were not metered, the true peak counts only
interpolated values computed from samples after them.

Every change to what the engine computes is a log entry applied at a
sample index: a gain change, `InputDown` (the input is silent from the
index on, whatever is readable), `InputUp` (it fades in from the index),
`Route` (it crossfades to another flow over the ramp length from the
index) and `Strip` (one parameter of an input's channel strip, a
`samplez_dsp::StripParam`, ramping over the ramp length). Each sets an
absolute target, so an entry applied again once its ramp has ended
changes nothing; applied again while it ramps, it starts the ramp over
from where it is. `InputUp` and `Route` need a new input generation:
the engine builds it when the entry is appended, one at a time, and
swaps it in at the entry's index. The status reports the 99.99th
percentile of the build times and buildLead, how far ahead such entries
are stamped. `Engine::start` keeps the log itself, stamping `InputDown`
when an input is pruned and `InputUp` once it has been on time for a
second, and each `InputUp` or `Route` at least buildLead after the one
before; `Engine::start_replicated` takes entries through `append`,
`commit` and `truncate` and sends those reports out instead.

Each input of 1 to 8 channels has a strip: trim, per-leg trim and
polarity, high-pass, a four-band equaliser, gate and compressor
(`samplez_dsp::strip`). It runs on every block computed, after the
input's copy, fade-in or crossfade and before the matrix, and passes
its input unchanged until an entry sets it. Its ramps advance over
skipped samples, as the matrix's do; its filter and envelope state
follows the samples it processed, so after a skip or a silenced read it
differs from that of an engine that computed them. Strips are not part
of a state copy yet, so until then a replica that skipped converges
with the others only through its strips' time constants. An entry for
an input wider than 8 channels, or one its strip refuses, is refused
when appended (`append`) or set (`set_strip`); one that a strip on the
RT thread still refuses is counted in `strip_refused`, not applied, and
marks the engine diverged.

`Engine::start_replicated` takes the set's voter count. With one voter
the engine commits every block at once. With more, each block of each
output gets a status word (`fenced`, `held`, `committed`, `withheld`),
moved by compare-and-swap and sent as `StatusChange`s; the replicated
state publishes F (`set_fence`), the rank, and evidence (`commit_through`)
and withholds blocks (`withhold`). An output commits only from its commit
start (`commit_from`), the first page boundary of 1024 samples in a block
it computes, and stops at `stop_at`; such a block is split and its status
covers one range. A healthy block with F past it and nothing held before
it commits at once; any other waits in the output's hold ring of
`D + w + 2B` samples (`holdMs`, default 10, and `contactWindowMs`, default
8) and is committed later in one write, or withheld and never written. A
withheld block, an overrun skip or a full ring withdraws the replica on
every output: it then commits only on evidence, from the next page
boundary. Each block carries a digest of what degraded it: inputs whose
read failed, entries applied without their generation and unresolved
entries (`unresolved`). A writer commits a range of up to the ring's
length, and an output whose flow ring is not twice that is not
realised. Each page of each realised output the engine computes whole,
in every commit mode, is hashed (XXH3-64 over its samples' f32 bit
patterns, channels interleaved per index) and sent as a `PageHash`
once every block overlapping it is final, with the fingerprint of the
log entries applied in it and whether the page was committed whole.

For a resync, `Engine::copy_core` has the RT thread copy the engine's
core after the next block k0 into a buffer allocated off it: the matrix
with its ramps, the pending entries and those applied in the page in
progress, each input's log state and fades, the entries waiting for a
generation and the pending generation's entry id, and the unresolved
entries. Meters, hold rings, statuses and readers stay with the replica.
History rings (`History` in samplez-dsp) hold a delay plus
`historySlackMs` of slack as relaxed atomic bit patterns; the RT thread
publishes the block it starts before it stores any history sample, and a
copy is kept only while the RT thread has not started the last block of
the slack after k0. The engine keeps no delay line yet, so a copy holds
the core alone. With more than one voter every input flow must offer
`resyncBudgetMs` of history beyond the read window; an input whose flow
is shorter is refused with an error naming the `history_duration` it
needs. A lone voter has no donor to resync from and needs none.

`Engine::replay` builds a replay clone from a donor's core: the engine's
block computation off the RT thread, its readers on a new instance per
input domain reading the blocks after k0 from the flows' history, its
entries and unresolved-entry decisions after the core's last log id
taken from the caller, and every page it computes whole hashed. A core
with a generation-needing entry in flight (`InputUp`, `Route`, a pending
generation or a crossfade) is not replayed. `Engine::hand_over` runs the
clone up to the newest block due and passes its state to the RT thread
by owned pointer; the RT thread installs it before the next block if it
has not started that block and still holds the generation the core
names, keeping pending entries the core did not know, and computes on
from it. Otherwise the state comes back refused and the clone runs on.
The status reports the replay speed s, blocks per block period, measured
at start-up on silence.

The engine runs on libmxl's clock (CLOCK_TAI) and detects a step of it
against CLOCK_MONOTONIC at each wake, when it also samples the kernel
TAI offset (CLOCK_TAI - CLOCK_REALTIME); the status reports both, and
whether `adjtimex` finds the kernel clock unsynchronised. A block index
is never computed twice: after a backward step the loop wakes once per
block period without computing until the clock is due at the block
after the last one again. In replicated mode a detected step fences the
blocks after it until the replicated state clears it
(`clear_step_fence`), or with one voter until its next clean
self-report; a backward step also withholds every block held, and an
unsynchronised kernel clock leaves blocks without a digest.

Real-time set-up reports each step and runs degraded rather than failing:
effective capabilities raised from the permitted set, `mlockall` with
`MCL_FUTURE` only under `CAP_IPC_LOCK` or an unlimited `RLIMIT_MEMLOCK`,
SCHED_FIFO for the RT thread, FTZ/DAZ and blocked signals on it.
`rtCpu: auto` pins the RT thread to the last allowed CPU only when
`SAMPLEZ_CPU_REQUEST_MILLICORES` and `SAMPLEZ_CPU_LIMIT_MILLICORES` are
equal whole CPUs matching the allowed set and the cgroup CPU quota is
unlimited or the same size.

The binary is `src/bin/samplez-hop.rs`, built with the library.

## Real-time requirements

The RT thread does not allocate once its loop runs (`tests/no_alloc.rs`
checks it). The loop also passes RealtimeSanitizer (`tests/rtsan.rs`,
built with `-Zsanitizer=realtime --cfg samplez_rtsan` on the nightly),
which aborts on allocations, pthread locks, blocking syscalls and the
sleeps its runtime intercepts. Allowed by call site are the calls that
block by design: libmxl's sync-group wait, `mxlSleepUntil` and the
`FUTEX_WAKE` of the output commit. RealtimeSanitizer does not see an
uncontended std lock, which makes no libc call, nor
`std::thread::sleep`, which calls `clock_nanosleep`; clippy denies those
and the other blocking std primitives listed in `clippy.toml` in the RT
modules. `cargo bench --bench rt` in samplez, samplez-dsp and
samplez-mxl fails when the instruction count of the hold ring's paths,
the page hashes, the strip calls of the RT loop, a block's DSP, the
matrix copy of the core, a block's history stores or an output commit,
gap zero-fill included, exceeds the limit in the benchmark. One failure
path is
excepted from the no-allocation rule, and its probe is allowed by call
site under RealtimeSanitizer: while an input is late, libmxl's flow
validity check, one `stat()` and path allocations, can run on the RT
thread. It ends when the input is pruned, after `pruneAfterBlocks`
blocks of a frozen head, or twice the producer's
`maxCommitBatchSizeHint` plus `readerMarginSamples` when that is longer.

The host needs swap off and `vm.compact_unevictable_allowed=0`, so that
neither swapping nor compaction moves the locked pages of the RT
thread. MXL domains belong on tmpfs: on a disk-backed file system a
commit into an output's shared mapping can wait for the page's
writeback, and the loop then skips blocks.

## Licence

Apache-2.0. The builder image carries the licence files of MXL and of the
libraries statically linked into libmxl under `/opt/mxl/share/licenses`.

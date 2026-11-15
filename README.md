# samplez

Real-time multichannel audio engine with MXL I/O, written in Rust.

## Building

`docker/builder` holds the build environment: Rust, clang and libclang for
bindgen, and a Release build of libmxl (MXL v1.1.0) installed under
`/opt/mxl`. CI builds and tests inside it.

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
50, `rampSamples` 480, per input `offsetSamples` 0, per output
`loudnessChannels` (one of `L`, `R`, `C`, `Ls`, `Rs`, `LFE` per channel,
weighted per ITU-R BS.1770-5 Table 3; without it every channel weighs 1).

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
checks it), with one failure path excepted: while an input is late,
libmxl's flow validity check, one `stat()` and path allocations, can
run on the RT thread. It ends when the input is pruned, after
`pruneAfterBlocks` blocks of a frozen head, or twice the producer's
`maxCommitBatchSizeHint` plus `readerMarginSamples` when that is longer.

The host needs swap off and `vm.compact_unevictable_allowed=0`, so that
neither swapping nor compaction moves the locked pages of the RT
thread. MXL domains belong on tmpfs: on a disk-backed file system a
commit into an output's shared mapping can wait for the page's
writeback, and the loop then skips blocks.

## Licence

Apache-2.0. The builder image carries the licence files of MXL and of the
libraries statically linked into libmxl under `/opt/mxl/share/licenses`.

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
50, `rampSamples` 480, per input `offsetSamples` 0.

Real-time set-up reports each step and runs degraded rather than failing:
effective capabilities raised from the permitted set, `mlockall` with
`MCL_FUTURE` only under `CAP_IPC_LOCK` or an unlimited `RLIMIT_MEMLOCK`,
SCHED_FIFO for the RT thread, FTZ/DAZ and blocked signals on it.
`rtCpu: auto` pins the RT thread to the last allowed CPU only when
`SAMPLEZ_CPU_REQUEST_MILLICORES` and `SAMPLEZ_CPU_LIMIT_MILLICORES` are
equal whole CPUs matching the allowed set and the cgroup CPU quota is
unlimited or the same size.

## Licence

Apache-2.0. The builder image carries the licence files of MXL and of the
libraries statically linked into libmxl under `/opt/mxl/share/licenses`.

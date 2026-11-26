# samplez

A real-time multichannel audio engine on MXL flows, written in Rust, with
a replicated mode in which several engines compute the same output from
one replicated log. It reads 48 kHz audio flows from MXL domains on
libmxl's TAI timeline, processes them in blocks of 16 to 1024 samples
through a channel strip per input and one gain matrix, and writes each
output to its own MXL flow at the sample index it read. Every change to
what it computes is a log entry applied at a sample index.

samplez is a set of libraries and one example binary, `samplez-hop`. How
it is built is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Features

- MXL input and output: up to 64 inputs, 64 outputs and 64 meters per
  engine, through libmxl v1.1.0 loaded at run time.
  [Configuration](docs/USAGE.md#configuration)
- Sample-accurate changes: every gain, route, strip parameter and other
  setting is an entry applied at a sample index, with ramps whose values
  depend on the index alone. [Log entries](docs/USAGE.md#log-entries)
- Input selection and routing: legs picked by flow channel, re-routing to
  another flow with a crossfade, fade-in and silencing at an index.
  [Inputs](docs/USAGE.md#inputs)
- Timed gain transitions: a fade with a start, a length and a value it
  starts from. [Timed gain transitions](docs/USAGE.md#timed-gain-transitions)
- A channel strip per input of up to 8 channels: trim, per-leg trim and
  polarity, high-pass, four-band equaliser, gate and compressor with a
  parallel mix. [Channel strip](docs/USAGE.md#channel-strip)
- Delay lines of up to 5 s per input and per output.
  [Input delay](docs/USAGE.md#input-delay), [Output delay](docs/USAGE.md#output-delay)
- Line-up tone generators of 20 Hz to 20 kHz with an ident.
  [Tone generator](docs/USAGE.md#tone-generator)
- Duck groups: 8 detectors keyed by inputs after their strips, ducking the
  matrix coefficients their masks name. [Duck groups](docs/USAGE.md#duck-groups)
- A channel strip, a delay line and a true-peak limiter per output of up
  to 8 channels. [Output strip](docs/USAGE.md#output-strip),
  [Output limiter](docs/USAGE.md#output-limiter)
- Meters: sample peak, RMS, true peak, EBU R 128 loudness and correlation
  per output and meter, and per input, before and after its strip.
  [Meters](docs/USAGE.md#meters)
- A real-time thread that does not allocate or lock once its loop runs,
  run `SCHED_FIFO` with locked memory on a CPU of its own, and degraded
  with a report when the privileges are missing.
  [Real-time set-up](docs/USAGE.md#real-time-set-up)
- Replicated mode: a Raft log over openraft, per-block commit, XXH3-64
  page hashes compared across replicas, resync of a diverged replica from
  a donor's engine core, and rollouts that hand the baseline over by a
  compatibility key. [Running a replica set](docs/REPLICAS.md),
  [Replicated mode](docs/ARCHITECTURE.md#replicated-mode)
- A service layer for products: API, machine, ops and NMOS listeners,
  TLS, the auth modes `none`, `token`, `proxy` and `oidc`, `/healthz`,
  `/readyz`, Prometheus `/metrics` and WebSocket fan-out.
  [Service layer](docs/USAGE.md#service-layer)
- NMOS: IS-04 v1.3 and IS-05 v1.2 with MXL senders and receivers per
  BCP-007-03. [NMOS](docs/USAGE.md#nmos)

## Requirements

- x86_64 Linux. The matrix and true peak have an AVX2 path and a scalar
  one computing the same bits.
- libmxl v1.1.0 (MXL v1.1.0), loaded from the path the configuration
  names.
- For the RT thread: `CAP_SYS_NICE` or an `RLIMIT_RTPRIO` of at least
  its priority (70 by default), and `CAP_IPC_LOCK` or an unlimited
  `RLIMIT_MEMLOCK`; without them it runs degraded. Swap off and `vm.compact_unevictable_allowed=0` on the host,
  and MXL domains on tmpfs. [Real-time set-up](docs/USAGE.md#real-time-set-up)
- Building: the builder image in `docker/builder` (Rust, libmxl under
  `/opt/mxl`). [docs/BUILDING.md](docs/BUILDING.md)

## Quick start

`samplez-hop` runs the engine from a JSON file with a static matrix. This
builds the builder image, builds `samplez-hop` in it and runs it against
an MXL domain in `/dev/shm`, mixing a stereo input flow to one mono
output flow.

```sh
docker build -t samplez-builder docker/builder
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/w" -w /w \
  samplez-builder cargo build --locked --release --bin samplez-hop
mkdir -p /dev/shm/mxl/a
cat > hop.json <<'JSON'
{
  "libmxl": "/opt/mxl/lib/libmxl.so",
  "domains": [{ "name": "a", "path": "/mxl/a" }],
  "inputs": [{ "name": "in", "domain": "a",
               "flowId": "5fbec3b1-1b0f-417d-9059-8b94a47197ed", "channels": 2 }],
  "outputs": [{ "name": "out", "domain": "a",
                "flowId": "6fbec3b1-1b0f-417d-9059-8b94a47197ed",
                "label": "hop out", "group": "hop", "role": "Audio", "channels": 1 }],
  "matrix": [[0.5], [0.5]]
}
JSON
docker run --rm --name samplez-hop --user "$(id -u):$(id -g)" \
  --ulimit rtprio=99 --ulimit memlock=-1 \
  -v "$PWD:/w" -v /dev/shm/mxl/a:/mxl/a -w /w \
  samplez-builder target/release/samplez-hop --config hop.json
```

In another shell:

```sh
docker run --rm -v /dev/shm/mxl/a:/mxl/a samplez-builder mxl-info -d /mxl/a -l
```

The output flow `6fbec3b1-...` exists from the start; until a producer
writes the input flow, `samplez-hop` logs that the input is not attached
and the output carries silence. It logs each real-time set-up step at
start (`samplez-hop: rt ...`); the `--ulimit` flags let the RT thread run
`SCHED_FIFO` with locked memory. `docker stop samplez-hop` stops it, and
it deletes its output flow on the way out.

Every configuration key is in [Configuration](docs/USAGE.md#configuration).

## Licence

Apache-2.0. The builder image carries the licence files of MXL and of the
libraries statically linked into libmxl under `/opt/mxl/share/licenses`.

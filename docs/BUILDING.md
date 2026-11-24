# Building and testing samplez

How samplez is built, tested and released. [USAGE.md](USAGE.md) describes
how it behaves.

## The builder image

`docker/builder` holds the build environment: Rust 1.96.0 with clippy and
rustfmt, clang and libclang for bindgen, a Release build of libmxl (MXL
v1.1.0) under `/opt/mxl` with its tools `mxl-info` and `mxl-data-probe`,
and the tools of the RT gates: a pinned nightly with `rust-src`
(`$SAMPLEZ_NIGHTLY`), Valgrind and the Gungraun runner. CI builds it and
builds and tests inside it. `.github/workflows/builder.yml` publishes it
from `main` as `ghcr.io/danielclasen/samplez-builder`, tagged `latest`,
`mxl-<sha>-rust-<version>` and the commit.

## Build and test

```sh
docker build -t samplez-builder docker/builder
docker run --rm --user "$(id -u):$(id -g)" \
  -e SAMPLEZ_REQUIRE_PATHS=avx2 -v "$PWD:/w" -w /w \
  samplez-builder cargo test --workspace --locked
```

`SAMPLEZ_REQUIRE_PATHS=avx2` makes a matrix test fail rather than skip the
AVX2 path on a CPU without it; CI sets it on every test run.

`cargo test --workspace` leaves out what needs more than a container:

- The end-to-end scenarios (`tests/hop.rs`) run `samplez-hop` against
  libmxl flows and need `SCHED_FIFO` and locked memory to hold their
  timing. With `SAMPLEZ_REQUIRE_RT=1` they fail rather than run without:

  ```sh
  docker run --rm --user "$(id -u):$(id -g)" --ulimit rtprio=99 --ulimit memlock=-1 \
    -e SAMPLEZ_REQUIRE_RT=1 -v "$PWD:/w" -w /w \
    samplez-builder cargo test --locked --test hop
  ```

- The NMOS schema test validates every document `samplez-nmos` serves
  against the AMWA schemas at the commits pinned in
  `crates/samplez-nmos/tests/specs.txt`, checked out by `fetch-specs.sh`
  into `target/nmos-specs` (or the directory `SAMPLEZ_NMOS_SPECS` names):

  ```sh
  crates/samplez-nmos/tests/fetch-specs.sh
  docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/w" -w /w \
    samplez-builder cargo test --locked -p samplez-nmos --test schemas
  ```

- The kernel clock tests of `samplez-replica` (`--test vm_clock`: a TAI
  offset step and a leap second) step the clock, so CI runs them in a VM.

## RT gates

The RT thread neither allocates nor locks once its loop runs. Four gates
hold that:

- `tests/no_alloc.rs` fails on an allocation in the loop.
- RealtimeSanitizer (`tests/rtsan.rs` and the lib tests built with
  `--cfg samplez_rtsan`) aborts on allocations, pthread locks, blocking
  syscalls and the sleeps its runtime intercepts. Allowed by call site
  are `mxlSleepUntil` and the `FUTEX_WAKE` of the output commit.
- It does not see an uncontended std lock, which makes no libc call, nor
  `std::thread::sleep`; clippy denies those and the other blocking std
  primitives listed in `clippy.toml` in the RT modules. The lint probe
  (`--cfg samplez_lint_probe`) puts a lock and a sleep in each RT crate,
  and CI fails when clippy passes it.
- `cargo bench --bench rt` in samplez, samplez-dsp and samplez-mxl
  (Gungraun) fails when the instruction count of an RT path exceeds its
  limit: the hold ring, page hashes, strips, a block's DSP, limiters,
  delay lines, duck detectors, input meters and the gap decision, a
  skip's metering, the core's matrix and detector copies, a duck batch,
  history stores and an output commit. A limit is the count measured in
  the builder image plus 10%.

```sh
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/w" -w /w \
  samplez-builder bash -c '
    RUSTFLAGS="-Zsanitizer=realtime --cfg samplez_rtsan" \
      cargo "+$SAMPLEZ_NIGHTLY" test --locked -Zbuild-std \
      --target x86_64-unknown-linux-gnu --lib --test rtsan
    cargo bench --locked -p samplez -p samplez-dsp -p samplez-mxl --bench rt'
```

## NMOS conformance

CI runs the AMWA nmos-testing suites IS-04-01 (unicast and multicast
DNS-SD), IS-05-01 and BCP-007-03-01 against the example node (`cargo run
-p samplez-nmos --example nmos-node`) with `tests/nmos/conformance.sh`,
restarting it between suites and failing when a resource id changes
across a restart. The tool runs from the image `.github/nmos-testing.image`
names by digest, built from the nmos-testing commit in
`.github/nmos-testing.ref`, which its revision label must carry; no run
builds it. It validates against the specification commits pinned in
`tests/nmos/specs.txt`. Results other than Pass are accepted only as
listed, with their reason from the tool's source, in
`tests/nmos/expected.json`, and each suite has to run at least the number
of tests listed there. Of the Manual tests, IS-04-01 test_22 (ids persist
over a restart) is the restart check; BCP-007-03-01 test_15 (MXL reads
and writes start and stop on activation) needs a product with an engine
and is not covered here.

## CI

`.github/workflows/ci.yml` runs every job in the builder image; `ci` is
the required check and passes only when every job did.

| Job | What it runs |
| --- | --- |
| `lint` | `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the lint probe, the lockfile release ages and `cargo deny check licenses` against `deny.toml` |
| `debug` | `cargo test --workspace`, the NMOS schema test and the end-to-end scenarios on the debug build |
| `release` | `cargo test --workspace --release` |
| `e2e-release` | the end-to-end scenarios on the release build |
| `vm-clock` | the kernel clock tests in a VM |
| `rt-gates` | RealtimeSanitizer and the instruction-count benches |
| `nmos` | the NMOS conformance suites |

A crates.io release `Cargo.lock` gains has to be 14 days old unless
`.github/lockfile-age/allow.txt` lists it.

## Releases

release-please (type `rust`, `release-please-config.json`) versions the
whole workspace and opens a release PR from the Conventional Commits on
`main`; before 1.0 a breaking change bumps the minor and a `feat` the
patch. Merging the release PR tags the release and updates
`CHANGELOG.md`.

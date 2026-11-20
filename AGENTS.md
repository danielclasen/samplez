# Contributor notes (agents and humans)

Rules for working in this repository. Read them before opening a PR or
running an automated assistant against this tree. `README.md` describes
what samplez does in detail and wins for anything about behaviour; this
file says how to change it without breaking what the README promises.

## STOP. Use a git worktree.

Before ANY mutation of this repository -- edit, write, commit, branch
create, push, rebase, `gh pr create` -- the very first action of the
session is to set up a dedicated `git worktree`. This rule has no
exceptions. There is no change small enough to skip it: typos,
single-line fixes, doc-only PRs, even edits to this file itself all
require the same worktree dance.

The repository is worked on by several sessions and editors at once. Two
writers in the same tree corrupt staging state, step on each other's
branches, and lose work without warning.

Worktrees ALWAYS live under `<repo>/.claude/worktrees/`, which
`.gitignore` lists. Not next to the repo, not in `/tmp`, not anywhere
else.

The preferred path is to delegate the mutation to a sub-agent with
`isolation: "worktree"` on the Agent call. The harness then creates
`<repo>/.claude/worktrees/agent-<id>/` with a unique id, so two
concurrent sub-agents never share a path. Do not assume a worktree from
earlier in the session is still mounted.

For a manual worktree, pick a short random tag so two sessions on the
same topic never collide:

```sh
git fetch origin
id=$(openssl rand -hex 4)
git worktree add .claude/worktrees/<topic>-$id -b <topic> origin/main
cd .claude/worktrees/<topic>-$id
```

All later edits, commits and pushes happen from the worktree. The only
thing allowed in the main checkout is read-only inspection: `git log`,
`git diff`, `git status`, `gh pr view`, reading files, grep and find.

After the PR has merged, drop a manual worktree, its branch and the
stale remote ref:

```sh
git worktree remove .claude/worktrees/<topic>-<id>
git branch -D <topic>
git fetch --prune origin
```

## Working rules

### Think before coding

Do not assume, do not hide confusion, surface tradeoffs. State
assumptions explicitly; if uncertain, ask. If several interpretations
exist, present them rather than picking one silently. If a simpler
approach exists, say so. If something is unclear, stop, name what is
confusing, and ask.

### Read before you write

Read the exports, immediate callers and shared utilities before adding
code. "It looks orthogonal" is dangerous here: most of the engine's
state is replicated, copied in a core or hashed into a page. If a
structure's reason is unclear, analyse it; if it stays unclear, ask.

### Simplicity first

The minimum code that solves the problem, nothing speculative. No
features beyond what was asked, no abstractions for single-use code, no
configurability nobody requested, no error handling for impossible
states. If 200 lines could be 50, rewrite it.

### Conventions

Conform to the codebase even where personal preference differs. If a
convention is genuinely harmful, say so; never silently fork it.

### Fail loudly

"Completed" is wrong if anything was silently skipped. "Tests pass" is
wrong if any were skipped, or if the suite that covers the change only
runs in CI and did not run. Indicate uncertainty by default.

### Leave it cleaner

Whatever gets touched comes out more adherent to these rules than it
went in: code, comments, structure, `README.md` freshness and the
factual correctness of everything stated. Documentation that can be
shortened is shortened as soon as it is encountered, wherever the code
can carry the truth instead.

Never write, and delete on sight: task references, todo-list items,
implementation phases or cycles from an agent session; references to
private repositories, private issue trackers or anything else outside
this public repository; SPDX headers or copyright lines on new files,
unless preserved from an external source. Only GitHub issues and PRs
that are themselves public may be referenced.

Do not invent new `.md` files. Write documentation when asked for it.

### Tests

Write tests at every level the repository has: unit tests beside the
code, integration tests under `tests/` and `crates/*/tests/`, the
end-to-end scenarios (`tests/hop.rs`), the RT gates and the benchmarks.
A test must fail without the change and pass with it, and capture why a
behaviour matters, not only what it does. A test that cannot fail when
the logic changes is broken. Update tests that drifted; delete fully
outdated ones. Never add `#[ignore]`, a skip or a timing-based
tolerance to get a test green.

### Typography

ASCII only, everywhere: code, comments, commit messages, PR
descriptions, prose. No em-dash, en-dash, typographic quotes, arrows,
ellipsis character or non-breaking spaces. Use `-`, `--`, `"`, `'`,
`->`, `...`.

### Documentation

- Keep `README.md` and code comments tight. State facts; do not
  speculate.
- Describe what a component does and needs, not what it "should be used
  for" or "is ideal for".
- Do not invent API behaviour. If it cannot be verified from the source,
  a run, or authoritative documentation, leave it out.
- A change to behaviour the README describes updates the README in the
  same PR.

## Architecture

samplez is a Cargo workspace of six crates. Only x86_64 Linux is built.

| Crate | Path | Contents |
| --- | --- | --- |
| `samplez` | `src/` | the engine: configuration (`config.rs`), supervisor (`engine.rs`), RT loop (`rt_loop.rs`), RT set-up (`rt.rs`), block statuses and hold rings (`blocks.rs`), entry folding (`batch.rs`), targets (`targets.rs`), duck groups (`ducks.rs`), supervisor-side metering (`meters.rs`), state transfer (`transfer.rs`), and the example binary `samplez-hop` (`src/bin/samplez-hop.rs`) |
| `samplez-dsp` | `crates/samplez-dsp/` | planar buffers, gain ramps, the matrix with duck masks and depths (`matrix.rs`, AVX2 kernels in `simd.rs`), the channel strip (`strip.rs`, `svf.rs`, `dynamics.rs`), the input delay and history rings (`delay.rs`, `history.rs`), the output limiter (`limiter.rs`), the duck detector (`duck.rs`), meters, true peak and loudness (`meter.rs`, `true_peak.rs`, `kweight.rs`, `loudness.rs`) |
| `samplez-mxl` | `crates/samplez-mxl/` | MXL input and output on libmxl v1.1.0 loaded at run time (`input.rs`, `output.rs`, `lookup.rs`), libmxl's clock and index conversions (`time.rs`), RealtimeSanitizer scopes (`rtsan.rs`) |
| `samplez-replica` | `crates/samplez-replica/` | a product's state replicated over openraft, and the replicated engine around it |
| `samplez-service` | `crates/samplez-service/` | the non-RT service layer of products |
| `samplez-nmos` | `crates/samplez-nmos/` | IS-04 v1.3 Node API and registration, IS-05 v1.2 Connection API for MXL senders and receivers per BCP-007-03; links no libmxl |

### The engine

`Engine::start` runs the engine alone, stamping its own entries;
`Engine::start_replicated` runs it under `samplez-replica`. Either way
two threads do the work:

- The **supervisor** (`src/engine.rs`) is the one thread that makes
  instance-mutating libmxl calls. It builds input generations and output
  writers, hands them to the RT thread through the command ring, destroys
  what comes back through the return ring, prunes and re-adds inputs,
  retries output creation, and turns RT telemetry into status, meters
  (`src/meters.rs`) and log lines.
- The **RT thread** (`src/rt_loop.rs`) runs the block loop on libmxl's
  TAI clock, `SCHED_FIFO` with locked memory (`src/rt.rs`). Per block of
  16 to 1024 samples it reads each input flow at the block's sample
  index (`samplez-mxl` `input.rs`), applies the entries due in the block
  (folded per log entry by `src/batch.rs`), computes the input delay
  lines, channel strips, duck detectors (`src/ducks.rs`), the gain
  matrix with its ramps, masks and depths and each output's true-peak
  limiter, writes each output at the index it read (`output.rs`), and
  sends meter records and block statuses back. Nothing on it allocates,
  frees or drops a libmxl object once the loop runs.

Every change to what the engine computes is an entry applied at a sample
index (README "Log entries"). Targets (`src/targets.rs`) are the
absolute state a product holds; the engine only receives the entries
between two target sets.

### Replicated mode

`samplez-replica` runs a product's state over openraft and drives a
replicated engine from it. README "Replicated mode" is the reference.

- **Log and state machine.** `log_store.rs` keeps the log, vote and
  snapshots on the replica's volume (`Store`). `state_machine.rs`
  applies entries: command dedup (`dedup.rs`), the product's state, and
  the engine entries derived from it. A product implements the `Product`
  trait there: `apply`, `save`/`load`, `config_hash`, `targets` and
  `DRIVES_ENGINE` for a product that drives the engine, and
  `DERIVATION_VERSION`. `entry.rs`
  defines what the log carries and `PROTOCOL`.
- **Contact loop.** `contact.rs` runs the leader-independent contact
  between voters: side reports each report period (`report.rs`), the
  fence F, silence held and relayed, and commit knowledge. It reaches the
  engine through the `Audio` trait. Replica states and the rules that
  move them without a leader are in `state.rs`; withholding in
  `withhold.rs`; clock checks in `clock.rs` and `verify.rs`; command
  scheduling and the apply lead in `schedule.rs`.
- **Leader decisions.** `baseline.rs` names the baseline replica,
  renames it and hands it over in a key-changing rollout; `verdict.rs`
  gives the page verdicts; `decide.rs` decides unresolved entries;
  `rank.rs` gives each replica's rank per block. All of them go into the
  log, so a new leader decides the same from the applied log.
- **Rollout.** `rollout.rs` holds the compatibility key, the target
  revision read from the audio StatefulSet (`rollout::follow`) and
  `hand_over_at`, which puts a hand-over's page P on a page and block
  boundary.
- **Resync and core copy.** `src/transfer.rs` copies the engine's core
  after a block on the RT thread's behalf, encodes and decodes it
  (`CoreCopy`), and replays it (`Engine::replay`, `Engine::hand_over`).
  `crates/samplez-replica/src/resync.rs` moves a diverged replica onto a
  committing donor's core over the peer listener (`peer.rs`, `net.rs`).
  `Engine::install_product` rebuilds a core from product state.
- **Bridge.** `bridge.rs` implements `Audio` over a replicated `Engine`
  (`EngineAudio`): F and unresolved entries go to the engine's control
  ring, records of applied log entries to its entry ring by log id, and
  reports read status words, the processed index, clock flags and the
  latest page hashes.
- **Start-up.** `bootstrap.rs` (cluster id through a marker ConfigMap),
  `membership.rs` (voters and learners), `horizon.rs`.
- **Locks.** `lock_watch.rs` wraps the non-RT locks that record their
  holders and runs the watchdog.

### Service layer

`samplez-service` is everything a product serves off the RT thread:
listeners and routers (`server.rs`, `Service`), TLS (`tls.rs`), the
auth modes `none`, `token`, `proxy` and `oidc` (`auth.rs`, `oidc.rs`),
the unauthenticated ops listener with `/healthz`, `/readyz` and
`/metrics` (`ops.rs`, `metrics.rs`), the OpenAPI document of `/api/v1`
with its drift check (`openapi.rs`), WebSocket fan-out (`ws.rs`), and
the `ReplicatedState` trait a product's replicated state is reached
through (`replica.rs`).

## Invariants

Each of these is load-bearing. A change that weakens one is a design
change: say so in the PR and get it agreed first.

- **Replicas compute bit-identical output from the same ordered log.**
  Everything the engine computes is a function of the sample index, the
  inputs and the log entries applied at their indices: no wall-clock
  time, thread timing, block size or SIMD path decides a sample. The
  matrix and limiter compute the same bits scalar or AVX2 and at any
  block size (README "Timed gain transitions", "Output limiter"). Pages
  are compared across replicas by XXH3-64 over their samples' f32 bit
  patterns.
- **No allocation and no locks on the RT thread.** The gates:
  `tests/no_alloc.rs` (nothing allocated or freed through the global
  allocator once the loop runs),
  RealtimeSanitizer (`tests/rtsan.rs` and the lib tests built with
  `--cfg samplez_rtsan`), clippy's disallowed types and methods in the RT
  modules (`clippy.toml`, with a lint probe in CI proving the lint
  fires), and the Gungraun instruction-count benchmarks
  (`benches/rt.rs`, `crates/samplez-dsp/benches/rt.rs`,
  `crates/samplez-mxl/benches/rt.rs`). A benchmark's hard limit is the
  count measured in the builder image plus 10%, rounded up; a change
  that moves a count re-measures it and resets the limit the same way,
  and says why in the PR.
- **Lock order.** The contact loop holds the `Contact` lock while it
  reads the store, so no code holds a store lock and waits for
  `Contact`. No path holds either across disk or network I/O or a wait.
  `Product` methods, and the closures the store's reads take, run under
  the store's locks: they must not call back into `Replica` or `Store`
  (such a call panics, naming the lock), nor wait for an entry to be
  applied. The watchdog aborts the process when a watched lock is held
  past 10 s, in tests too. README "Ops endpoints and the lock watchdog",
  `crates/samplez-replica/src/lock_watch.rs`.
- **A resynced replica matches its peers from its first page.** A
  replay of a donor's core is handed over at a block boundary and
  computes the donor's bits from there. A rebuild from product state
  (`Engine::install_product`) matches as far as gains, inputs, strips
  and limiters go; the differences README "Starting a replica" lists
  (duck detector state, a mask crossfade or depth ramp in progress) cost
  one diverged page and a resync. Do not widen that list.
- **Tests compare pages only once verdicts confirm them, never by
  timing.** A comparison waits for a verdict past the page and leaves
  out pages a silenced block touched (`comparable` in
  `tests/transfer.rs`); a loaded host narrows the set compared, it
  never changes what the set holds. No sleep decides that a page is
  final.
- **Retries only for runs the engine's own counters prove disturbed.**
  `undisturbed` in `tests/common/mod.rs` and `tests/hold.rs` reruns a
  scenario at most four times, and only when an overrun skip, a ring
  overflow the scenario did not cause, or a late input read is recorded
  for that run. An undisturbed run must pass; every run disturbed fails
  the test.

## Versions and compatibility

Three versions decide whether two replicas may run side by side:

- `samplez::COMPUTE_VERSION` (`u32`, `src/transfer.rs`). Bumped when a
  release changes the samples or meters the engine computes from the
  same log and inputs, or what a core copy holds and means. A core is
  replayed only by an engine of the version that copied it
  (`CoreCopy::decode`).
- `samplez_replica::PROTOCOL` (`u32`, `crates/samplez-replica/src/entry.rs`).
  The log protocol a binary writes and applies; its doc comment records
  what each value added. Bumped when that changes.
- `Product::DERIVATION_VERSION` (`u32`, per product). Bumped when the
  same log would apply to another product state, derive other engine
  entries, or save another state.

The compatibility key is `rollout::key::<P>(output)`: XXH3-64 over the
three versions (`rollout::Versions`) and the output config, and nothing
else. A release that bumps none of them keeps every replica's key, and
its pods roll into a running set as peers. The crate version is not in
the key: `the_key_holds_no_crate_version` pins the key's value for
fixed inputs, so a change to how the key is built fails there first.

`tests/golden.rs` runs a fixed signal through every stage that touches
output samples or meters and pins the hashes of the samples and of the
loudness per `COMPUTE_VERSION` in `GOLDEN`. A change to what any stage
computes fails it; the fix is a `COMPUTE_VERSION` bump and a new
`GOLDEN` row, never an edited row. A new stage that touches output bits
joins the golden run in the change that adds it.

A product's `config_hash` inputs stay fixed while a set runs. While a
member reports another hash the leader orders no client command, and a
replica refuses a snapshot of another hash before writing anything and
exits with status 78. A configuration change of that kind is made with
every replica stopped (README, "Starting a replica").

A release that changes the key hands the baseline over instead of
mixing: the leader names the top target-revision replica with the
target's key once it is a candidate (`baseline.rs`, `hand_over`), the
old-key replicas stop at the hand-over's page P (`rollout::hand_over_at`)
and only fill from it, and a replica of any other key commits nothing. Mixed keys never commit side by
side. The hand-over follows the audio StatefulSet's target revision, so
a rollback to an earlier revision moves the baseline back to that
revision's key the same way
(`superseded_and_rolled_back_rollouts_follow_the_target` in
`crates/samplez-replica/tests/rollout.rs`).

## Build, test, lint

Everything builds and runs in the builder image (`docker/builder/`,
published by `.github/workflows/builder.yml`). Several suites need
`SCHED_FIFO`, locked memory, KVM, AVX2 or a pinned nightly, and the
whole set takes long: CI runs every suite on every PR. Run locally what
the change touches; CI is the gate.

```sh
docker build -t samplez-builder docker/builder
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/w" -w /w \
  samplez-builder cargo test --workspace --locked
```

The jobs in `.github/workflows/ci.yml`, each in the builder image:

| Job | Runs | Proves |
| --- | --- | --- |
| `lint` | `cargo fmt --all --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings`; the lint probe (`--cfg samplez_lint_probe=...` must fail clippy in `src/rt_loop.rs`, `crates/samplez-mxl/src/output.rs` and `crates/samplez-dsp/src/lib.rs`); `.github/lockfile-age/check.py`; `cargo-deny ... check licenses` | formatting and lints; the RT lint actually rejects a lock, a sleep and the libmxl group wait; no crates.io release under 14 days old enters `Cargo.lock` unless `.github/lockfile-age/allow.txt` lists it; licences match `deny.toml` |
| `debug` | `cargo test --workspace --locked`; `cargo test --locked -p samplez-nmos --test schemas` after `crates/samplez-nmos/tests/fetch-specs.sh`; `cargo test --locked --test hop` with `--ulimit rtprio=99 --ulimit memlock=-1` and `SAMPLEZ_REQUIRE_RT=1` | the workspace tests, NMOS documents against the AMWA schemas pinned in `crates/samplez-nmos/tests/specs.txt`, and the end-to-end scenarios under real-time scheduling, over the debug build |
| `release` | `cargo test --workspace --locked --release` | the workspace tests over the release build |
| `e2e-release` | `cargo test --locked --release --test hop` with the RT limits | the end-to-end scenarios over the release build |
| `rt-gates` | `RUSTFLAGS="-Zsanitizer=realtime --cfg samplez_rtsan" cargo "+$SAMPLEZ_NIGHTLY" test --locked -Zbuild-std --target x86_64-unknown-linux-gnu --lib --test rtsan`, then `cargo bench --locked -p samplez -p samplez-dsp -p samplez-mxl --bench rt` | no blocking call on the RT thread (the job checks the named gate tests ran and passed), and every RT path within its instruction-count limit |
| `nmos` | `cargo build --locked -p samplez-nmos --example nmos-node`, then `tests/nmos/conformance.sh` with the pinned nmos-testing image and `tests/nmos/gate.py tests/nmos/expected.json` | the AMWA IS-04-01 (unicast and multicast DNS-SD), IS-05-01 and BCP-007-03-01 suites pass, apart from results `tests/nmos/expected.json` lists with a reason |
| `vm-clock` | `cargo test -p samplez-replica --test vm_clock` run inside a virtme-ng VM | an engine across a TAI offset step and a kernel-inserted leap second; these step the clock, so they never run on a shared host |
| `ci` | needs every job above | the required check: green only when all passed |

`lint`, `debug`, `release`, `e2e-release` and `rt-gates` set
`SAMPLEZ_REQUIRE_PATHS=avx2`, so a matrix test fails rather than skips
the AVX2 path. `cargo test --workspace` leaves out the `hop`, `schemas`
and `vm_clock` suites; they need the set-up above.

## Branches and PRs

- Every change opens a feature branch and a PR against `main`. Commit
  directly to `main` only when the maintainer has approved it for that
  specific change.
- Never force-push `main`. Force-push a feature branch only with
  approval, since another editor may be reviewing it.
- The repository allows squash merging only and deletes the head
  branch on merge.

### Merging and the squash commit

The repository is set to `squash_merge_commit_title=PR_TITLE` and
`squash_merge_commit_message=COMMIT_MESSAGES`: the PR title becomes the
commit subject with the PR number appended, and the body is the list of
the branch's commit messages. The PR description does not reach the
commit.

- Merge with the squash button or `gh pr merge --squash`.
- Leave the prefilled body alone; once the branch is deleted it is the
  only record of how the change was built. Never pass `--subject`,
  `--body` or `--body-file`, nor `commit_title` or `commit_message` to
  the merge API.

release-please takes one changelog entry per squash, from the PR title,
and parses `!`, `BREAKING CHANGE:` and `Release-As:` only as footers at
the very bottom of the squashed message. Three rules follow:

1. **The PR title is a Conventional Commit**, `<type>(<scope>): <subject>`,
   at most 72 characters, imperative mood.
2. **A PR carrying more than one release-relevant change is split, or
   carries a `BEGIN_COMMIT_OVERRIDE` block in its PR body.** release-please
   reads that block from the pull request over the API. It replaces the
   whole commit message, so it repeats the entry the title would have
   produced:

   ```
   BEGIN_COMMIT_OVERRIDE
   feat(replica): <the first change> (#<pr>)

   fix(dsp): <the second change> (#<pr>)
   END_COMMIT_OVERRIDE
   ```

   Adding the block to an already merged PR works: the next
   release-please run rebuilds the pending release PR from it.
3. **`!`, `BREAKING CHANGE:` and `Release-As: X.Y.Z` go on the PR title
   or in the override block.** A footer written into an intermediate
   commit of a multi-commit branch ends up as prose and is inert.

## Commits

- Conventional Commits with a scope matching the area changed: `engine`
  for the `samplez` crate, else the crate's suffix (`dsp`, `mxl`,
  `replica`, `service`, `nmos`).
  Cross-cutting changes use `chore:`, `ci:`, `build:` or `docs:` without
  a scope. Breaking changes take `!` or a `BREAKING CHANGE:` footer.
- Subject at most 72 characters, imperative mood. Body wrapped at 72.
- Prefer small, focused commits.

A message documents why the change exists, in terms still useful read
alone years later. The same rules apply to PR descriptions.

- Explain why. The diff shows what.
- Stay scoped to this repository and this change. No speculation about
  upstream, downstream or follow-ups. Name a deliberate omission only
  when it matters for understanding the change.
- Reference another project only when its state is the direct reason
  for the change, such as a dependency bump or a pinned API contract.
- Declarative facts. No pronouns, no addressing a reader.
- No narration of what was tried first or what failed.
- No filler verbs without specifics ("clean up", "improve", "refactor").
- No checklists, summary or test-plan sections, marketing phrasing or
  emojis.
- No tool-authored `Co-Authored-By:` trailers.
- Cross-reference an issue or PR only when it is itself the reason for
  the change.

## Releases and versioning

release-please (`.github/workflows/release-please.yml`,
`release-please-config.json`, `.release-please-manifest.json`) cuts
releases with release type `rust`: one version for the whole workspace,
tags without a component, and while the major version is 0 a breaking
change bumps the minor version and a `feat` the patch version. Every
crate is `publish = false`.

Merging a change is not a release. The workflow runs on the merge and
opens or updates the release PR; merging that PR bumps every crate's
version, writes `CHANGELOG.md` and tags the release. Do not hand-tag,
hand-edit `CHANGELOG.md` or bump crate versions by hand. A crate
version is not a compatibility statement between replicas; the three
versions above are.

## Graphify

The [graphify](https://github.com/safishamsi/graphify) knowledge graph
is generated locally and not committed: `graphify-out/` is gitignored
because its files embed an absolute, machine-specific path. The skill
is tracked at `.agents/skills/graphify/` and the hook in
`.claude/settings.json`.

- Build or refresh the graph with `graphify update .`. Extraction is
  AST-only and needs no API key; `.graphifyignore` scopes it.
- For codebase questions, prefer `graphify query "<question>"` over wide
  grep sweeps once `graphify-out/graph.json` exists; `graphify path "<A>"
  "<B>"` for relationships, `graphify explain "<concept>"` for a focused
  subgraph. Use it to find what to read, then read it; it carries no
  behaviour.
- Read `graphify-out/GRAPH_REPORT.md` only for broad architecture
  context.
- `graphify install --project` reinstalls the skill and hook. It writes
  them to `.claude/skills/graphify/`, appends a graphify section to
  `CLAUDE.md` and creates `.claude/CLAUDE.md`: move the skill to
  `.agents/skills/graphify/`, restore `.claude/skills` as the symlink,
  and revert both `CLAUDE.md` changes.

## Skills

Skills live in `.agents/skills/<name>/SKILL.md`; `.claude/skills` is a
symlink to `../.agents/skills`. `desloppify` is the code-health scanner
skill, copied from what desloppify 1.0 generates with its first section
written for this repository; scan from the repository root as it
describes.

## Harness notes

`CLAUDE.md` stays the one line `@AGENTS.md`, so every harness reads this
file and the two never drift.

## When in doubt

Ask the maintainer before changing a public API, any of the three
versions, the compatibility key, the golden hashes, an RT gate's limit,
or the release and tagging setup. Prefer doing nothing to doing the
wrong thing.

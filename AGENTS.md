# Contributor notes (agents and humans)

How to change samplez. `README.md` describes what it does and wins on
behaviour.

## Worktrees

Every change is made in its own `git worktree` under
`.claude/worktrees/` (gitignored), never in the main checkout, which is
for reading: several sessions work here at once, and two writers in one
tree corrupt each other's index and branches.

```sh
git fetch origin && id=$(openssl rand -hex 4)
git worktree add .claude/worktrees/<topic>-$id -b <topic> origin/main
git worktree remove .claude/worktrees/<topic>-$id   # after the merge
```

## Working rules

- **Think before coding.** State assumptions; present competing
  readings; when unclear, stop and ask; name a simpler approach.
- **Read before you write**: exports, callers, shared utilities. Most
  engine state is replicated, copied in a core or hashed into a page.
- **Simplicity first**: no speculative features, single-use
  abstractions or unrequested knobs.
- **Conventions**: conform; call out a harmful one, never fork it.
- **Fail loudly**: "tests pass" is wrong if any were skipped, or if the
  suite covering the change runs only in CI and has not run.
- **Leave it cleaner**: what is touched leaves more correct and shorter,
  `README.md` included. No session artefacts (task refs, phases, todos),
  nothing private, only public issues and PRs, no SPDX/copyright headers
  on new files, no new `.md` files unasked.
- **Tests at every level** (unit, `tests/`, `crates/*/tests/`,
  `tests/hop.rs`, RT gates, benches), each failing without the change.
  `#[ignore]` never gets a test green; the two manual ones stay:
  `snapshot_text_gate_hold` (`log_store.rs`, a measurement) and
  `a_slow_follower_disk_refuses_and_contradicts_nothing_for_30_s`
  (`tests/derive.rs`, whose 3 s variant runs).
- **ASCII only** in code, docs, commits and PRs (`-`, `--`, `"`, `->`,
  `...`); vendored and generated skills under `.agents/skills/` are
  exempt.
- **Documentation states facts** the source, a run or authoritative docs
  confirm; a behaviour change updates the README in the same PR.

## Orientation

Six crates, x86_64 Linux only; README "Crates" has the detail.

- `samplez` (`src/`): `engine.rs` the supervisor, the one thread making
  instance-mutating libmxl calls; `rt_loop.rs` the RT block loop;
  `transfer.rs` core copy, replay and hand-over.
- `samplez-dsp` the DSP kernels; `samplez-mxl` MXL I/O on libmxl v1.1.0.
- `samplez-replica`: openraft log and snapshots (`log_store.rs`), the
  state machine and the `Product` trait (`state_machine.rs`), the
  contact loop (`contact.rs`), leader decisions (`baseline.rs`,
  `verdict.rs`, `decide.rs`), rollout (`rollout.rs`), resync
  (`resync.rs`) and the engine bridge (`bridge.rs`).
- `samplez-service`, `samplez-nmos`: the non-RT service layer and NMOS.

## Invariants

Weakening one is a design change: say so and agree it first.
- **Replicas compute bit-identical output from the same ordered log.**
  Output is a function of the sample index, the inputs and the entries
  applied at their indices; wall-clock time and thread timing decide no
  sample. Pages are compared as XXH3-64 over f32 bit patterns.
  Enforced by: the page-hash scenarios in `tests/pages.rs` and
  `tests/transfer.rs`; the goldens `tests/golden.rs` and
  `src/engine/golden.rs` (`GOLDEN` for samples, meters and page hashes,
  `CORES` for the bytes of `CoreCopy::encode`); scalar-vs-AVX2 bit
  equality of the matrix and true peak (`bit_exact_avx2`,
  `ducked_bit_exact_avx2`, `true_peak.rs` `bit_exact_vs_scalar`);
  block-split independence of
  delayed fades, the limiter and strip crossfades
  (`a_delayed_fade_is_bit_exact_across_paths_and_block_sizes`,
  `the_output_does_not_depend_on_the_blocks`,
  `a_crossfade_ignores_block_sizes`).
- **No allocation and no locks on the RT thread.**
  Enforced by: `tests/no_alloc.rs`; RealtimeSanitizer (`tests/rtsan.rs`
  and the lib tests built with `--cfg samplez_rtsan`); clippy's
  disallowed types and methods in the RT modules (`clippy.toml`) with
  the lint probe; the Gungraun instruction-count benches
  (`benches/rt.rs`, `crates/samplez-dsp/benches/rt.rs`,
  `crates/samplez-mxl/benches/rt.rs`), whose limits are the count
  measured in the builder image plus 10%, reset the same way, with the
  reason in the PR, when a change moves a count.
- **Lock order.** Nothing holds a store lock and waits for `Contact`
  (the contact loop reads the store under it); no lock is held across
  I/O or a wait; `Product` methods and store-read closures never call
  back into `Replica` or `Store` nor wait for an apply. A watched lock
  held past 10 s aborts the process, tests included. README "Ops
  endpoints and the lock watchdog".
  Enforced by: the call-back rule, the `log_store.rs` tests that such a
  call panics (`*_under_the_lock_panics`,
  `an_apply_inside_with_state_panics`). No test checks the order itself:
  a violation shows only as a stall the 10 s watchdog in `lock_watch.rs`
  aborts.
- **A rebuilt or resynced replica matches its peers from its first
  page.** The exceptions README "Starting a replica" lists for a rebuild
  (duck detector state, a mask crossfade or depth ramp in progress) cost
  one diverged page and a resync; do not widen that list.
  Enforced by: the replay, install and rebuild tests in
  `tests/transfer.rs` (`a_replayed_core_computes_the_donors_pages`,
  `a_replayed_core_carries_*`, `a_product_core_installs_at_the_index_*`,
  `a_rebuild_restores_a_route_and_a_strip_parameter_*`).
- **Tests compare pages only once verdicts confirm them**, leaving out
  pages a silenced block touched; no sleep decides a page is final.
  Enforced by: review; `comparable` in `tests/transfer.rs` is the form
  to reuse.
- **Retries only for runs the engine's own counters prove disturbed**
  (an overrun skip, an uncaused ring overflow, a late input read), at
  most four runs; an undisturbed run must pass.
  Enforced by: review; `undisturbed` in `tests/common/mod.rs` and
  `tests/hold.rs` is the only retry helper.

## Versions and compatibility

Replicas run side by side only at equal `samplez::COMPUTE_VERSION`,
`samplez_replica::PROTOCOL` and `Product::DERIVATION_VERSION`. The README
paragraph on the compatibility key (`rollout::key`, in "Starting a
replica") has the bump rules:

- `PROTOCOL`: the log's wire format, the library's replicated semantics
  (state machine, `Entry` variants, `Targets` fields, restamp and void
  handling), and any change to how page hashes or entry fingerprints are
  computed, also where the samples stay the same.
- `COMPUTE_VERSION`: any change to the samples or meters the engine
  computes, dependency and toolchain changes included, or to what a core
  copy holds or how it encodes. `samplez-dsp` pins `libm = "=0.2.16"`,
  and products pin it too.
- Two goldens pin what `COMPUTE_VERSION` stands for: `tests/golden.rs`
  the DSP kernels on a fixed signal, and `src/engine/golden.rs` the RT
  loop run offline, with `GOLDEN` (samples, meters, page hashes) and
  `CORES` (the encoded core copy). These are tables with a row per
  `COMPUTE_VERSION`.
- `FINGERPRINTS` in `src/engine/golden.rs` is a single pinned value: a
  change to how page hashes or fingerprints are computed bumps
  `PROTOCOL` and replaces it in the same change. The `samplez` crate
  cannot see `PROTOCOL`, so that pairing is a review step.

A version bump a change needs belongs to that change: add a new row to
`GOLDEN` or `CORES`, never edit one. Ask before changing the rules.

## Build and test

Run the way `.github/workflows/ci.yml` runs, in the builder image:

```sh
docker build -t samplez-builder:ci docker/builder
mkdir -p .cargo-registry
docker run --rm --user "$(id -u):$(id -g)" \
  -e SAMPLEZ_REQUIRE_PATHS=avx2 -e CARGO_INCREMENTAL=0 \
  -v "$PWD:/w" -v "$PWD/.cargo-registry:/usr/local/cargo/registry" \
  -w /w samplez-builder:ci cargo test --workspace --locked
```

`.github/workflows/ci.yml` has each job's exact command and what it
proves; `ci` is the required check. Only CI runs what `cargo test
--workspace` leaves out: the end-to-end scenarios (`--test hop`, needing
`SCHED_FIFO` and locked memory), the NMOS schema and AMWA conformance
suites, RealtimeSanitizer on the pinned nightly, the instruction-count
benches, and `vm_clock`, which steps the kernel clock inside a VM. Run
locally what the change touches; CI is the gate.

Gotchas: without `SAMPLEZ_REQUIRE_PATHS=avx2` a matrix test skips the
AVX2 path on a host lacking it instead of failing; the lint probe needs a
lock and a sleep in each RT crate to keep failing clippy after a
`clippy.toml` change; a crates.io release under 14 days old fails `lint`
unless `.github/lockfile-age/allow.txt` lists it.

## Branches, PRs, merging, releases

- A feature branch and a PR per change; never push or force-push `main`.
- Squash merge only, PR title as subject and the branch's commits as
  body; leave that body alone (no `--subject`, `--body`, `--body-file`,
  `commit_title` or `commit_message`).
- release-please takes one changelog entry per squash, from the PR
  title, and reads `!`, `BREAKING CHANGE:` and `Release-As:` only as
  footers at the very end. So the PR title is a Conventional Commit, and
  a PR with more than one release-relevant change is split or carries a
  `BEGIN_COMMIT_OVERRIDE` ... `END_COMMIT_OVERRIDE` block in its PR body,
  one Conventional Commit per entry, repeating the title's entry too.
- release-please (type `rust`) versions the whole workspace; before 1.0
  a breaking change bumps the minor and a `feat` the patch. Merging the
  release PR releases. Never hand-tag, hand-edit `CHANGELOG.md` or bump
  crate versions.

## Commits

Conventional Commits, scope `engine` for the `samplez` crate, else the
crate suffix (`dsp`, `mxl`, `replica`, `service`, `nmos`); cross-cutting
changes use `chore:`, `ci:`, `build:` or `docs:`. Subject at most 72
characters, imperative; body wrapped at 72. A message, like a PR
description, says why in declarative facts: no pronouns, narration,
filler verbs, checklists, speculation or tool-authored `Co-Authored-By:`
trailers; another project or issue only when it is the reason.

## Tooling

- Skills live in `.agents/skills/<name>/SKILL.md`; `.claude/skills` is a
  symlink to it.
- `graphify update .` builds the local graph (gitignored, scoped by
  `.graphifyignore`); `graphify query "<q>"` finds what to read.
  `graphify install --project` regenerates the skill and the stock hook
  in `.claude/settings.json`; move the skill back to `.agents/skills/`
  and revert its `CLAUDE.md` and `.claude/CLAUDE.md` edits.
- desloppify 1.0's Rust scan was seen running `cargo clippy`, `check`
  and `rustdoc` and building a ~722 MB `target/`. Running it in the
  builder container would need desloppify installed there, and has not
  been tried.

## Harness notes

`CLAUDE.md` is the one line `@AGENTS.md`. In Claude Code, a sub-agent
that edits gets `isolation: "worktree"`, which creates
`.claude/worktrees/agent-<id>/` for it.

## When in doubt

Ask before changing a public API, the version rules, the compatibility
key, a golden row, an RT gate's limit or the release setup.

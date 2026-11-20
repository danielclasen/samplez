---
name: desloppify
description: >
  Multi-language codebase health scanner. Use when the user explicitly asks
  to run desloppify, scan for technical debt, get a health score, or create
  a cleanup plan. Do NOT trigger for general code review, renaming, or
  fixing individual bugs.
---

<!-- Copied from the skill desloppify 1.0 generates (skill version 7), with
this section in place of the generator's own. -->

## In this repository

- Install desloppify 1.0 with its `full` extras, for example
  `pipx install 'desloppify[full]==1.0'`. The Prerequisite section at the end
  of the shared part installs an unpinned version.
- Scan from the repository root: `desloppify scan --path .`. The state lands
  in `.desloppify/`, which `.gitignore` lists. Set `DESLOPPIFY_NO_BADGE=true`
  or pass `--no-badge` to `scan`, so no `scorecard.png` lands at the root.
- desloppify reads no `.gitignore` and scans everything under `--path`,
  including worktrees under `.claude/worktrees/`. Before the first scan of a
  checkout, exclude what git ignores, and `.claude/worktrees/` by name. The
  patterns land in `.desloppify/config.json` and stay local:

  ```sh
  git -c core.quotePath=false ls-files --others --ignored --exclude-standard --directory |
    while IFS= read -r p; do desloppify exclude "$p"; done
  desloppify exclude .claude/worktrees/
  ```
- Branches, commits and pull requests follow `AGENTS.md`, not the examples
  below. Do not run `desloppify config set commit_pr` or
  `desloppify plan commit-log record`, which link a pull request and rewrite
  its description, or anything else that edits a pull request description or
  a commit message.
- A reference to `docs/<HARNESS>.md`, such as `docs/CLAUDE.md`, means that
  harness's overlay further down this file. The `.gemini/agents/` and
  `.github/agents/` definitions the overlays describe do not exist here;
  create them only when asked.
- Do not run `desloppify update-skill` or `desloppify setup`, which a scan
  suggests when it finds no skill in the checkout. The first replaces this
  file; the second installs skills into the user's home directory.
- The shared part below is followed by one overlay per harness, each under its
  own heading: Claude Code, Codex, Gemini CLI, OpenCode, Copilot and Hermes.
  Follow the overlay of the harness running this session. Mistral Vibe has
  none; the shared part applies on its own.

<!-- desloppify-begin -->
<!-- desloppify-skill-version: 7 -->

# Desloppify

## 1. Your Job

Maximise the **strict score** honestly. Your main cycle: **scan -> plan -> execute -> rescan**. Follow the scan output's **INSTRUCTIONS FOR AGENTS** -- don't substitute your own analysis.

**Don't be lazy.** Do large refactors and small detailed fixes with equal energy. If it takes touching 20 files, touch 20 files. If it's a one-line change, make it. No task is too big or too small -- fix things properly, not minimally.

## 2. The Workflow

Three phases, repeated as a cycle.

### Monorepos and multi-project directories

If the workspace contains multiple programs (e.g., frontend + backend in sibling folders), scan each one separately -- do not scan the parent directory:

```bash
desloppify --lang typescript scan --path ./frontend
desloppify --lang python scan --path ./backend
```

Each `--path` target should be a single coherent project. Scanning a parent that contains multiple programs mixes state and path context, producing unreliable results.

### Phase 1: Scan and review -- understand the codebase

```bash
desloppify scan --path .       # analyse the codebase
desloppify status              # check scores -- are we at target?
```

After scanning, **always run `desloppify next`** -- it tells you exactly what to do, in order. Don't interpret the scan output yourself or ask the user what to do. Just run `next` and follow its instructions.

The scan will tell you if subjective dimensions need review. Follow its instructions. To trigger a review manually:
```bash
desloppify review --prepare    # then follow your runner's review workflow
```

### Phase 2: Plan -- decide what to work on

After reviews, triage stages and plan creation appear in the execution queue surfaced by `next`. Complete them in order -- `next` tells you what each stage expects in the `--report`:
```bash
desloppify next                                        # shows the next execution workflow step
desloppify plan triage --stage observe --report "themes and root causes..."
desloppify plan triage --stage reflect --report "comparison against completed work..."
desloppify plan triage --stage organize --report "summary of priorities..."
desloppify plan triage --complete --strategy "execution plan..."
```

For automated triage: `desloppify plan triage --run-stages --runner codex` (Codex), `--runner claude` (Claude), or `--runner rovodev` (Rovo Dev). Options: `--only-stages`, `--dry-run`, `--stage-timeout-seconds`.

Then shape the queue. **The plan shapes everything `next` gives you** -- `next` is the execution queue, not the full backlog. Don't skip this step.

```bash
desloppify plan                          # see the living plan details
desloppify plan queue                    # compact execution queue view
desloppify plan reorder <pat> top        # reorder -- what unblocks the most?
desloppify plan cluster create <name>    # group related issues to batch-fix
desloppify plan focus <cluster>          # scope next to one cluster
desloppify plan skip <pat>              # defer -- hide from next
```

### Phase 3: Execute -- grind the queue to completion

Trust the plan and execute. Don't rescan mid-queue -- finish the queue first.

**Branch first.** Create a dedicated branch -- never commit health work directly to main:
```bash
git checkout -b desloppify/code-health    # or desloppify/<focus-area>
desloppify config set commit_pr 42        # link a PR for auto-updated descriptions
```

**The loop:**
```bash
# 1. Get the next item from the execution queue
desloppify next

# 2. Fix the issue in code

# 3. Resolve it (next shows the exact command including required attestation)

# 4. When you have a logical batch, commit and record
git add <files> && git commit -m "desloppify: fix 3 deferred_import findings"
desloppify plan commit-log record      # moves findings uncommitted -> committed, updates PR

# 5. Push periodically
git push -u origin desloppify/code-health

# 6. Repeat until the queue is empty
```

Score may temporarily drop after fixes -- cascade effects are normal, keep going.
If `next` suggests an auto-fixer, run `desloppify autofix <fixer> --dry-run` to preview, then apply.

**When the queue is clear, go back to Phase 1.** New issues will surface, cascades will have resolved, priorities will have shifted. This is the cycle.

## 3. Reference

### Key concepts

- **Tiers**: T1 auto-fix -> T2 quick manual -> T3 judgment call -> T4 major refactor.
- **Auto-clusters**: related findings are auto-grouped in `next`. Drill in with `next --cluster <name>`.
- **Zones**: production/script (scored), test/config/generated/vendor (not scored). Fix with `zone set`.
- **Wontfix cost**: widens the lenient<->strict gap. Challenge past decisions when the gap grows.

### Scoring

Overall score = **25% mechanical** + **75% subjective**.

- **Mechanical (25%)**: auto-detected issues -- duplication, dead code, smells, unused imports, security. Fixed by changing code and rescanning.
- **Subjective (75%)**: design quality review -- naming, error handling, abstractions, clarity. Starts at **0%** until reviewed. The scan will prompt you when a review is needed.
- **Strict score** is the north star: wontfix items count as open. The gap between overall and strict is your wontfix debt.
- **Score types**: overall (lenient), strict (wontfix counts), objective (mechanical only), verified (confirmed fixes only).

### Reviews

Four paths to get subjective scores:

- **Local runner (Codex)**: `desloppify review --run-batches --runner codex --parallel --scan-after-import` -- automated end-to-end.
- **Local runner (Claude)**: `desloppify review --prepare` -> launch parallel subagents -> `desloppify review --import merged.json` -- see skill doc overlay for details.
- **Local runner (Rovo Dev)**: `desloppify review --run-batches --runner rovodev --parallel --scan-after-import` -- automated end-to-end via `acli rovodev run` subprocesses.
- **Cloud/external**: `desloppify review --external-start --external-runner claude` -> follow session template -> `--external-submit`.
- **Manual path**: `desloppify review --prepare` -> review per dimension -> `desloppify review --import file.json`.

**Batch output vs import filenames:** Individual batch outputs from subagents must be named `batch-N.raw.txt` (plain text/JSON content, `.raw.txt` extension). The `.json` filenames in `--import merged.json` or `--import findings.json` refer to the final merged import file, not individual batch outputs. Do not name batch outputs with a `.json` extension.

**Subagent parallelism limit:** Do not launch every review batch at once. Run subagents in small waves, usually **3-5 concurrent agents**, and wait for a wave to finish before starting the next. If agents return empty, partial, or rate-limit-shaped results, reduce the wave size and retry only failed batches. Launching 20+ subagents at once can exhaust API quota and produce no usable review output.

- Import first, fix after -- import creates tracked state entries for correlation.
- Target-matching scores trigger auto-reset to prevent gaming. Use the blind-review workflow described in your agent overlay doc (e.g. `docs/CLAUDE.md`, `docs/HERMES.md`).
- Even moderate scores (60-80) dramatically improve overall health.
- Stale dimensions auto-surface in `next` -- just follow the queue.

**Integrity rules:** Score from evidence only -- no prior chat context, score history, or target-threshold anchoring. When evidence is mixed, score lower and explain uncertainty. Assess every requested dimension; never drop one.

#### Review output format

Return machine-readable JSON for review imports. For `--external-submit`, include `session` from the generated template:

```json
{
  "session": {
    "id": "<session_id_from_template>",
    "token": "<session_hmac_from_template>"
  },
  "assessments": {
    "<dimension_from_query>": 0
  },
  "findings": [
    {
      "dimension": "<dimension_from_query>",
      "identifier": "short_id",
      "summary": "one-line defect summary",
      "related_files": ["relative/path/to/file.py"],
      "evidence": ["specific code observation"],
      "suggestion": "concrete fix recommendation",
      "confidence": "high|medium|low"
    }
  ]
}
```

`findings` MUST match `query.system_prompt` exactly (including `related_files`, `evidence`, and `suggestion`). Use `"findings": []` when no defects found. Import is fail-closed: invalid findings abort unless `--allow-partial` is passed. Assessment scores are auto-applied from trusted internal or cloud session imports. Legacy `--attested-external` remains supported.

#### Import paths

- Robust session flow (recommended): `desloppify review --external-start --external-runner claude` -> use generated prompt/template -> run printed `--external-submit` command.
- Durable scored import (legacy): `desloppify review --import findings.json --attested-external --attest "I validated this review was completed without awareness of overall score and is unbiased."`
- Findings-only fallback: `desloppify review --import findings.json`

#### Reviewer agent prompt

Runners that support agent definitions (Cursor, Copilot, Gemini) can create a dedicated reviewer agent. Use this system prompt:

```
You are a code quality reviewer. You will be given a codebase path, a set of
dimensions to score, and what each dimension means. Read the code, score each
dimension 0-100 from evidence only, and return JSON in the required format.
Do not anchor to target thresholds. When evidence is mixed, score lower and
explain uncertainty.
```

See your editor's overlay section below for the agent config format.

### Plan commands

```bash
desloppify plan reorder <cluster> top       # move all cluster members at once
desloppify plan reorder <a> <b> top        # mix clusters + findings in one reorder
desloppify plan reorder <pat> before -t X  # position relative to another item/cluster
desloppify plan cluster reorder a,b top    # reorder multiple clusters as one block
desloppify plan resolve <pat>              # mark complete
desloppify plan reopen <pat>               # reopen
desloppify backlog                          # broader non-execution backlog
```

### Commit tracking

```bash
desloppify plan commit-log                      # see uncommitted + committed status
desloppify plan commit-log record               # record HEAD commit, update PR description
desloppify plan commit-log record --note "why"  # with rationale
desloppify plan commit-log record --only "smells::*"  # record specific findings only
desloppify plan commit-log history              # show commit records
desloppify plan commit-log pr                   # preview PR body markdown
desloppify config set commit_tracking_enabled false  # disable guidance
```

After resolving findings as `fixed`, the tool shows uncommitted work, committed history, and a suggested commit message. After committing externally, run `record` to move findings from uncommitted to committed and auto-update the linked PR description.

### Agent directives

Directives are messages shown to agents at lifecycle phase transitions -- use them to switch models, set constraints, or give context-specific instructions.

```bash
desloppify directives                     # show all configured directives
desloppify directives set execute "Switch to claude-sonnet-4-6. Focus on speed."
desloppify directives set triage "Switch to claude-opus-4-6. Read carefully."
desloppify directives set review "Use blind packet. Do not anchor on previous scores."
desloppify directives unset execute       # remove a directive
```

Available phases: `execute`, `review`, `triage`, `workflow`, `scan` (and fine-grained variants like `review_initial`, `triage_postflight`, etc.).

### Quick reference

```bash
desloppify next --count 5                         # top 5 execution items
desloppify next --cluster <name>                  # drill into a cluster
desloppify backlog --count 5                      # top 5 backlog items outside execution
desloppify show <pattern>                         # filter by file/detector/ID
desloppify show --status open                     # all open findings
desloppify plan skip --permanent "<id>" --note "reason" --attest "..." # accept debt
desloppify exclude <path>                         # exclude a directory from scanning
desloppify config show                            # show all config including excludes
desloppify scan --path . --reset-subjective       # reset subjective baseline to 0
```

## 4. Fix Tool Issues Upstream

When desloppify itself appears wrong or inconsistent -- a bug, a bad detection, a crash, confusing output -- **fix it and open a PR**. If you can't confidently fix it, file an issue instead.

### Fix and PR (preferred)

Clone the tool repo to a temp directory, make the fix there, and verify it works against the project you're scanning before pushing.

```bash
git clone https://github.com/peteromallet/desloppify.git /tmp/desloppify-fix
cd /tmp/desloppify-fix
git checkout -b fix/<short-description>
```

Make your changes, then run the test suite and verify the fix against the original project:

```bash
python -m pytest desloppify/tests/ -q
python -m desloppify scan --path <project-root>   # the project you were scanning
```

Once it looks good, push and open a PR:

```bash
git add <files> && git commit -m "fix: <what and why>"
git push -u origin fix/<short-description>
gh pr create --title "fix: <short description>" --body "$(cat <<'EOF'
## Problem
<what went wrong -- include the command and output>

## Fix
<what you changed and why>
EOF
)"
```

Clean up after: `rm -rf /tmp/desloppify-fix`

### File an issue (fallback)

If the fix is unclear or the change needs discussion, open an issue at `https://github.com/peteromallet/desloppify/issues` with a minimal repro: command, path, expected output, actual output.

## Prerequisite

`command -v desloppify >/dev/null 2>&1 && echo "desloppify: installed" || echo "NOT INSTALLED -- run: uvx --from git+https://github.com/peteromallet/desloppify.git desloppify"`

If `uvx` is not available: `pip install desloppify[full] && desloppify setup`

<!-- desloppify-end -->

## Claude Code Overlay

Use Claude subagents for subjective scoring work. **Do not use `--runner codex`** -- use Claude subagents exclusively.

### Review workflow

Run `desloppify review --prepare` first to generate review data, then use Claude subagents:

1. **Prepare**: `desloppify review --prepare` -- writes `query.json` and `.desloppify/review_packet_blind.json`.
2. **Launch subagents**: Split the review across N parallel Claude subagents (one message, multiple Task calls). Each agent reviews a subset of dimensions.
3. **Merge & import**: Merge agent outputs, then `desloppify review --import merged.json --manual-override --attest "Claude subagents ran blind reviews against review_packet_blind.json" --scan-after-import`.

#### How to split dimensions across subagents

- Read `dimension_prompts` from `query.json` for dimensions with definitions and seed files.
- Read `.desloppify/review_packet_blind.json` for the blind packet (no score targets, no anchoring data).
- Group dimensions into 3-4 batches by theme (e.g., architecture, code quality, testing, conventions).
- Launch one Task agent per batch with `subagent_type: "general-purpose"`. Each agent gets:
  - The codebase path and list of dimensions to score
  - The blind packet path to read
  - Instruction to score from code evidence only, not from targets
- Each agent writes output to `results/batch-N.raw.txt` (matching the batch index). Merge assessments (average overlapping dimension scores) and concatenate findings.

### Subagent rules

1. Each agent must be context-isolated -- do not pass conversation history or score targets.
2. Agents must consume `.desloppify/review_packet_blind.json` (not full `query.json`) to avoid score anchoring.

### Triage workflow

Orchestrate triage with per-stage subagents:
1. `desloppify plan triage --run-stages --runner claude` -- prints orchestrator instructions
2. For each stage (observe -> reflect -> organize -> enrich):
   - Get prompt: `desloppify plan triage --stage-prompt <stage>`
   - Launch a subagent with that prompt
   - Verify: `desloppify plan triage` (check dashboard)
   - Confirm: `desloppify plan triage --confirm <stage> --attestation "..."`
3. Complete: `desloppify plan triage --complete --strategy "..." --attestation "..."`

<!-- desloppify-overlay: claude -->
<!-- desloppify-end -->

## Codex Overlay

This is the canonical Codex overlay used by the README install command.

1. Prefer first-class batch runs: `desloppify review --run-batches --runner codex --parallel --scan-after-import`.
2. The command writes immutable packet snapshots under `.desloppify/review_packets/holistic_packet_*.json`; use those for reproducible retries.
3. Keep reviewer input scoped to the immutable packet and the source files named in each batch.
4. If a batch fails, retry only that slice with `desloppify review --run-batches --packet <packet.json> --only-batches <idxs>`.
5. Manual override is safety-scoped: you cannot combine it with `--allow-partial`, and provisional manual scores expire on the next `scan` unless replaced by trusted internal or attested-external imports.

### Subagent policy

Do not ask Codex review or triage prompts to spawn their own child agents. The supported Codex path is the first-class batch runner above: it already isolates packet slices, supports parallel subprocess execution, preserves retry artifacts, and keeps execution guardrails outside the model prompt. Revisit this only after Codex exposes a stable non-interactive subagent contract that can cap concurrency, preserve blind-packet isolation, and retry failed child tasks without increasing cost or weakening guardrails.

### Sandbox

Codex batch runs default to `-s workspace-write`. On hosts where that sandbox cannot run, such as WSL1 systems without the needed Linux namespace support, set `DESLOPPIFY_CODEX_SANDBOX=danger-full-access` in an externally sandboxed environment before running review batches. Supported values are `read-only`, `workspace-write`, and `danger-full-access`; invalid values fall back to `workspace-write`.

### Triage workflow

Prefer automated triage: `desloppify plan triage --run-stages --runner codex`

Options: `--only-stages observe,reflect` (subset), `--dry-run` (prompts only), `--stage-timeout-seconds N` (per-stage).

Run artifacts go to `.desloppify/triage_runs/<timestamp>/` -- each run gets its own directory with `run.log` (live timestamped events), `run_summary.json`, per-stage `prompts/`, `output/`, and `logs/`. Check `run.log` to diagnose stalls or failures. Re-running resumes from the last confirmed stage.

If automated triage stalls, check `run.log` for the last event, then use `desloppify plan triage --stage-prompt <stage>` to get the full prompt with gate rules.

<!-- desloppify-overlay: codex -->
<!-- desloppify-end -->

## Gemini CLI Overlay

Gemini CLI has experimental subagent support, but subagents currently run
sequentially (not in parallel). Review dimensions one at a time.

### Setup

Enable subagents in Gemini CLI settings:
```json
{
  "experimental": {
    "enableAgents": true
  }
}
```

Optionally define a reviewer agent in `.gemini/agents/desloppify-reviewer.md`:

```yaml
---
name: desloppify-reviewer
description: Scores subjective codebase quality dimensions for desloppify
kind: local
tools:
  - read_file
  - search_code
temperature: 0.2
max_turns: 10
---
```

Use the prompt from the "Reviewer agent prompt" section above.

### Review workflow

Invoke the reviewer agent for each group of dimensions sequentially.
Even without parallelism, isolating dimensions across separate agent
invocations prevents score bleed between concerns.

Merge assessments and findings, then import.

When Gemini CLI adds parallel subagent execution, split dimensions across
concurrent agent calls instead.

<!-- desloppify-overlay: gemini -->
<!-- desloppify-end -->

## OpenCode Overlay

When installed (via `desloppify update-skill opencode`), OpenCode automatically loads this skill for code quality, technical debt, and health score questions.

### Review workflow

Use the native `--runner opencode` for automated batch reviews:

```
desloppify review --run-batches --runner opencode --parallel --scan-after-import
```

This spawns OpenCode subprocesses (`opencode run --format json`) for each batch, extracts results from the NDJSON stream, merges them, and imports as trusted assessments -- identical pipeline to the Codex runner but using OpenCode as the execution engine.

#### Warm server mode (optional, recommended for parallel runs)

Start a persistent OpenCode server to avoid MCP cold-start overhead per batch:

```
opencode serve --port 4096 &
export DESLOPPIFY_OPENCODE_ATTACH=http://localhost:4096
desloppify review --run-batches --runner opencode --parallel --scan-after-import
```

When `DESLOPPIFY_OPENCODE_ATTACH` is set, each batch subprocess attaches to the running server via `--attach <url>` instead of spawning a fresh instance.

#### Preparing a review manually

1. **Prepare**: `desloppify review --prepare` -- writes `query.json` and `.desloppify/review_packet_blind.json`.
2. **Run batches**: `desloppify review --run-batches --runner opencode --parallel --scan-after-import`

The runner handles batch splitting, prompt generation, parallel execution, retry/stall detection, result extraction, merge, and trusted import automatically.

<!-- desloppify-overlay: opencode -->
<!-- desloppify-end -->

## VS Code Copilot Overlay

VS Code Copilot supports native subagents via `.github/agents/` definitions.
Use them for context-isolated subjective reviews.

### Review workflow

Define a reviewer in `.github/agents/desloppify-reviewer.md`:

```yaml
---
name: desloppify-reviewer
tools: ['read', 'search']
---
```

Use the prompt from the "Reviewer agent prompt" section above.

Define an orchestrator in `.github/agents/desloppify-review-orchestrator.md`:

```yaml
---
name: desloppify-review-orchestrator
tools: ['agent', 'read', 'search']
agents: ['desloppify-reviewer']
---
```

Split dimensions across `desloppify-reviewer` calls (Copilot runs them concurrently), merge assessments and findings, then import.

<!-- desloppify-overlay: copilot -->
<!-- desloppify-end -->

## Hermes Agent Overlay

Hermes has built-in parallel subagent support via `delegate_task` (up to 3
concurrent children). Use `delegate_task(tasks=[...])` for subjective review
batches and per-stage triage support; avoid the older worktree-based guidance
here.

### Review workflow

1. Prepare review prompts and the blind packet:
   ```bash
   desloppify review --run-batches --dry-run
   ```
   This generates one prompt file per batch in
   `.desloppify/subagents/runs/<run-id>/prompts/` and prints the run directory.

2. Launch Hermes subagents in batches of 3 with `delegate_task(tasks=[...])`.
   Each subagent should:
   - read its prompt file at
     `.desloppify/subagents/runs/<run-id>/prompts/batch-N.md`
   - read `.desloppify/review_packet_blind.json`
   - inspect the repository from the prompt's dimension
   - write ONLY valid JSON to
     `.desloppify/subagents/runs/<run-id>/results/batch-N.raw.txt`

   Example task payload:
   ```json
   {
     "goal": "Review batch N. Read the prompt at .desloppify/subagents/runs/<run-id>/prompts/batch-N.md, follow it exactly, inspect the repository, and write ONLY valid JSON to .desloppify/subagents/runs/<run-id>/results/batch-N.raw.txt.",
     "context": "Repository root: <cwd>. Blind packet: .desloppify/review_packet_blind.json. The prompt file defines the required output schema. Do not edit repository source files. Only write the review result file.",
     "toolsets": ["terminal", "file"]
   }
   ```

   Repeat for batches 1-3, 4-6, 7-9, etc. Wait for each group of 3 to finish
   before launching the next group.

3. After all prompt files for that run have matching results, import them:
   ```bash
   desloppify review --import-run .desloppify/subagents/runs/<run-id> --scan-after-import
   ```

### Key constraints

- `delegate_task` supports at most 3 concurrent children at a time.
- Subagents do not inherit parent context; the prompt file and blind packet must
  provide everything needed.
- Subagents cannot call `delegate_task`, `clarify`, `memory`, or `send_message`.
- The importer expects `results/batch-N.raw.txt` files, not `.json` filenames.
- The blind packet intentionally omits score history to prevent anchoring bias.

### Triage workflow

Run triage stages sequentially. For each stage:

1. Get the stage prompt or use the command suggested by `desloppify next`.
2. If the stage benefits from parallel review work, use `delegate_task(tasks=[...])`
   in groups of 3; otherwise run the stage directly in the parent session.
3. Record the stage output with `desloppify plan triage --stage <stage> --report "..."`
   or the corresponding `--run-stages --runner ...` command when available.
4. Confirm with `desloppify plan triage --confirm <stage> --attestation "..."`.
5. Finish with `desloppify plan triage --complete --strategy "..." --attestation "..."`.

<!-- desloppify-overlay: hermes -->
<!-- desloppify-end -->

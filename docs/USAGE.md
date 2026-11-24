# Using samplez

How the engine behaves and how it is configured, one section per
feature. Running replicas of it is in [REPLICAS.md](REPLICAS.md), how the
parts are built and fit together in [ARCHITECTURE.md](ARCHITECTURE.md),
and building and testing in [BUILDING.md](BUILDING.md).

## Configuration

The engine takes a JSON configuration (`samplez::Config`), the one
`samplez-hop` reads. Keys are camelCase; unknown keys are refused.

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
  "rt": { "enabled": true, "priority": 70, "rtCpu": "auto", "isolateRtCpu": true }
}
```

| Key | Default | Range and effect |
| --- | --- | --- |
| `libmxl` | required | path of the libmxl shared library |
| `domains` | required | `{ name, path }` per MXL domain |
| `sampleRate` | 48000 | 48000 only |
| `blockSize` | 48 | 16 to 1024 samples |
| `readerMarginSamples` | 576 | block `k` is due at the MXL time of sample `(k + 1) blockSize + margin` |
| `maxCatchupBlocks` | 2 | an overdue backlog up to this long is computed in order; beyond it only the newest block |
| `pruneAfterBlocks` | 50 | consecutive late blocks before an input is pruned ([Inputs](#inputs)) |
| `rampSamples` | 480 | length of every gain ramp and input fade, at most `samplez_dsp::MAX_EXACT_RAMP_LEN` (2^23) |
| `holdMs` | 10 | replicated mode: D, how long a fenced block is held past its deadline |
| `contactWindowMs` | 8 | replicated mode: w, the contact window |
| `historySlackMs` | 50 | replicated mode: slack of every history ring past its delay; at most 1000 and at least two blocks |
| `resyncBudgetMs` | 50 | replicated mode: input flow history a resync needs beyond the read window |
| `delayBudgetSamples` | 2^22 | cap on `maxDelaySamples` times `channels`, summed over inputs and outputs |
| `inputs`, `outputs`, `meters` | none | below |
| `matrix` | required | one row per input channel, one column per output channel and then per meter channel |
| `duckColumns` | none | matrix columns whose coefficients a duck mask may duck ([Duck groups](#duck-groups)) |
| `rt` | below | [Real-time set-up](#real-time-set-up) |

`holdMs`, `contactWindowMs` and `resyncBudgetMs` take 1 to 1000. Every
matrix gain is finite and within `samplez_dsp::MAX_GAIN` (1e6).

An input:

| Key | Default | Effect |
| --- | --- | --- |
| `name`, `channels` | required | `channels` are its legs: its matrix rows, meter and strip |
| `domain` | required, none for a tone | the domain of the flows it reads |
| `flowId` | none | the flow read at start; without it the input starts detached ([Inputs](#inputs)) |
| `flowChannels` | none | the 0-based flow channel each leg reads ([Inputs](#inputs)) |
| `offsetSamples` | 0 | block `k` reads the flow's samples `[kB + offset, (k + 1)B + offset)` |
| `tone` | none | `{}` makes the input a [tone generator](#tone-generator) |
| `loudnessChannels` | none | one of `L`, `R`, `C`, `Ls`, `Rs`, `LFE` per channel, weighted per ITU-R BS.1770-5 Table 3; without it every channel weighs 1 |
| `maxDelaySamples` | 0 | [Input delay](#input-delay) |
| `truePeak` | false | true peak on the input's meter ([Meters](#meters)) |
| `preStripMeter` | false | a second meter before the strip ([Meters](#meters)) |
| `preStripRows` | false | matrix rows ahead of the strip ([Channel strip](#channel-strip)) |

A tone generator has `tone: {}` and no `domain`, `flowId`,
`flowChannels` or `offsetSamples`:

```json
{ "name": "lineup", "tone": {}, "channels": 2 }
```

An output:

| Key | Default | Effect |
| --- | --- | --- |
| `name`, `domain`, `flowId`, `channels` | required | the flow it writes |
| `label`, `group`, `role` | required | the flow's label and its group hint `<group>:<role>` |
| `enabled` | true | false creates no flow until an `Output` entry enables it |
| `loudnessChannels` | none | as an input's |
| `strip` | false | [Output strip](#output-strip) |
| `maxDelaySamples` | 0 | [Output delay](#output-delay) |
| `limiter` | false | [Output limiter](#output-limiter) |
| `correlation` | false | [Meters](#meters) |

A meter (`meters`) is matrix columns after the outputs' that are computed
and metered and written to no flow: `name`, `channels`,
`loudnessChannels` and `correlation`, as an output's.

Output flow ids are unique, and no input reads a flow an output writes in
the same domain. A matrix has at most 150 coefficients per sample of the
block (`COEFFICIENTS_PER_BLOCK_SAMPLE`), so that a batch setting all of
them applies within a quarter of the block period: 7,200 at a
`blockSize` of 48, 153,600 at 1024, and never more than
`samplez_dsp::MAX_COEFFICIENTS` (2^18). A coefficient of a column in
`duckColumns` counts 6 times (`DUCKED_TERM_COST`). A larger matrix needs
a larger `blockSize`; a configuration beyond the limit is refused, naming
the `blockSize` it needs.

## samplez-hop

`samplez-hop --config <file>` (`src/bin/samplez-hop.rs`) runs the engine
standalone from a configuration, with the static matrix it names. It logs
each real-time set-up step, stops on SIGINT or SIGTERM and releases its
writers, which deletes the output flows. A usage or configuration error
exits with status 2, an engine that fails to start with status 1.

## Log entries

Every change to what the engine computes is an entry applied at a sample
index:

| Entry | Effect from its index |
| --- | --- |
| `Gain` | a matrix coefficient ramps to its target over `rampSamples`, or as its `Fade` says ([Timed gain transitions](#timed-gain-transitions)) |
| `InputDown` | the input is silent, whatever is readable |
| `InputUp` | the input fades in |
| `Route` | the input crossfades to another flow over `rampSamples` |
| `Detach` | the input crossfades to silence over `rampSamples` and reads no flow, is not polled and is not reported until a `Route` fades it in; its up or down state is kept |
| `Output` | disabled: nothing is written and the writer is released, which deletes the flow unless another writer holds it; enabled: the flow is created as at start-up and written from the first block due once the writer is in |
| `Strip` | one parameter of an input's strip (`samplez_dsp::StripParam`), ramping over `rampSamples` |
| `OutputStrip` | one parameter of an output's strip, as `Strip` |
| `Limiter` | an output's limiter parameters |
| `Delay`, `OutputDelay` | an input's or output's delay |
| `Tone` | a tone generator's frequency, level and ident |
| `Duck`, `DuckMask` | a duck group setting, a coefficient's duck mask |
| `PreStripMeter` | an input's pre-strip meter on or off |
| `LoudnessRun` | a meter's loudness measurement paused or run |
| `ResetLoudness` | a meter's loudness measurement restarted at the first 100 ms window from the index |

Each sets an absolute target, so an entry applied again once its ramp
has ended changes nothing; applied again while it ramps, it starts the
ramp over from where it is, except a `Tone` entry, which leaves a running
ramp alone. An enabled output whose flow another writer holds reports it
in its status and retries the creation.

A standalone engine (`Engine::start`) keeps its own log: `set_gain`,
`set_gain_fade`, `route`, `detach`, `set_output_enabled`, `set_strip`,
`set_output_strip`, `set_limiter`, `set_delay`, `set_output_delay`,
`set_tone`, `set_duck`, `set_duck_mask`, `set_pre_strip_meter`,
`reset_loudness` and `set_loudness_running` append entries to it, and
the engine stamps `InputDown` and `InputUp` from its own input health
([Inputs](#inputs)). Each `InputUp`, `Route` or `Detach` is stamped at
least buildLead after the one before. A replicated engine
(`Engine::start_replicated`) takes entries only from the log
([Replicated mode](ARCHITECTURE.md#replicated-mode)).

An entry the engine cannot hold is refused when it is appended or set: a
value out of range, a strip entry for an input wider than 8 channels, an
`OutputStrip`, `OutputDelay` or `Limiter` entry for an output without
one, a `Tone` entry for an input that reads flows, and `InputDown`,
`InputUp`, `Route` or `Detach` for a tone generator. A strip entry the RT
thread still refuses is counted in `strip_refused`, not applied, and
marks the engine diverged.

## Inputs

An input's `channels` are its legs. Without `flowChannels` the input reads
a flow of exactly that many channels, in order. `flowChannels` names the
0-based flow channel each leg reads, in leg order, as many as `channels`,
each below 64; an index may repeat, so `[0, 0]` feeds one channel to both
legs of a stereo input. The input then reads any flow with more channels
than the highest index, its configured flow and every flow a `Route` moves
it to, and copies only the selected channels. Legs 7 and 8 of a
16-channel flow:

```json
{ "name": "in", "domain": "a", "flowId": "5fbec3b1-1b0f-417d-9059-8b94a47197ed",
  "channels": 2, "flowChannels": [6, 7] }
```

An input reads a flow only if it is 48 kHz float32 audio with those
channels and its readable history covers the read window,
`(maxCatchupBlocks + 2) blockSize + readerMarginSamples + |offsetSamples|`.
An input without `flowId` starts as a `Detach` leaves it: it opens no
reader, is not polled and is not reported until a `Route`. Its status
reports the margin the producer's `maxCommitBatchSizeHint` needs when
`readerMarginSamples` is below it (`margin_short`).

Pruning. An input is pruned after `pruneAfterBlocks` consecutive late
blocks with its head frozen, or after twice the producer's
`maxCommitBatchSizeHint` plus `readerMarginSamples` when that is longer.
A producer that replaces its flow under the same id leaves the input
silent for that limit, as one that stops does. An input whose head
advances behind its blocks, as one stamped on another clock does, is
pruned after that limit or one second of consecutive late blocks,
whichever is longer; a shorter lag is silenced only while it lasts.

A standalone engine stamps `InputDown` when an input is pruned and
`InputUp` once its head has advanced for a second and held the newest
block due at every poll of that second. In replicated mode the audio
voters decide liveness by majority
([Input liveness](ARCHITECTURE.md#input-liveness)).

A pruned input's flow that is missing is looked up again at every poll
(every 100 ms) for 10 s after it was wanted (first polled, re-routed to
or raised), then with a gap from 100 ms doubling up to 5 s. A stopped
head is checked with libmxl's validity check only once a lookup found the
flow's directory, at once and then at the same doubling gaps.

A head that stays farther from the MXL clock than its flow's readable
history (half its ring) plus `readerMarginSamples` and `|offsetSamples|`,
behind or ahead, leaves no block in the ring: the input is silent. Once
the head has stayed that far for as long as that history, moving,
`InputStatus::error` names the gap, as in `head 37.0 s behind the MXL
clock, beyond the flow's history`, and clears once the head is within it
again; each change is logged once. A head unchanged for longer than the
frozen-head prune limit is not reported so.

## Timed gain transitions

A gain change takes an optional `Fade { from, start, len }`, absolute:
the coefficient holds `from` below the sample index `start`, ramps
linearly to the target over `len` samples, and holds the target after.
Its value at index `n` is `from + (n - start) * step`, `step = (target -
from) / len`, computed from `n` directly, so the same bits come out
whatever the block size, the SIMD path or the blocks the ramp spans.
`start` may lie ahead of the entry's index (a delay;
`Fade::after(from, index, delay, len)` builds it) or at it. With no fade
a gain change ramps over `rampSamples` from the value held at its index.

If the fade started before the entry applies, the coefficient joins it
from the value it holds at the index (`Fade::joined`): a coefficient
already on the fade's value stays on the fade, so an entry repeating the
fade mid-ramp, as `Targets::assert` does, changes no bit of it; any other
value goes in a straight line to the target, ending where the fade ends
and taking at least `rampSamples`; a fade already over ramps to its
target over `rampSamples`. An entry that repeats the fade while the
coefficient is on its join starts the join again from the value held: no
step and the same bits on every replica, but rounded apart from the join
that ran without it, and where the join outlasts the fade, ending
`rampSamples` after the repeat.

`from` is a valid gain, `len` at most `samplez_dsp::MAX_EXACT_RAMP_LEN`
(2^23, about 174 s) and `start` at most that far past the entry's index;
an entry beyond them is refused when appended or installed, and a decoded
core holding one is refused. A later entry for the coefficient replaces a
fade still waiting or running. A block counts as ramping in its verdict
until `start + len`. A masked coefficient that a gain change brings in
from silence is ducked from the start, with no mask crossfade of its own.

## Channel strip

Each input of 1 to 8 channels has a strip (`samplez_dsp::strip`): trim
(-20 to 20 dB), per-leg trim and polarity, high-pass, a four-band
equaliser, gate and compressor. It runs on every block computed, after
the input's copy, fade-in or crossfade and its delay line, and before the
matrix, and passes its input unchanged until an entry sets it
(`Engine::set_strip`). Its ramps advance over skipped samples, as the
matrix's do; its filter and envelope state follows the samples it
processed, so after a skip or a silenced read it differs from that of an
engine that computed them, until a resync copies a donor's. A filter
state above 1e30 or not finite, which only a non-finite or overflowing
input leaves, is set to zero after each block.

The compressor's `mix`, 0 to 1, is the share of the compressed signal in
the output, the rest the input unchanged (parallel compression): `(1 -
mix) x + mix c`, `c` the compressor's output with its makeup. It is
applied as one gain per sample, `1 + mix (g - 1)`, `g` the compressor's
gain with the makeup, so the two shares stay sample-aligned, cost nothing
over the compressor alone and add no latency. A mix change ramps over the
strip's ramp. At 0 an enabled compressor passes its input's bits while
its gain keeps following the key, so a mix raised from 0 starts from that
gain; only a compressor switched off rests its gain. At 1, the default,
the output is the compressor's alone.

An input with `preStripRows: true` also feeds the matrix ahead of its
strip: its channels are added as matrix rows after every input's rows, in
configuration order, so the `matrix` and `Gain` entries address them as
rows `input_channels()` on. They carry the input after its delay line and
before the trim and the rest of the strip. They mix, fade and hash as
every row does and count against the coefficient limit, but take no duck:
a duck mask other than 0 on one is refused, and a depth applies to the
input's own rows only.

## Input delay

An input with `maxDelaySamples` above 0, at most 240000 (5 s), has a delay
line (`samplez_dsp::Delay`): two rings per leg of that length plus
`historySlackMs`, allocated at start; an input without one costs nothing.
`delayBudgetSamples` caps `maxDelaySamples` times `channels` summed over
inputs and outputs, since a resync copies at most that much history,
about 6 bytes a sample; a replay builds rings of its own, so during a
resync a replica holds the rings twice more.

A `Delay` entry (`Engine::set_delay`) sets the delay in samples, at most
`maxDelaySamples`; every input takes 0. The line runs first on every block
computed, read or silenced, after the copy, fade-in or crossfade, so every
tap of the input sees it late: the strip, the pre-strip meter and rows,
the matrix, the duck keys and the input's meter. A strip entry at index
`n` applies to the delayed sample there, the input's sample at `n - d`.

Outside a change the line's output at `n` is its input at `n - d`, bit for
bit. A change crossfades from the old tap to the new over `rampSamples`
from its index, and one during a crossfade waits until it ends, the latest
counting. A raised delay reads silence for the samples it reaches back
over, which the line played already, rather than playing them again. The
rings are indexed by sample index, so the output does not depend on the
block size; samples an overrun skipped are stored as silence.

## Tone generator

An input with `tone: {}` is a line-up tone generator (`samplez_dsp::Tone`)
in place of a flow: it reads none, belongs to no input domain and is not
polled, so it is never late, silenced, pruned or down, and its status
reads `InputState::Tone`. Its legs are an input's: matrix rows, strip,
delay line, duck key and meter. It starts at 1 kHz, -18 dBFS, no ident;
switching it on or off is a matter of matrix gains.

A `Tone` entry (`Engine::set_tone`, `samplez_dsp::ToneParams`) sets its
frequency in whole hertz, 20 to 20000, its sample-peak level, -60 to 0
dBFS, and its ident. Every leg at index `n` carries `level * sin(2 pi ((n
f) mod R) / R)`, `R` the sample rate, the phase reduced in integers and
the sine taken by the pinned libm, so the value is a function of `n`
alone: no drift, and the same bits on every replica whatever the block
size or the samples skipped. A level or ident change ramps over
`rampSamples`; a frequency change crossfades from the old sine to the new
one, and one during a crossfade waits until it ends, the latest counting.

With the ident on, leg `l` is silent over every window `[k Q, (k + 1) Q)`
with `k mod (legs + 1) = l`, `Q = R / 4` (250 ms): the legs are cut in
turn, and the window after the last leg's sounds on all of them. Each cut
fades out over the 10 ms before its window and in over the 10 ms after
it. A `Tone` entry that repeats the parameters a generator heads to
leaves its running ramps alone.

## Duck groups

An engine has 8 duck groups (`samplez_dsp::MAX_DUCKS`), each a detector
(`samplez_dsp::duck`) whose envelope, 0 to 1, ducks every matrix
coefficient whose duck mask names the group by its input channel's depth
under the group times the envelope, in dB; of several groups on one
coefficient the deepest counts.

`Entry::Duck` sets one setting of a group (`DuckSetting`): the detector's
parameters, its keys (up to 8 input slots after their strips, each at a
linear gain), its mode (`Auto`, `On`, `Off`), its momentary hold, or the
depth of an input slot's rows, 0 to 40 dB. `Entry::DuckMask` sets the
mask of one coefficient, bit `g` for group `g`; a mask other than 0 is
taken only on a column `duckColumns` names. A depth ramps and a mask
crossfades over `rampSamples`; a parameter or mode change moves only the
detector's decision, so no change steps. A key whose slot changes, or
that is newly keyed, starts with its filters at rest. A ninth key, a slot
keyed twice, a slot beyond the inputs or wider than 8 channels and a
value out of range are refused.

The detectors run on every block computed, after the strips, on the grid
of the sample index, so the output depends on neither the block size nor
where a core was copied. Over skipped samples a detector holds, and a
silenced read keys nothing, so afterwards its envelope differs from an
engine that computed them while the envelope is in transit; once both are
at rest they compute the same bits again. A group with no key, not on
and at rest is advanced rather than run, which computes the same
envelope, 0. A coefficient of a
`duckColumns` column costs 6 plain ones because a ducked term mixes at
5.5 times the instructions on the AVX2 path. `Status::ducks` carries each
group's readings per 10 ms meter slice it is in use in: its largest key,
its largest envelope and whether it was engaged.

## Output strip

An output with `strip: true`, of at most 8 channels, has a channel strip
of the input strip's kind, which passes its input unchanged until an
`OutputStrip` entry sets it (`Engine::set_output_strip`). It runs on the
matrix's output for the output's channels, after the matrix gains and
before the output's delay line and limiter, so the limiter's ceiling holds
on what the strip makes of the mix. The write, the page hash and the
output's meters read after them; a configured meter reads matrix columns
of its own and passes through no output strip. Its state follows the
samples it processed, as an input strip's does.

## Output delay

An output with `maxDelaySamples` above 0, at most 240000 (5 s), has a
delay line of the input delay's kind, counted in `delayBudgetSamples`. An
`OutputDelay` entry (`Engine::set_output_delay`) sets it in samples, at
most `maxDelaySamples`. It runs after the output's strip and before its
limiter, so the ceiling holds on what leaves, a delay's crossfade
included, and the write, the page hash and the output's meters see the
output late. A change crossfades, waits and reaches back over silence as
an input's does.

## Output limiter

An output with `limiter: true`, of at most 8 channels, has a limiter
(`samplez_dsp::Limiter`), off until a `Limiter` entry sets it
(`Engine::set_limiter`, `samplez_dsp::LimiterParams`): on or off, a
ceiling of -20 to 0 dBTP and a release time constant of 1 to 1000 ms. It
runs after the matrix, the output's strip and its delay line, and before
the write, the page hash and the output's meters.

Such an output is `samplez_dsp::LIMITER_DELAY` samples (53, 1.1 ms) later
than one without, on or off; it is its delay plus that much later than the
mix. Switching it on or off ramps over `rampSamples`; a new ceiling or
release takes effect at the next sample. Switched on, it takes the samples
already on their way out into its sidechain first, so the ceiling holds
from the first sample.

It is a feed-forward look-ahead limiter linked over the output's
channels. Its sidechain is the ITU-R BS.1770-5 Annex 2 interpolator of the
true-peak meter: per sample index, the largest of the two samples and the
four phases between them over every channel. The gain each needs to stay
at the ceiling is held over the 1 ms look-ahead, released by the release
time and averaged over the look-ahead, so no output sample exceeds the
ceiling and the true peak the meter reads stays within 0.05 dB of it on
the tests' inter-sample-peak signals. Every value is computed per sample
index in a fixed order, scalar or AVX2 with the same bits; a limiter
started cold matches a running one from the first peak deeper than both
are releasing from. A jump of the sample index restarts it from silence,
and a sample that is not finite is taken as 0.

## Meters

Each output's and meter's status carries, per channel over the last
100 ms, the sample peak, RMS and true peak; the largest true peak, the
largest momentary and the largest short-term loudness since the
measurement started; and momentary, short-term and integrated loudness
and loudness range per EBU R 128. Meters are numbered as their statuses
are: the outputs, then the configured meters. RMS is referenced to a sine
as in AES17, the root mean square times the square root of 2, so a
full-scale sine reads 1.0 (0 dBFS).

Each input's status carries the sample peak, RMS and momentary loudness
per channel after its strip, read or silenced; with `truePeak`, also its
true peak and largest true peak, at an output's cost per channel.

With `preStripMeter` an input reserves a second meter with the same
readings of the input after its delay line and before its strip. A
`PreStripMeter` entry (`Engine::set_pre_strip_meter`) switches it on or
off from its index, and it starts off; on an input without the setting
one switching it on is refused. While on it costs a copy of the block and
a second input meter on the RT thread, and the status carries
`pre_strip_meters`; while off nothing is copied or metered. Switched on,
it starts afresh at the first 10 ms slice at or after the entry's index.

With `correlation`, an output or meter of 2 channels or more carries the
correlation of its legs 0 and 1 over the 400 ms up to the end of the last
100 ms window: the sum of their products over the root of the product of
their sums of squares, -1 to 1, over the 10 ms slices whose records all
arrived. It reads 0 with one leg silent and none with both. It costs one
multiply-add per sample on the RT thread and one meter record more per
10 ms slice. Like the peaks and RMS it reads the samples metered: gap and
paused slices count in it, and a reset of the loudness measurement does
not restart it.

Meters enter no page hash. Meter state is per replica: a core copy
carries none of it but which meters the log holds paused
([Resync](ARCHITECTURE.md#resync)). After samples that were not metered,
the true peak counts only interpolated values computed from samples after
them.

### Loudness measurement

A meter's loudness measurement is reset and paused by log entries, so
every replica resets and pauses it at the same sample. A `ResetLoudness`
entry restarts the integrated loudness, the loudness range, the maxima
and the counts below at the first 100 ms window starting at or after its
index. A `LoudnessRun` entry pauses the measurement from its index, or
runs it again: every 10 ms slice with a sample metered while paused is
left out of the integrated loudness, the loudness range and the maxima,
and counted in `paused`; the momentary and short-term loudness keep
following the output. Once the measurement runs again, the maxima take no
momentary or short-term value reaching back into the pause. `running`
reports whether the last slice metered was not paused; every meter starts
running. A standalone engine takes both through `Engine::reset_loudness`
and `Engine::set_loudness_running`.

Integrated loudness and loudness range leave out every 10 ms slice that is
a gap, lost or paused, and carry on over the rest as if those slices had
not been there. This deviates on purpose from EBU Tech 3341 and 3342,
which measure a programme without interruption: an input taken down or a
slice lost neither blanks the measurement nor lowers it with the silence
it leaves. Momentary and short-term loudness restart after a gap, a lost
slice or a paused slice without a power, not after a paused slice metered
whole. `since` is the sample index the measurement started at; it
restarts only with the process or at a reset.

Which meters the log holds paused is product state
(`Targets::loudness_running`), so a replica rebuilt or handed over pauses
the meters its peers pause. A resync, a rebuild and a hand-over do not
reset the measurement: the replica measures on from its own state, the
slices it lost meanwhile counted in `lost`. A reset entry a replica takes
without a core, or after its window from a replayed or rebuilt core,
restarts the measurement at its window all the same.

### Gaps and lost slices

Each output's and meter's status counts, since the measurement started:

- `gaps`: slices in which an input down by the log (an `InputDown`
  applied; a `Detach` is not one) fed one of the meter's columns through
  a coefficient whose target is other than 0, and slices whose loudness
  power is not finite. A gain change with a delayed fade sets its target
  at its entry's index, so a gap follows the target, not the level heard.
  A read that failed on one replica only is not a gap. Samples an overrun
  or a forward clock step skipped are decided by the same rule.
- `lost`: slices this replica did not meter whole, or metered while
  diverged: skipped samples that are not gaps, blocks not computed without
  a core, blocks computed while the replica is held diverged or resyncing
  before its hand-over (`Engine::set_diverged`), slices whose meter
  records were lost on the way from the RT thread, and after a reset the
  slices from its window to the first one metered. A slice that never
  arrived is counted once a later one is metered. A slice metered while
  diverged is lost even where it would be a gap or paused.

`gaps` and `paused` are equal on all replicas whose `lost` is 0. A
replica with `lost` above 0 left out slices its peers measured, so its
integrated loudness and range may differ slightly from theirs. The
divergence flag covers the blocks from detection to the hand-over and up
to one contact tick around each; blocks computed between the entry that
put the replica apart and its detection are metered as computed.

## Clock

The engine runs on libmxl's clock (`CLOCK_TAI`) and detects a step of it
against `CLOCK_MONOTONIC` at each wake, when it also samples the kernel
TAI offset (`CLOCK_TAI - CLOCK_REALTIME`); the status reports both, and
whether `adjtimex` finds the kernel clock unsynchronised. A block index
is never computed twice: after a backward step the loop computes nothing
until the clock is due at the block after the last one again; after a
forward step it re-anchors to the clock. In replicated mode a detected
step fences the blocks after it until the replicated state clears it
(`clear_step_fence`), or with one voter until its next clean self-report;
a backward step also withholds every block held, and an unsynchronised
kernel clock leaves blocks without a digest.

## Real-time set-up

`rt::setup_process` runs before any other thread exists and the RT
thread sets itself up; each step is reported, and a step that fails runs
degraded rather than failing the start:

| Step | What it does |
| --- | --- |
| `capabilities` | raises the effective capabilities from the permitted set |
| `dumpable` | sets `PR_SET_DUMPABLE` back to 1 after a start with file capabilities |
| `mlock` | `mlockall(MCL_CURRENT \| MCL_FUTURE)` under `CAP_IPC_LOCK` or an unlimited `RLIMIT_MEMLOCK`, else `MCL_CURRENT` only |
| `stack` | caps the default stack of later threads at 512 KiB (`rt::STACK_SIZE`) |
| `isolation` | keeps every later thread off the RT CPU (below) |
| `sched` | `SCHED_FIFO` at `rt.priority`; needs `CAP_SYS_NICE` or `RLIMIT_RTPRIO` at least the priority |
| `affinity` | pins the RT thread to `rt.rtCpu` |
| `ftz_daz` | sets FTZ and DAZ on the RT thread |
| `signals` | blocks signals on the RT thread |

`rt` takes `enabled` (true; false turns the RT steps off), `priority` (70,
1 to 99), `rtCpu` and `isolateRtCpu` (true). `rtCpu` is `auto`, `none`,
`first`, `last` or an index into the allowed CPUs. `auto` pins the RT
thread to the last allowed CPU only when `SAMPLEZ_CPU_REQUEST_MILLICORES`
and `SAMPLEZ_CPU_LIMIT_MILLICORES` are equal whole CPUs matching the
allowed set and the cgroup CPU quota is unlimited or the same size.

With the RT thread pinned and `isolateRtCpu` true, `setup_process` sets
the calling thread's affinity to the allowed CPUs without the RT CPU, so
every thread created after it inherits that set and only the RT thread
runs on its CPU: a time-shared thread there would wait out every block.
While a CPU on another core is allowed, the set also leaves out the RT
CPU's SMT siblings (`thread_siblings_list` in sysfs), which then stay
idle; the `isolation` step names them. With one core allowed its sibling
takes the other threads. With one allowed CPU, or the RT thread not
pinned, isolation is off. Should the RT thread then fail to pin itself,
`affinity` is degraded and the CPU left out stays idle.

The host needs swap off and `vm.compact_unevictable_allowed=0`, so that
neither swapping nor compaction moves the RT thread's locked pages. MXL
domains belong on tmpfs: on a disk-backed file system a commit into an
output's shared mapping can wait for the page's writeback, and the loop
then skips blocks.

## Service layer

`samplez-service` is the non-RT service layer of products: the API,
machine, ops and NMOS listeners with TLS on each, the auth modes `none`,
`token`, `proxy` and `oidc` (OpenID Connect sign-in in the service), the
OpenAPI document and WebSocket send queues. A product passes its
replicated state, its `/api/v1` router, its UI router, its WebSocket
handler and its NMOS router to `Service`, then binds and serves it. The
auth middleware tells a write by its HTTP method and refuses viewers every
method but `GET`, `HEAD` and `OPTIONS`; a product checks the role itself,
with `Connect::require_operator`, before it applies a command received
over the WebSocket.

## NMOS

`samplez-nmos` gives a product an NMOS node for its MXL writers and
readers per AMWA BCP-007-03: one Source, Flow and Sender per writer and
one Receiver per reader, transport `urn:x-nmos:transport:mxl`, with
BCP-004-01 receiver caps and BCP-002-01/02 tags. The product describes
them in a `NodeModel`, implements `Connections` (what a domain holds
under a flow id, and applying an activation) and `Store` (the last
`/active` per port, read at start), serves `Nmos::router()` on its NMOS
listener (`Service::nmos`) and runs `Nmos::registration()`. The crate
links no libmxl and runs on the product's non-RT runtime.

- IS-04 v1.3 (Node API and registration) and IS-05 v1.2 (single and bulk)
  only. `PUT /receivers/{id}/target` answers 501.
- `/active` is written when an activation fires. Each port hands its
  activations to `Connections::apply` one at a time and in order: an
  immediate one once `/active` shows it, a scheduled one
  `Connections::lead` before its time. A cancel after that hands the
  product the still-active connection for the same time. A failed apply
  sets `master_enable` false in `/active` with a new IS-04 version.
- Scheduled activations are held in memory: a restart drops them.
- No IS-10 or BCP-003-01: the APIs answer without authentication, and the
  registration client sends no credentials and uses only registries
  advertising `api_proto=http` and `api_auth=false`, so no TLS.
- No peer-to-peer mode: without a registry the node is not advertised.

Resource versions are TAI times. `Nmos::new` reads them from the
product's `TaiClock`. `Nmos::with_versions` takes them only from the
product, so instances serving one Node, such as a replica set's, serve
the same documents when fed the same changes: a served activation writes
`/active` but leaves the IS-04 resource and its version; the product takes
the change from `Nmos::activation` in its `apply`, replicates it, and
every instance passes it to `Nmos::replicated_change` with the change's
TAI time. The port's version becomes that time, or the next nanosecond
after its own where that is not later, and is returned for the product
to keep: `Nmos::with_versions` at start and `Nmos::set_versions` at a
takeover (which never moves one back) take it, and a `Store` that also
returns the replicated activations (`Store::activations`) starts an
instance on its peers' `/active`. A sender's Source and Flow and a
receiver's caps carry the port's version, the Node and the Device the
latest of all. A failed apply shows the replicated connection in
`/active` again, and a scheduled activation that no longer checks out at
its time goes to the product with `master_enable` false. `/staged` and
pending scheduled activations are each instance's own, so a controller
stages and activates a port on one instance. With shared versions, the
serving instance's `/active` can briefly step back to the activation last
replicated until its newer served one replicates; every instance
converges.

A 200 on a process's first Node POST deletes the Node and registers it
anew, as IS-04 asks of a restarted Node, and so does any 400, once per
pass, as after a replica set is rebuilt under the same Node id. With
`NmosConfig::continue_held` the process heartbeats first instead and,
when the registry still holds the Node, gives every resource a new
version and posts it without deleting the Node, for processes that take
turns under one Node id with the same resource ids, such as a replica
set's leaders. A resource the predecessor registered and the process
lacks stays registered. When the registry refuses any continued resource,
the Node is deleted and every resource registered anew.

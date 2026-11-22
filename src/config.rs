//! The engine configuration, read from a JSON file.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// The most inputs and the most outputs an engine takes: per-block
/// verdicts carry one bit per input and per output.
pub const MAX_INPUTS: usize = 64;

/// The matrix coefficients a configuration may have per sample of its
/// block. A log entry's batch sets every coefficient it holds at its
/// index, in one block, a scene recall the whole matrix, and the block
/// reads, mixes, runs the strips and meters and writes as well. A set
/// takes 20 ns in the builder image into a populated, ramping matrix of
/// 88 by 88 coefficients and 33 ns into one of 384 by 432; at 34 ns a
/// set, a full matrix of this many coefficients per sample is set within
/// a quarter of the block period, 20.8 us a sample at 48 kHz. A larger
/// matrix takes a larger `blockSize`.
pub const COEFFICIENTS_PER_BLOCK_SAMPLE: usize = 150;
/// The coefficients a coefficient of a column in `duckColumns` counts as
/// against [`COEFFICIENTS_PER_BLOCK_SAMPLE`]. That bound is one of set
/// time: a batch setting every coefficient applies within a quarter of
/// the block. Setting a mask costs less than setting a gain, so the set
/// time alone needs no weight; the weight is one of mixing time, laid on
/// the same bound. A ducked term mixes in 17.7 instructions per sample on
/// the AVX2 path against 3.2 for a plain one (callgrind, a 64 by 96
/// matrix at a `blockSize` of 48), 5.5 times; on the scalar path twice.
/// So a configuration taken mixes, with every coefficient of those
/// columns ducked, in no more than a configuration of plain coefficients
/// at the bound.
pub const DUCKED_TERM_COST: usize = 6;
pub const MAX_OUTPUTS: usize = 64;
/// The most meters an engine takes.
pub const MAX_METERS: usize = 64;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_sample_rate")]
    pub sample_rate: u32,
    /// Block size B in samples, 16..=1024.
    #[serde(default = "default_block_size")]
    pub block_size: u32,
    #[serde(default = "default_margin")]
    pub reader_margin_samples: u32,
    #[serde(default = "default_catchup")]
    pub max_catchup_blocks: u32,
    #[serde(default = "default_prune")]
    pub prune_after_blocks: u32,
    /// Length of every gain ramp and input fade in samples, at most
    /// `samplez_dsp::MAX_EXACT_RAMP_LEN`, the longest whose values never
    /// pass its ends, which a decoded strip is held to.
    #[serde(default = "default_ramp")]
    pub ramp_samples: u32,
    /// Replicated mode: D, how long past a block's deadline T_k a fenced
    /// block is held before it is withheld, in ms.
    #[serde(default = "default_hold_ms")]
    pub hold_ms: u32,
    /// Replicated mode: w, the contact window, in ms. A held block is
    /// withheld at `T_k + D + w`.
    #[serde(default = "default_contact_window_ms")]
    pub contact_window_ms: u32,
    /// Replicated mode: the slack of every history ring past its delay, in
    /// ms. A history copy is kept only while the RT thread has not
    /// reached the slack's last block.
    #[serde(default = "default_history_slack_ms")]
    pub history_slack_ms: u32,
    /// Replicated mode with more than one voter: the time a resync takes
    /// to hand over, in ms. Every input flow must keep that much history
    /// beyond the read window.
    #[serde(default = "default_resync_budget_ms")]
    pub resync_budget_ms: u32,
    /// The most delay-line history the inputs and outputs may hold
    /// together: `maxDelaySamples` times `channels`, summed over the
    /// inputs and outputs, in samples; a resync copies at most that much.
    /// 2^22 by default.
    #[serde(default = "default_delay_budget")]
    pub delay_budget_samples: u64,
    /// Path of the libmxl shared library.
    pub libmxl: PathBuf,
    pub domains: Vec<Domain>,
    #[serde(default)]
    pub inputs: Vec<Input>,
    #[serde(default)]
    pub outputs: Vec<Output>,
    /// Matrix columns that are computed and metered and written to no
    /// flow, after the outputs' columns.
    #[serde(default)]
    pub meters: Vec<Meter>,
    /// Gain from each input channel (row) to each output channel and then
    /// each meter channel (column), channels numbered across inputs,
    /// outputs and meters in configuration order.
    pub matrix: Vec<Vec<f32>>,
    /// The matrix columns whose coefficients a duck mask may duck, each
    /// once; none by default.
    #[serde(default)]
    pub duck_columns: Vec<usize>,
    #[serde(default)]
    pub rt: Rt,
    /// How the engine looks a flow's directory up before libmxl opens the
    /// flow; tests replace it.
    #[doc(hidden)]
    #[serde(skip, default = "default_look")]
    pub look: samplez_mxl::lookup::Look,
}

fn default_look() -> samplez_mxl::lookup::Look {
    samplez_mxl::lookup::exists
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Domain {
    pub name: String,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Input {
    pub name: String,
    /// The domain of the flows the input reads; none, and required to be
    /// none, for a tone generator.
    #[serde(default)]
    pub domain: Option<String>,
    /// Makes the input a line-up tone generator
    /// ([`samplez_dsp::Tone`]) in place of a flow: it reads no flow, has
    /// no domain, `flowId`, `flowChannels` or `offsetSamples`, is never
    /// late or pruned and takes no `InputDown`, `InputUp`, `Route` or
    /// `Detach`. Its legs are routed as an input's: matrix rows, strip,
    /// delay, duck key and meter. `Tone` entries set it.
    #[serde(default)]
    pub tone: Option<Tone>,
    /// The flow the input reads from the start; none starts it detached,
    /// as a `Detach` leaves it: no reader is opened until a `Route`. In
    /// replicated mode the product's targets set the flow from their
    /// first record and every rebuild on: this is the start until then,
    /// and has to match the product's initial targets.
    #[serde(default)]
    pub flow_id: Option<String>,
    /// The input's legs: its matrix rows, meter and strip width.
    pub channels: u32,
    /// The 0-based flow channel each leg reads, `channels` long, each
    /// below `samplez_mxl::input::MAX_FLOW_CHANNELS`; an index may repeat.
    /// The input then reads any flow with more channels than the highest
    /// index, and so does every flow a `Route` moves it to. Without it the
    /// flow has exactly `channels` channels, read in order.
    #[serde(default)]
    pub flow_channels: Option<Vec<u32>>,
    #[serde(default)]
    pub offset_samples: i64,
    /// The loudness role of each channel, as for an output; weights the
    /// momentary loudness of the input's meter.
    #[serde(default)]
    pub loudness_channels: Option<Vec<String>>,
    /// The longest delay a `Delay` entry may set on the input, in
    /// samples, at most `samplez_dsp::MAX_DELAY_SAMPLES` (5 s); 0, the
    /// default, gives the input no delay line. The line runs ahead of the
    /// strip and of every tap of the input. Its rings, two per leg of
    /// this length plus `historySlackMs`, are allocated at start.
    #[serde(default)]
    pub max_delay_samples: u32,
    /// Meters the input's true peak per channel, as an output's: 48
    /// multiply-adds per sample per channel on the RT thread. Off by
    /// default; the input's meters then carry no true peak. Meters are
    /// per replica and enter no page hash, so replicas that differ in it
    /// compute the same pages; a product may still keep it in its
    /// configuration hash to keep the meters of a set alike.
    #[serde(default)]
    pub true_peak: bool,
    /// Adds a second meter on the input: the selected input before its
    /// strip, metered as the input's meter is (`truePeak` included), so
    /// its trim, EQ and dynamics do not reach it. Before the strip means
    /// the block as read or generated, its `flowChannels` selected, after
    /// the fade-in, a re-route's crossfade, silencing and the input's
    /// delay line. Off by default: the input then has no such meter and
    /// no block copy is made for it.
    #[serde(default)]
    pub pre_strip_meter: bool,
    /// Adds the input's channels as matrix rows after every input's
    /// rows, in configuration order, fed the input before its strip as
    /// `preStripMeter` meters it, delayed by its delay line. Off by
    /// default: the input then has no such rows. The rows count against
    /// the matrix's coefficient limits as every row does.
    #[serde(default)]
    pub pre_strip_rows: bool,
}

/// A tone generator's configuration: none beyond its presence, `{}`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tone {}

impl Input {
    /// The loudness weight of each channel.
    pub fn loudness_weights(&self) -> Result<Vec<f64>, Error> {
        loudness_weights(
            "input",
            &self.name,
            self.channels,
            self.loudness_channels.as_deref(),
        )
    }
}

/// Matrix columns metered like an output and written nowhere.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Meter {
    pub name: String,
    pub channels: u32,
    /// The loudness role of each channel, as for an output.
    #[serde(default)]
    pub loudness_channels: Option<Vec<String>>,
    /// Meters the correlation of legs 0 and 1 (L and R of a stereo or
    /// wider element) in [`crate::OutputMeters::correlation`]: one
    /// multiply-add per sample on the RT thread. Off by default; at least
    /// 2 channels. A product that hashes its configuration into its
    /// compatibility key covers it there, so replicas metering
    /// differently do not run as peers.
    #[serde(default)]
    pub correlation: bool,
}

impl Meter {
    /// The loudness weight of each channel.
    pub fn loudness_weights(&self) -> Result<Vec<f64>, Error> {
        loudness_weights(
            "meter",
            &self.name,
            self.channels,
            self.loudness_channels.as_deref(),
        )
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Output {
    pub name: String,
    pub domain: String,
    pub flow_id: String,
    pub label: String,
    pub group: String,
    pub role: String,
    pub channels: u32,
    /// Whether the output starts enabled, true by default; one started
    /// disabled creates no flow until an `Output` entry enables it. In
    /// replicated mode the product's targets set it only by an `Output`
    /// entry, which their diff emits when the output's enabled state
    /// changes, or by a rebuild, so it has to match the product's initial
    /// targets.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// The loudness role of each channel: `L`, `R`, `C`, `Ls`, `Rs` or
    /// `LFE`, weighted per ITU-R BS.1770-5 Table 3 (the LFE channel is not
    /// measured). Without it every channel is weighted 1.
    #[serde(default)]
    pub loudness_channels: Option<Vec<String>>,
    /// Whether the output has a limiter ([`samplez_dsp::Limiter`]), off
    /// until a `Limiter` entry sets it; false by default. An output with
    /// one is [`samplez_dsp::LIMITER_DELAY`] samples late, the limiter on
    /// or off. At most [`samplez_dsp::strip::MAX_LEGS`] channels wide.
    #[serde(default)]
    pub limiter: bool,
    /// Meters the correlation of legs 0 and 1 (L and R of a stereo or
    /// wider element) in [`crate::OutputMeters::correlation`]: one
    /// multiply-add per sample on the RT thread. Off by default; at least
    /// 2 channels. A product that hashes its configuration into its
    /// compatibility key covers it there, so replicas metering
    /// differently do not run as peers.
    #[serde(default)]
    pub correlation: bool,
    /// Whether the output has a channel strip ([`samplez_dsp::Strip`]),
    /// run on the matrix's output before the limiter and passing it
    /// unchanged until an `OutputStrip` entry sets it; false by default.
    /// At most [`samplez_dsp::strip::MAX_LEGS`] channels wide.
    #[serde(default)]
    pub strip: bool,
    /// The longest delay an `OutputDelay` entry may set on the output, in
    /// samples, at most `samplez_dsp::MAX_DELAY_SAMPLES` (5 s); 0, the
    /// default, gives the output no delay line. As an input's: its rings
    /// are allocated at start and count in `delayBudgetSamples`. The line
    /// runs after the strip and before the limiter.
    #[serde(default)]
    pub max_delay_samples: u32,
}

impl Output {
    /// The loudness weight of each channel.
    pub fn loudness_weights(&self) -> Result<Vec<f64>, Error> {
        loudness_weights(
            "output",
            &self.name,
            self.channels,
            self.loudness_channels.as_deref(),
        )
    }
}

/// The loudness weight of each of `channels` channels named by `names`;
/// every channel weighs 1 without names.
fn loudness_weights(
    what: &str,
    name: &str,
    channels: u32,
    names: Option<&[String]>,
) -> Result<Vec<f64>, Error> {
    use samplez_dsp::weight;
    let Some(names) = names else {
        return Ok(vec![weight::FRONT; channels as usize]);
    };
    if names.len() != channels as usize {
        return Err(Error(format!(
            "{what} {name}: loudnessChannels names {} channels, the {what} has {channels}",
            names.len(),
        )));
    }
    names
        .iter()
        .map(|n| match n.as_str() {
            "L" | "R" | "C" => Ok(weight::FRONT),
            "Ls" | "Rs" => Ok(weight::SURROUND),
            "LFE" => Ok(weight::LFE),
            _ => Err(Error(format!(
                "{what} {name}: loudness channel {n:?} is not one of L, R, C, Ls, Rs, LFE"
            ))),
        })
        .collect()
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Rt {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// SCHED_FIFO priority, 1..=99.
    #[serde(default = "default_priority")]
    pub priority: i32,
    #[serde(default)]
    pub rt_cpu: RtCpu,
    /// Keeps every other thread off the RT thread's CPU when it is pinned.
    #[serde(default = "default_true")]
    pub isolate_rt_cpu: bool,
}

impl Default for Rt {
    fn default() -> Self {
        Rt {
            enabled: true,
            priority: default_priority(),
            rt_cpu: RtCpu::default(),
            isolate_rt_cpu: true,
        }
    }
}

/// Where the RT thread is pinned: `auto`, `none`, `first`, `last` or an
/// index into the allowed CPU set.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(from = "RtCpuValue")]
pub enum RtCpu {
    #[default]
    Auto,
    None,
    First,
    Last,
    Index(usize),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RtCpuValue {
    Name(CpuName),
    Index(usize),
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum CpuName {
    Auto,
    None,
    First,
    Last,
}

impl From<RtCpuValue> for RtCpu {
    fn from(v: RtCpuValue) -> Self {
        match v {
            RtCpuValue::Name(CpuName::Auto) => RtCpu::Auto,
            RtCpuValue::Name(CpuName::None) => RtCpu::None,
            RtCpuValue::Name(CpuName::First) => RtCpu::First,
            RtCpuValue::Name(CpuName::Last) => RtCpu::Last,
            RtCpuValue::Index(i) => RtCpu::Index(i),
        }
    }
}

fn default_sample_rate() -> u32 {
    48_000
}
fn default_block_size() -> u32 {
    48
}
fn default_margin() -> u32 {
    576
}
fn default_catchup() -> u32 {
    2
}
fn default_prune() -> u32 {
    50
}
fn default_hold_ms() -> u32 {
    10
}
fn default_contact_window_ms() -> u32 {
    8
}
fn default_history_slack_ms() -> u32 {
    50
}
fn default_resync_budget_ms() -> u32 {
    50
}
fn default_ramp() -> u32 {
    480
}
fn default_delay_budget() -> u64 {
    1 << 22
}

fn default_true() -> bool {
    true
}
fn default_priority() -> i32 {
    70
}

#[derive(Debug)]
pub struct Error(pub String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "config: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl Config {
    /// Reads and validates the configuration at `path`.
    pub fn load(path: &Path) -> Result<Config, Error> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error(format!("reading {}: {e}", path.display())))?;
        Config::parse(&text)
    }

    pub fn parse(text: &str) -> Result<Config, Error> {
        let c: Config = serde_json::from_str(text).map_err(|e| Error(e.to_string()))?;
        c.validate()?;
        Ok(c)
    }

    pub fn input_channels(&self) -> usize {
        self.inputs.iter().map(|i| i.channels as usize).sum()
    }

    /// The matrix rows: the input channels, then the channels of each
    /// input whose config sets `preStripRows`, in configuration order.
    pub fn rows(&self) -> usize {
        let pre: usize = (self.inputs.iter())
            .filter(|i| i.pre_strip_rows)
            .map(|i| i.channels as usize)
            .sum();
        self.input_channels() + pre
    }

    pub fn output_channels(&self) -> usize {
        self.outputs.iter().map(|o| o.channels as usize).sum()
    }

    pub fn meter_channels(&self) -> usize {
        self.meters.iter().map(|m| m.channels as usize).sum()
    }

    /// The matrix columns: the output channels, then the meter channels.
    pub fn columns(&self) -> usize {
        self.output_channels() + self.meter_channels()
    }

    /// The hold ring of a replicated output in blocks: `D + w + 2B`
    /// samples, rounded up to whole blocks.
    pub fn hold_blocks(&self) -> usize {
        let samples =
            u64::from(self.hold_ms + self.contact_window_ms) * u64::from(self.sample_rate) / 1000;
        samples.div_ceil(u64::from(self.block_size)) as usize + 2
    }

    /// The slack of a history ring in whole blocks, rounded down.
    pub fn history_slack_blocks(&self) -> u64 {
        u64::from(self.history_slack_ms) * u64::from(self.sample_rate)
            / 1000
            / u64::from(self.block_size)
    }

    /// `resyncBudgetMs` in samples.
    pub fn resync_budget_samples(&self) -> u64 {
        u64::from(self.resync_budget_ms) * u64::from(self.sample_rate) / 1000
    }

    /// The index of the domain named `name`.
    pub fn domain(&self, name: &str) -> Option<usize> {
        self.domains.iter().position(|d| d.name == name)
    }

    fn validate(&self) -> Result<(), Error> {
        let fail = |m: String| Err(Error(m));
        if self.sample_rate != 48_000 {
            return fail(format!("sampleRate {} is not 48000", self.sample_rate));
        }
        if !(16..=1024).contains(&self.block_size) {
            return fail(format!("blockSize {} is not in 16..=1024", self.block_size));
        }
        if !(1..=1000).contains(&self.hold_ms) || !(1..=1000).contains(&self.contact_window_ms) {
            return fail(format!(
                "holdMs {} and contactWindowMs {} must be in 1..=1000",
                self.hold_ms, self.contact_window_ms
            ));
        }
        if !(1..=1000).contains(&self.resync_budget_ms) || self.history_slack_ms > 1000 {
            return fail(format!(
                "resyncBudgetMs {} must be in 1..=1000 and historySlackMs {} at most 1000",
                self.resync_budget_ms, self.history_slack_ms
            ));
        }
        // The block in progress and one guard block.
        if self.history_slack_blocks() < 2 {
            return fail(format!(
                "historySlackMs {} is shorter than two blocks of {}",
                self.history_slack_ms, self.block_size
            ));
        }
        if self.ramp_samples > samplez_dsp::MAX_EXACT_RAMP_LEN {
            return fail(format!(
                "rampSamples {} is above {}",
                self.ramp_samples,
                samplez_dsp::MAX_EXACT_RAMP_LEN
            ));
        }
        if !(1..=99).contains(&self.rt.priority) {
            return fail(format!("rt.priority {} is not in 1..=99", self.rt.priority));
        }
        let delays: u64 = (self.inputs.iter())
            .map(|i| u64::from(i.max_delay_samples) * u64::from(i.channels))
            .chain(
                (self.outputs.iter())
                    .map(|o| u64::from(o.max_delay_samples) * u64::from(o.channels)),
            )
            .sum();
        if delays > self.delay_budget_samples {
            return fail(format!(
                "maxDelaySamples times channels sums to {delays} samples over the inputs and \
                 outputs, above delayBudgetSamples {}",
                self.delay_budget_samples
            ));
        }
        if self.inputs.len() > MAX_INPUTS {
            return fail(format!(
                "{} inputs, at most {MAX_INPUTS}",
                self.inputs.len()
            ));
        }
        if self.outputs.len() > MAX_OUTPUTS {
            return fail(format!(
                "{} outputs, at most {MAX_OUTPUTS}",
                self.outputs.len()
            ));
        }
        let coefficients = self.rows().saturating_mul(self.columns());
        if coefficients > samplez_dsp::MAX_COEFFICIENTS {
            return fail(format!(
                "{} matrix rows by {} output and meter channels, at most {} matrix coefficients",
                self.rows(),
                self.columns(),
                samplez_dsp::MAX_COEFFICIENTS
            ));
        }
        if let Some(&c) = self.duck_columns.iter().find(|&&c| c >= self.columns()) {
            return fail(format!(
                "duckColumns names column {c}, of {} columns",
                self.columns()
            ));
        }
        let mut seen = HashSet::new();
        if let Some(c) = self.duck_columns.iter().find(|&&c| !seen.insert(c)) {
            return fail(format!("duckColumns names column {c} twice"));
        }
        // Of every column in duckColumns, every coefficient counts at a
        // ducked term's cost.
        let coefficients =
            coefficients + (DUCKED_TERM_COST - 1) * self.rows() * self.duck_columns.len();
        let per_block = self.block_size as usize * COEFFICIENTS_PER_BLOCK_SAMPLE;
        if coefficients > per_block {
            let needed = coefficients.div_ceil(COEFFICIENTS_PER_BLOCK_SAMPLE);
            return fail(format!(
                "{coefficients} matrix coefficients, those of duckColumns {DUCKED_TERM_COST} \
                 times, at most {COEFFICIENTS_PER_BLOCK_SAMPLE} per sample of the block, \
                 {per_block} at blockSize {}, so that a batch setting all of them applies in \
                 one block; a blockSize of at least {needed} takes them",
                self.block_size
            ));
        }
        if self.meters.len() > MAX_METERS {
            return fail(format!(
                "{} meters, at most {MAX_METERS}",
                self.meters.len()
            ));
        }
        unique(self.domains.iter().map(|d| d.name.as_str()), "domain")?;
        unique(self.inputs.iter().map(|i| i.name.as_str()), "input")?;
        unique(self.outputs.iter().map(|o| o.name.as_str()), "output")?;
        unique(self.meters.iter().map(|m| m.name.as_str()), "meter")?;
        unique(
            self.outputs.iter().map(|o| o.flow_id.as_str()),
            "output flow id",
        )?;
        for i in &self.inputs {
            if i.channels == 0 {
                return fail(format!("input {}: channels must be at least 1", i.name));
            }
            if i.tone.is_some() {
                if i.domain.is_some()
                    || i.flow_id.is_some()
                    || i.flow_channels.is_some()
                    || i.offset_samples != 0
                {
                    return fail(format!(
                        "input {}: a tone generator takes no domain, flowId, flowChannels or \
                         offsetSamples",
                        i.name
                    ));
                }
            } else {
                let Some(domain) = &i.domain else {
                    return fail(format!("input {}: no domain, and no tone", i.name));
                };
                if self.domain(domain).is_none() {
                    return fail(format!("input {}: no domain {domain}", i.name));
                }
                let flow_id = i.flow_id.as_deref().unwrap_or("none");
                samplez_mxl::input::legs(&self.input_config(i, flow_id))
                    .map_err(|e| Error(format!("input {}: {e}", i.name)))?;
            }
            i.loudness_weights()?;
            if i.max_delay_samples > samplez_dsp::MAX_DELAY_SAMPLES {
                return fail(format!(
                    "input {}: maxDelaySamples {} is above {}",
                    i.name,
                    i.max_delay_samples,
                    samplez_dsp::MAX_DELAY_SAMPLES
                ));
            }
        }
        for m in &self.meters {
            if m.channels == 0 {
                return fail(format!("meter {}: channels must be at least 1", m.name));
            }
            m.loudness_weights()?;
            if m.correlation && m.channels < 2 {
                return fail(format!(
                    "meter {}: 1 channel, correlation takes at least 2",
                    m.name
                ));
            }
        }
        for o in &self.outputs {
            if self.domain(&o.domain).is_none() {
                return fail(format!("output {}: no domain {}", o.name, o.domain));
            }
            samplez_mxl::output::flow_definition(&self.output_config(o))
                .map_err(|e| Error(format!("output {}: {e}", o.name)))?;
            o.loudness_weights()?;
            let max = samplez_dsp::strip::MAX_LEGS;
            for (on, what) in [(o.limiter, "a limiter"), (o.strip, "a strip")] {
                if on && o.channels as usize > max {
                    return fail(format!(
                        "output {}: {} channels, {what} takes at most {max}",
                        o.name, o.channels
                    ));
                }
            }
            if o.correlation && o.channels < 2 {
                return fail(format!(
                    "output {}: 1 channel, correlation takes at least 2",
                    o.name
                ));
            }
            if o.max_delay_samples > samplez_dsp::MAX_DELAY_SAMPLES {
                return fail(format!(
                    "output {}: maxDelaySamples {} is above {}",
                    o.name,
                    o.max_delay_samples,
                    samplez_dsp::MAX_DELAY_SAMPLES
                ));
            }
        }
        for (slot, i) in self.inputs.iter().enumerate() {
            if let Some(flow_id) = &i.flow_id {
                self.check_input_flow(slot, flow_id)?;
            }
        }
        let (n, m) = (self.rows(), self.columns());
        if self.matrix.len() != n || self.matrix.iter().any(|row| row.len() != m) {
            return fail(format!(
                "matrix must have {n} rows (input channels, then preStripRows channels) of {m} \
                 gains (output and meter \
                 channels)"
            ));
        }
        if let Some(g) = self
            .matrix
            .iter()
            .flatten()
            .find(|&&g| !samplez_dsp::valid_gain(g))
        {
            return fail(format!("matrix gain {g} is not finite within MAX_GAIN"));
        }
        Ok(())
    }

    /// Whether input `slot` reads flows: it exists and is no tone
    /// generator, so it takes `InputDown`, `InputUp`, `Route` and
    /// `Detach`.
    pub fn check_flow_slot(&self, slot: usize) -> Result<(), Error> {
        let i = self
            .inputs
            .get(slot)
            .ok_or_else(|| Error(format!("no input slot {slot}")))?;
        if i.tone.is_some() {
            return Err(Error(format!(
                "input {} is a tone generator: it reads no flow and is never down",
                i.name
            )));
        }
        Ok(())
    }

    /// Whether input `slot` may read flow `flow_id`: no tone generator, a
    /// valid id, and no output's flow in the input's domain.
    pub fn check_input_flow(&self, slot: usize, flow_id: &str) -> Result<(), Error> {
        self.check_flow_slot(slot)?;
        let i = &self.inputs[slot];
        samplez_mxl::validate_id(flow_id).map_err(|e| Error(format!("input {}: {e}", i.name)))?;
        let domain = i.domain.as_deref().and_then(|d| self.domain(d));
        if let Some(o) = self
            .outputs
            .iter()
            .find(|o| o.flow_id == flow_id && domain == self.domain(&o.domain))
        {
            return Err(Error(format!(
                "output {} writes flow {flow_id} in the domain of input {}",
                o.name, i.name
            )));
        }
        Ok(())
    }

    /// The samplez-mxl attach parameters of `i` reading `flow_id`.
    pub fn input_config<'a>(
        &self,
        i: &'a Input,
        flow_id: &'a str,
    ) -> samplez_mxl::input::InputConfig<'a> {
        samplez_mxl::input::InputConfig {
            flow_id,
            channels: i.channels,
            flow_channels: i.flow_channels.as_deref(),
            offset: i.offset_samples,
        }
    }

    /// The samplez-mxl creation parameters of `o`.
    pub fn output_config<'a>(&self, o: &'a Output) -> samplez_mxl::output::OutputConfig<'a> {
        samplez_mxl::output::OutputConfig {
            flow_id: &o.flow_id,
            label: &o.label,
            group: &o.group,
            role: &o.role,
            channels: o.channels,
            block: u64::from(self.block_size),
            batch: u64::from(self.block_size),
        }
    }
}

fn unique<'a>(names: impl Iterator<Item = &'a str>, what: &str) -> Result<(), Error> {
    let mut seen = HashSet::new();
    for n in names {
        if !seen.insert(n) {
            return Err(Error(format!("{what} {n} appears twice")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const IN: &str = "5fbec3b1-1b0f-417d-9059-8b94a47197ed";
    const OUT: &str = "6fbec3b1-1b0f-417d-9059-8b94a47197ed";

    fn base() -> serde_json::Value {
        serde_json::json!({
            "libmxl": "/opt/mxl/lib/libmxl.so",
            "domains": [{ "name": "a", "path": "/mxl/a" }],
            "inputs": [{ "name": "in", "domain": "a", "flowId": IN, "channels": 2 }],
            "outputs": [{ "name": "out", "domain": "a", "flowId": OUT, "label": "hop out",
                          "group": "hop", "role": "Audio", "channels": 1 }],
            "matrix": [[0.5], [0.5]],
        })
    }

    fn parse(v: &serde_json::Value) -> Result<Config, Error> {
        Config::parse(&v.to_string())
    }

    #[test]
    fn defaults_follow_the_plan() {
        let c = parse(&base()).unwrap();
        assert_eq!(c.sample_rate, 48_000);
        assert_eq!(c.block_size, 48);
        assert_eq!(c.reader_margin_samples, 576);
        assert_eq!(c.max_catchup_blocks, 2);
        assert_eq!(c.prune_after_blocks, 50);
        assert_eq!(c.ramp_samples, 480);
        assert!(c.rt.enabled);
        assert_eq!(c.rt.rt_cpu, RtCpu::Auto);
        assert_eq!(c.inputs[0].offset_samples, 0);
        assert_eq!((c.hold_ms, c.contact_window_ms), (10, 8));
        assert_eq!((c.history_slack_ms, c.resync_budget_ms), (50, 50));
        assert_eq!(c.history_slack_blocks(), 50);
        assert_eq!(c.resync_budget_samples(), 2400);
        // D + w + 2B: 864 + 96 samples.
        assert_eq!(c.hold_blocks() as u64 * 48, 960);
    }

    /// An output has no limiter unless `limiter` is true, and takes one
    /// only up to a strip's width.
    #[test]
    fn a_limiter_is_opt_in_and_at_most_eight_channels_wide() {
        assert!(!parse(&base()).unwrap().outputs[0].limiter);
        let mut v = base();
        v["outputs"][0]["limiter"] = serde_json::json!(true);
        assert!(parse(&v).unwrap().outputs[0].limiter);
        v["outputs"][0]["channels"] = serde_json::json!(9);
        v["matrix"] = serde_json::json!(vec![vec![0.5; 9]; 2]);
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.contains("a limiter takes at most 8"), "{e}");
        v["outputs"][0]["limiter"] = serde_json::json!(false);
        assert!(parse(&v).is_ok());
    }

    /// Correlation is metered only where an output or a configured meter
    /// asks for it, and only on 2 channels or more: a mono element has no
    /// second leg.
    #[test]
    fn correlation_is_opt_in_and_needs_two_channels() {
        let mut v = base();
        v["meters"] = serde_json::json!([{ "name": "aux", "channels": 1 }]);
        v["matrix"] = serde_json::json!([[0.5, 0.5], [0.5, 0.5]]);
        let c = parse(&v).unwrap();
        assert!(!c.outputs[0].correlation && !c.meters[0].correlation);
        v["outputs"][0]["correlation"] = serde_json::json!(true);
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.contains("output out: 1 channel, correlation"), "{e}");
        v["outputs"][0]["channels"] = serde_json::json!(2);
        v["meters"][0]["correlation"] = serde_json::json!(true);
        v["matrix"] = serde_json::json!([[0.5, 0.5, 0.5], [0.5, 0.5, 0.5]]);
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.contains("meter aux: 1 channel, correlation"), "{e}");
        v["meters"][0]["channels"] = serde_json::json!(6);
        v["matrix"] = serde_json::json!(vec![vec![0.5; 8]; 2]);
        let c = parse(&v).unwrap();
        assert!(c.outputs[0].correlation && c.meters[0].correlation);
    }

    /// An output has no strip unless `strip` is true, and takes one only
    /// up to a strip's width.
    #[test]
    fn an_output_strip_is_opt_in_and_at_most_eight_channels_wide() {
        assert!(!parse(&base()).unwrap().outputs[0].strip);
        let mut v = base();
        v["outputs"][0]["strip"] = serde_json::json!(true);
        assert!(parse(&v).unwrap().outputs[0].strip);
        v["outputs"][0]["channels"] = serde_json::json!(9);
        v["matrix"] = serde_json::json!(vec![vec![0.5; 9]; 2]);
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.contains("9 channels, a strip takes at most 8"), "{e}");
        v["outputs"][0]["strip"] = serde_json::json!(false);
        assert!(parse(&v).is_ok());
    }

    /// The delay lines of every input together hold at most
    /// `delayBudgetSamples` samples of history, 2^22 by default.
    #[test]
    fn delay_lines_stay_within_the_budget() {
        let mut v = base();
        assert_eq!(parse(&v).unwrap().delay_budget_samples, 1 << 22);
        v["inputs"][0]["maxDelaySamples"] = serde_json::json!(240_000);
        v["delayBudgetSamples"] = serde_json::json!(480_000);
        assert!(parse(&v).is_ok(), "2 channels of 240000");
        v["delayBudgetSamples"] = serde_json::json!(479_999);
        let e = parse(&v).unwrap_err().to_string();
        assert!(
            e.contains("sums to 480000 samples") && e.contains("delayBudgetSamples 479999"),
            "{e}"
        );
    }

    /// An input has no delay line unless `maxDelaySamples` names one, at
    /// most 5 s.
    #[test]
    fn a_delay_line_is_opt_in_and_at_most_five_seconds() {
        assert_eq!(parse(&base()).unwrap().inputs[0].max_delay_samples, 0);
        let mut v = base();
        v["inputs"][0]["maxDelaySamples"] = serde_json::json!(240_000);
        assert_eq!(parse(&v).unwrap().inputs[0].max_delay_samples, 240_000);
        v["inputs"][0]["maxDelaySamples"] = serde_json::json!(240_001);
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.contains("maxDelaySamples 240001 is above 240000"), "{e}");
    }

    /// An output has no delay line unless `maxDelaySamples` names one, at
    /// most 5 s, and its line counts in the budget the inputs' lines share:
    /// an input's and an output's lines together over it are refused.
    #[test]
    fn an_output_delay_line_is_opt_in_and_counts_in_the_budget() {
        assert_eq!(parse(&base()).unwrap().outputs[0].max_delay_samples, 0);
        let mut v = base();
        v["outputs"][0]["maxDelaySamples"] = serde_json::json!(240_000);
        assert_eq!(parse(&v).unwrap().outputs[0].max_delay_samples, 240_000);
        v["outputs"][0]["maxDelaySamples"] = serde_json::json!(240_001);
        let e = parse(&v).unwrap_err().to_string();
        assert!(
            e.contains("output out: maxDelaySamples 240001 is above 240000"),
            "{e}"
        );
        // 2 input channels of 1000 and 1 output channel of 2000.
        v["inputs"][0]["maxDelaySamples"] = serde_json::json!(1000);
        v["outputs"][0]["maxDelaySamples"] = serde_json::json!(2000);
        v["delayBudgetSamples"] = serde_json::json!(4000);
        assert!(parse(&v).is_ok(), "4000 of 4000");
        v["delayBudgetSamples"] = serde_json::json!(3999);
        let e = parse(&v).unwrap_err().to_string();
        assert!(
            e.contains("sums to 4000 samples over the inputs and outputs")
                && e.contains("delayBudgetSamples 3999"),
            "{e}"
        );
    }

    /// An input's flow is optional: absent or null starts it without one,
    /// an empty id is refused as any invalid id is, and an input without
    /// a flow skips the check against the outputs' flows. An output
    /// starts enabled unless `enabled` is false.
    #[test]
    fn an_input_flow_is_optional_and_an_output_starts_enabled_by_default() {
        let c = parse(&base()).unwrap();
        assert_eq!(c.inputs[0].flow_id.as_deref(), Some(IN));
        assert!(c.outputs[0].enabled);
        let mut v = base();
        v["inputs"][0].as_object_mut().unwrap().remove("flowId");
        v["outputs"][0]["enabled"] = serde_json::json!(false);
        let c = parse(&v).unwrap();
        assert_eq!(c.inputs[0].flow_id, None);
        assert!(!c.outputs[0].enabled);
        v["inputs"][0]["flowId"] = serde_json::Value::Null;
        assert_eq!(parse(&v).unwrap().inputs[0].flow_id, None);
        v["inputs"][0]["flowId"] = serde_json::json!("");
        assert!(parse(&v).is_err(), "an empty flow id taken");
        // Reading the output's flow is refused only for an input with one.
        v["inputs"][0]["flowId"] = serde_json::json!(OUT);
        assert!(
            parse(&v).is_err(),
            "an input reading the output's flow taken"
        );
        v["inputs"][0].as_object_mut().unwrap().remove("flowId");
        v["inputs"][0]["flowChannels"] = serde_json::json!([0]);
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.contains("flow none"), "{e}");
    }

    /// A matrix beyond `MAX_COEFFICIENTS` is refused with the
    /// configuration, as a replica refuses a core holding one.
    #[test]
    fn a_matrix_beyond_the_coefficient_limit_is_refused() {
        let mut v = base();
        v["inputs"][0]["channels"] = serde_json::json!(1024);
        v["outputs"][0]["channels"] = serde_json::json!(512);
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.contains("matrix coefficients"), "{e}");
    }

    /// A matrix whose every coefficient a batch could not set within a
    /// quarter of the block period is refused: at blockSize 48 one of
    /// 7,200 coefficients is taken, one of 8,192 refused, naming the
    /// blockSize of 55 that takes it, and taken at blockSize 64.
    #[test]
    fn a_matrix_a_batch_cannot_set_in_one_block_is_refused() {
        let mut v = base();
        v["blockSize"] = serde_json::json!(48);
        let shape = |v: &mut serde_json::Value, columns: usize| {
            v["inputs"][0]["channels"] = serde_json::json!(8);
            v["outputs"][0]["channels"] = serde_json::json!(columns);
            v["matrix"] = serde_json::json!(vec![vec![0.0; columns]; 8]);
        };
        shape(&mut v, 900);
        parse(&v).unwrap();
        shape(&mut v, 1024);
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.contains("at most 150 per sample of the block"), "{e}");
        assert!(e.contains("a blockSize of at least 55 takes them"), "{e}");
        v["blockSize"] = serde_json::json!(64);
        parse(&v).unwrap();
    }

    /// A coefficient of a duck column counts six times against the
    /// limit: 8 rows by 900 columns, 7,200 coefficients, are taken at
    /// blockSize 48 with no duck column and refused with one, which adds
    /// 40; a column out of range or named twice is refused.
    #[test]
    fn duck_columns_count_at_a_ducked_terms_cost() {
        let mut v = base();
        v["inputs"][0]["channels"] = serde_json::json!(8);
        v["outputs"][0]["channels"] = serde_json::json!(895);
        v["matrix"] = serde_json::json!(vec![vec![0.0; 895]; 8]);
        // 7,160 coefficients and 5 x 8 for a duck column: 7,200.
        v["duckColumns"] = serde_json::json!([3]);
        assert_eq!(parse(&v).unwrap().duck_columns, [3]);
        v["duckColumns"] = serde_json::json!([3, 4]);
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.contains("7240 matrix coefficients"), "{e}");
        v["duckColumns"] = serde_json::json!([895]);
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.contains("duckColumns names column 895"), "{e}");
        v["duckColumns"] = serde_json::json!([2, 2]);
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.contains("twice"), "{e}");
        assert!(parse(&base()).unwrap().duck_columns.is_empty());
    }

    #[test]
    fn the_hold_ring_rounds_up_to_whole_blocks() {
        let mut v = base();
        v["holdMs"] = serde_json::json!(1);
        v["contactWindowMs"] = serde_json::json!(1);
        v["blockSize"] = serde_json::json!(64);
        // 96 samples of D + w take two blocks of 64, plus 2B.
        assert_eq!(parse(&v).unwrap().hold_blocks(), 4);
        v["holdMs"] = serde_json::json!(0);
        assert!(parse(&v).is_err());
    }

    #[test]
    fn the_history_slack_holds_two_whole_blocks() {
        let mut v = base();
        v["blockSize"] = serde_json::json!(1024);
        // 50 ms is 2400 samples: two blocks of 1024.
        assert_eq!(parse(&v).unwrap().history_slack_blocks(), 2);
        v["historySlackMs"] = serde_json::json!(42);
        assert!(parse(&v).is_err(), "2016 samples, one block");
        v["blockSize"] = serde_json::json!(48);
        v["resyncBudgetMs"] = serde_json::json!(0);
        assert!(parse(&v).is_err());
    }

    #[test]
    fn rt_cpu_takes_names_and_indices() {
        for (v, want) in [
            (serde_json::json!("auto"), RtCpu::Auto),
            (serde_json::json!("none"), RtCpu::None),
            (serde_json::json!("first"), RtCpu::First),
            (serde_json::json!("last"), RtCpu::Last),
            (serde_json::json!(3), RtCpu::Index(3)),
        ] {
            let mut c = base();
            c["rt"] = serde_json::json!({ "rtCpu": v });
            assert_eq!(parse(&c).unwrap().rt.rt_cpu, want);
        }
        let mut c = base();
        c["rt"] = serde_json::json!({ "rtCpu": "middle" });
        assert!(parse(&c).is_err());
    }

    /// BS.1770-5 Table 3: surround channels 1.41, LFE left out, every
    /// channel 1 without a layout.
    #[test]
    fn loudness_channels_give_the_weights() {
        let mut c = base();
        c["outputs"][0]["loudnessChannels"] = serde_json::json!(["LFE"]);
        assert_eq!(
            parse(&c).unwrap().outputs[0].loudness_weights().unwrap(),
            [0.0]
        );
        let mut o = parse(&base()).unwrap().outputs.remove(0);
        assert_eq!(o.loudness_weights().unwrap(), [1.0]);
        o.channels = 6;
        o.loudness_channels = Some(
            ["L", "R", "C", "LFE", "Ls", "Rs"]
                .map(String::from)
                .to_vec(),
        );
        assert_eq!(
            o.loudness_weights().unwrap(),
            [1.0, 1.0, 1.0, 0.0, 1.41, 1.41]
        );
    }

    #[test]
    fn invalid_configurations_are_refused() {
        let cases: [(&str, serde_json::Value); 15] = [
            ("sampleRate", serde_json::json!(44_100)),
            ("blockSize", serde_json::json!(8)),
            ("blockSize", serde_json::json!(2048)),
            ("matrix", serde_json::json!([[0.5]])),
            ("matrix", serde_json::json!([[0.5, 1.0], [0.5, 1.0]])),
            ("matrix", serde_json::json!([[2.0e6], [0.5]])),
            ("rt", serde_json::json!({ "priority": 0 })),
            (
                "rampSamples",
                serde_json::json!(samplez_dsp::MAX_EXACT_RAMP_LEN + 1),
            ),
            (
                "outputs",
                serde_json::json!([{ "name": "out", "domain": "a", "flowId": IN, "label": "l",
                                     "group": "hop", "role": "Audio", "channels": 1 }]),
            ),
            ("unknownField", serde_json::json!(1)),
            (
                "inputs",
                serde_json::json!([{ "name": "in", "domain": "b", "flowId": IN, "channels": 2 }]),
            ),
            (
                "inputs",
                serde_json::json!([{ "name": "in", "domain": "a", "flowId": "x", "channels": 2 }]),
            ),
            (
                "outputs",
                serde_json::json!([{ "name": "out", "domain": "a", "flowId": OUT, "label": "l",
                                     "group": "a:b", "role": "Audio", "channels": 1 }]),
            ),
            (
                "outputs",
                serde_json::json!([{ "name": "out", "domain": "a", "flowId": OUT, "label": "l",
                                     "group": "hop", "role": "Audio", "channels": 1,
                                     "loudnessChannels": ["L", "R"] }]),
            ),
            (
                "outputs",
                serde_json::json!([{ "name": "out", "domain": "a", "flowId": OUT, "label": "l",
                                     "group": "hop", "role": "Audio", "channels": 1,
                                     "loudnessChannels": ["Lc"] }]),
            ),
        ];
        for (field, value) in cases {
            let mut c = base();
            c[field] = value.clone();
            assert!(parse(&c).is_err(), "{field} = {value}");
        }
    }

    #[test]
    fn limits_and_flows_are_checked() {
        let mut c = base();
        c["rampSamples"] = serde_json::json!(samplez_dsp::MAX_EXACT_RAMP_LEN);
        assert!(parse(&c).is_ok());
        // The same flow id in another domain is another flow.
        let mut c = base();
        c["domains"] = serde_json::json!([{ "name": "a", "path": "/mxl/a" },
                                          { "name": "b", "path": "/mxl/b" }]);
        c["outputs"][0]["flowId"] = serde_json::json!(IN);
        c["outputs"][0]["domain"] = serde_json::json!("b");
        assert!(parse(&c).is_ok());
        c["outputs"][0]["domain"] = serde_json::json!("a");
        assert!(parse(&c).is_err());
    }

    /// A meter adds its channels as matrix columns after the outputs';
    /// an input's and a meter's loudnessChannels are checked as an
    /// output's.
    #[test]
    fn meters_take_matrix_columns_after_the_outputs() {
        let mut c = base();
        c["meters"] = serde_json::json!([{ "name": "aux", "channels": 2,
                                           "loudnessChannels": ["L", "R"] }]);
        assert!(parse(&c).is_err(), "the matrix has no meter columns");
        c["matrix"] = serde_json::json!([[0.5, 1.0, 0.0], [0.5, 0.0, 1.0]]);
        let p = parse(&c).unwrap();
        assert_eq!(
            (p.output_channels(), p.meter_channels(), p.columns()),
            (1, 2, 3)
        );
        c["meters"][0]["loudnessChannels"] = serde_json::json!(["L"]);
        assert!(parse(&c).is_err());
        c["meters"][0]["loudnessChannels"] = serde_json::json!(["L", "R"]);
        c["meters"][0]["channels"] = serde_json::json!(0);
        c["matrix"] = serde_json::json!([[0.5], [0.5]]);
        assert!(parse(&c).is_err(), "a meter of no channels");
        let mut c = base();
        c["inputs"][0]["loudnessChannels"] = serde_json::json!(["L", "R"]);
        assert_eq!(
            parse(&c).unwrap().inputs[0].loudness_weights().unwrap(),
            [1.0, 1.0]
        );
        c["inputs"][0]["loudnessChannels"] = serde_json::json!(["L", "X"]);
        assert!(parse(&c).is_err());
    }

    /// A selection names one flow channel per leg, below
    /// MAX_FLOW_CHANNELS; without one the input reads the flow in order.
    #[test]
    fn flow_channels_name_one_flow_channel_per_leg() {
        let max = samplez_mxl::input::MAX_FLOW_CHANNELS;
        let mut c = base();
        assert_eq!(parse(&c).unwrap().inputs[0].flow_channels, None);
        for ok in [
            serde_json::json!([2, 3]),
            serde_json::json!([7, 7]),
            serde_json::json!([max - 1, 0]),
        ] {
            c["inputs"][0]["flowChannels"] = ok.clone();
            let p = parse(&c).unwrap();
            assert_eq!(serde_json::json!(p.inputs[0].flow_channels), ok, "{ok}");
        }
        for bad in [
            serde_json::json!([7]),
            serde_json::json!([0, 1, 2]),
            serde_json::json!([]),
            serde_json::json!([0, max]),
            serde_json::json!([0, -1]),
        ] {
            c["inputs"][0]["flowChannels"] = bad.clone();
            assert!(parse(&c).is_err(), "{bad}");
        }
        c["inputs"][0]["channels"] = serde_json::json!(1);
        c["inputs"][0]["flowChannels"] = serde_json::json!([7]);
        c["matrix"] = serde_json::json!([[0.5]]);
        assert!(parse(&c).is_ok());
    }

    /// A tone generator is an input with `tone: {}` and none of the keys
    /// of a flow: a domain, a flow, flow channels or an offset with it is
    /// refused, as an input with neither a domain nor a tone, and a
    /// generator reads no flow. Fails with the check of the flow keys
    /// dropped from `validate`.
    #[test]
    fn a_tone_input_takes_no_flow_keys() {
        let mut v = base();
        v["inputs"] = serde_json::json!([{ "name": "tone", "tone": {}, "channels": 2 }]);
        let c = parse(&v).unwrap();
        assert!(c.inputs[0].tone.is_some());
        assert_eq!(c.inputs[0].domain, None);
        let e = c.check_input_flow(0, IN).unwrap_err().to_string();
        assert!(e.contains("tone generator"), "{e}");
        assert!(c.check_flow_slot(0).is_err());
        for (key, value) in [
            ("domain", serde_json::json!("a")),
            ("flowId", serde_json::json!(IN)),
            ("flowChannels", serde_json::json!([0, 1])),
            ("offsetSamples", serde_json::json!(48)),
        ] {
            let mut bad = v.clone();
            bad["inputs"][0][key] = value;
            let e = parse(&bad).unwrap_err().to_string();
            assert!(e.contains("a tone generator takes no"), "{key}: {e}");
        }
        let mut bad = v.clone();
        bad["inputs"][0]["tone"] = serde_json::json!({ "frequencyHz": 1000 });
        assert!(parse(&bad).is_err(), "an unknown tone key taken");
        let mut bad = v.clone();
        bad["inputs"][0].as_object_mut().unwrap().remove("tone");
        let e = parse(&bad).unwrap_err().to_string();
        assert!(e.contains("no domain, and no tone"), "{e}");
        // A delay line takes its legs as any input's.
        let mut delayed = v.clone();
        delayed["inputs"][0]["maxDelaySamples"] = serde_json::json!(480);
        assert!(parse(&delayed).is_ok());
    }

    /// JSON has no NaN, so the gain is set on a parsed configuration.
    #[test]
    fn a_nan_matrix_gain_is_refused() {
        let mut c = parse(&base()).unwrap();
        c.matrix[0][0] = f32::NAN;
        assert!(c.validate().is_err());
        c.matrix[0][0] = f32::INFINITY;
        assert!(c.validate().is_err());
        c.matrix[0][0] = 0.5;
        assert!(c.validate().is_ok());
    }
}

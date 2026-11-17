//! The engine configuration, read from a JSON file.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// The most inputs and the most outputs an engine takes: per-block
/// verdicts carry one bit per input and per output.
pub const MAX_INPUTS: usize = 64;
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
    /// `samplez_dsp::MAX_RAMP_LEN`.
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
    #[serde(default)]
    pub rt: Rt,
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
    pub domain: String,
    pub flow_id: String,
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
}

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
    /// The loudness role of each channel: `L`, `R`, `C`, `Ls`, `Rs` or
    /// `LFE`, weighted per ITU-R BS.1770-5 Table 3 (the LFE channel is not
    /// measured). Without it every channel is weighted 1.
    #[serde(default)]
    pub loudness_channels: Option<Vec<String>>,
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
}

impl Default for Rt {
    fn default() -> Self {
        Rt {
            enabled: true,
            priority: default_priority(),
            rt_cpu: RtCpu::default(),
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
        if self.ramp_samples > samplez_dsp::MAX_RAMP_LEN {
            return fail(format!(
                "rampSamples {} is above {}",
                self.ramp_samples,
                samplez_dsp::MAX_RAMP_LEN
            ));
        }
        if !(1..=99).contains(&self.rt.priority) {
            return fail(format!("rt.priority {} is not in 1..=99", self.rt.priority));
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
            if self.domain(&i.domain).is_none() {
                return fail(format!("input {}: no domain {}", i.name, i.domain));
            }
            if i.channels == 0 {
                return fail(format!("input {}: channels must be at least 1", i.name));
            }
            samplez_mxl::input::legs(&self.input_config(i, &i.flow_id))
                .map_err(|e| Error(format!("input {}: {e}", i.name)))?;
            i.loudness_weights()?;
        }
        for m in &self.meters {
            if m.channels == 0 {
                return fail(format!("meter {}: channels must be at least 1", m.name));
            }
            m.loudness_weights()?;
        }
        for o in &self.outputs {
            if self.domain(&o.domain).is_none() {
                return fail(format!("output {}: no domain {}", o.name, o.domain));
            }
            samplez_mxl::output::flow_definition(&self.output_config(o))
                .map_err(|e| Error(format!("output {}: {e}", o.name)))?;
            o.loudness_weights()?;
        }
        for (slot, i) in self.inputs.iter().enumerate() {
            self.check_input_flow(slot, &i.flow_id)?;
        }
        let (n, m) = (self.input_channels(), self.columns());
        if self.matrix.len() != n || self.matrix.iter().any(|row| row.len() != m) {
            return fail(format!(
                "matrix must have {n} rows (input channels) of {m} gains (output and meter \
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

    /// The samplez-mxl creation parameters of `o`.
    /// Whether input `slot` may read flow `flow_id`: a valid id, and no
    /// output's flow in the input's domain.
    pub fn check_input_flow(&self, slot: usize, flow_id: &str) -> Result<(), Error> {
        let i = self
            .inputs
            .get(slot)
            .ok_or_else(|| Error(format!("no input slot {slot}")))?;
        samplez_mxl::validate_id(flow_id).map_err(|e| Error(format!("input {}: {e}", i.name)))?;
        if let Some(o) = self
            .outputs
            .iter()
            .find(|o| o.flow_id == flow_id && self.domain(&i.domain) == self.domain(&o.domain))
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
                serde_json::json!(samplez_dsp::MAX_RAMP_LEN + 1),
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
        c["rampSamples"] = serde_json::json!(samplez_dsp::MAX_RAMP_LEN);
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

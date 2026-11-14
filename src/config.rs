//! The engine configuration, read from a JSON file.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// The most inputs and the most outputs an engine takes: per-block
/// verdicts carry one bit per input and per output.
pub const MAX_INPUTS: usize = 64;
pub const MAX_OUTPUTS: usize = 64;

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
    /// Length of every gain ramp and input fade in samples.
    #[serde(default = "default_ramp")]
    pub ramp_samples: u32,
    /// Path of the libmxl shared library.
    pub libmxl: PathBuf,
    pub domains: Vec<Domain>,
    #[serde(default)]
    pub inputs: Vec<Input>,
    #[serde(default)]
    pub outputs: Vec<Output>,
    /// Gain from each input channel (row) to each output channel (column),
    /// channels numbered across inputs and outputs in configuration order.
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
    pub channels: u32,
    #[serde(default)]
    pub offset_samples: i64,
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
        unique(self.domains.iter().map(|d| d.name.as_str()), "domain")?;
        unique(self.inputs.iter().map(|i| i.name.as_str()), "input")?;
        unique(self.outputs.iter().map(|o| o.name.as_str()), "output")?;
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
            samplez_mxl::validate_id(&i.flow_id)
                .map_err(|e| Error(format!("input {}: {e}", i.name)))?;
        }
        for o in &self.outputs {
            if self.domain(&o.domain).is_none() {
                return fail(format!("output {}: no domain {}", o.name, o.domain));
            }
            samplez_mxl::output::flow_definition(&self.output_config(o))
                .map_err(|e| Error(format!("output {}: {e}", o.name)))?;
        }
        let (n, m) = (self.input_channels(), self.output_channels());
        if self.matrix.len() != n || self.matrix.iter().any(|row| row.len() != m) {
            return fail(format!(
                "matrix must have {n} rows (input channels) of {m} gains (output channels)"
            ));
        }
        if let Some(g) = self
            .matrix
            .iter()
            .flatten()
            .find(|g| g.abs() > samplez_dsp::MAX_GAIN)
        {
            return fail(format!("matrix gain {g} is not finite within MAX_GAIN"));
        }
        Ok(())
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

    #[test]
    fn invalid_configurations_are_refused() {
        let cases: [(&str, serde_json::Value); 11] = [
            ("sampleRate", serde_json::json!(44_100)),
            ("blockSize", serde_json::json!(8)),
            ("blockSize", serde_json::json!(2048)),
            ("matrix", serde_json::json!([[0.5]])),
            ("matrix", serde_json::json!([[0.5, 1.0], [0.5, 1.0]])),
            ("matrix", serde_json::json!([[2.0e6], [0.5]])),
            ("rt", serde_json::json!({ "priority": 0 })),
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
        ];
        for (field, value) in cases {
            let mut c = base();
            c[field] = value.clone();
            assert!(parse(&c).is_err(), "{field} = {value}");
        }
    }
}

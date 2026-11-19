//! samplez: a real-time multichannel audio engine on MXL flows.
//!
//! N input flows are read on libmxl's TAI timeline, mixed through a gain
//! matrix and written to M output flows at the indices they were read at.

#![cfg_attr(samplez_rtsan, feature(sanitize))]

#[cfg(not(target_arch = "x86_64"))]
compile_error!("samplez supports x86_64 only");

mod batch;
pub mod blocks;
pub mod config;
mod ducks;
pub mod engine;
mod meters;
pub mod rt;
mod rt_loop;
mod targets;
pub mod transfer;

pub use config::Config;
pub use ducks::{DuckMeter, DuckSetting, DuckTargets};
pub use engine::{
    ClockStatus, ControlError, DuckError, Engine, Entry, EntryError, Fade, GainError,
    GenerationStatus, InputState, InputStatus, MeterStatus, OutputStatus, Progress, Replication,
    Report, ResetError, Status,
};
pub use meters::{InputMeters, OutputMeters};
pub use rt_loop::{BlockVerdict, Counters, LogId, Verdict};
pub use samplez_dsp::MAX_COEFFICIENTS;
pub use targets::Targets;
pub use transfer::{
    CopyError, Core, CoreCopy, DecodeError, ENGINE_VERSION, ProductState, Replay, ReplayError,
};

/// The lint probe's lock on a static (CI): its users never name its type.
#[cfg(samplez_lint_probe = "samplez")]
pub static PROBE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

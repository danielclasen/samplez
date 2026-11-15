//! samplez: a real-time multichannel audio engine on MXL flows.
//!
//! N input flows are read on libmxl's TAI timeline, mixed through a gain
//! matrix and written to M output flows at the indices they were read at.

#![cfg_attr(samplez_rtsan, feature(sanitize))]

#[cfg(not(target_arch = "x86_64"))]
compile_error!("samplez supports x86_64 only");

pub mod blocks;
pub mod config;
pub mod engine;
mod meters;
pub mod rt;
mod rt_loop;

pub use config::Config;
pub use engine::{
    ControlError, Engine, Entry, EntryError, GainError, GenerationStatus, InputState, InputStatus,
    OutputStatus, Replication, Report, ResetError, Status,
};
pub use meters::OutputMeters;
pub use rt_loop::{BlockVerdict, Counters, LogId, Verdict};

/// The lint probe's lock on a static (CI): its users never name its type.
#[cfg(samplez_lint_probe = "samplez")]
pub static PROBE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

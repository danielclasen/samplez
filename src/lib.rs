//! samplez: a real-time multichannel audio engine on MXL flows.
//!
//! N input flows are read on libmxl's TAI timeline, mixed through a gain
//! matrix and written to M output flows at the indices they were read at.

#[cfg(not(target_arch = "x86_64"))]
compile_error!("samplez supports x86_64 only");

pub mod config;
pub mod engine;
pub mod rt;
mod rt_loop;

pub use config::Config;
pub use engine::{Engine, GainError, InputState, InputStatus, OutputStatus, Status};
pub use rt_loop::{BlockVerdict, Counters, Verdict};

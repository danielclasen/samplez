//! DSP core of samplez: planar buffers, gain ramps, the mixing matrix,
//! the input channel strip and delay, the output limiter, the duck detector,
//! meters, true peak and loudness, and history rings.
//!
//! Every `process` function works on state and scratch allocated at
//! construction and does not allocate.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

#[cfg(not(target_arch = "x86_64"))]
compile_error!("samplez-dsp supports x86_64 only");

mod buffer;
pub mod delay;
pub mod duck;
mod dynamics;
mod history;
mod kweight;
mod level;
pub mod limiter;
mod loudness;
mod matrix;
mod meter;
mod mxcsr;
mod ramp;
#[cfg(test)]
mod require;
mod simd;
pub mod strip;
mod svf;
#[cfg(test)]
mod test_util;
mod true_peak;

pub use buffer::{MAX_FRAMES, PlanarBuffer};
pub use delay::{Delay, DelayRings, MAX_DELAY_SAMPLES, Taps, copy_since};
pub use duck::{Detector, DetectorParams, DuckMode, DuckParam, KeyFilter, MAX_DUCKS, duck_gain};
pub use history::{History, Started};
pub use kweight::KWeighting;
pub use limiter::{LIMITER_DELAY, Limiter, LimiterParams};
pub use loudness::{Loudness, SLICE, Slice, WINDOW, lufs, weight};
pub use matrix::{MAX_COEFFICIENTS, Matrix, Path};
pub use meter::{Meters, accumulate};
pub use mxcsr::{DAZ, FTZ, SavedMxcsr, mxcsr, restore_mxcsr, set_ftz_daz};
pub use ramp::{MAX_EXACT_RAMP_LEN, MAX_GAIN, MAX_RAMP_LEN, Ramp, valid_gain};
pub use strip::{BandType, COMP_OFF, CompParams, EqBand, GATE_OFF, GateParams, Strip, StripParam};
pub use true_peak::TruePeak;

/// The lint probe (CI): built with `--cfg samplez_lint_probe="samplez-dsp"`,
/// clippy must refuse this lock and this sleep, and the lock on a static
/// whose type it does not name.
#[cfg(samplez_lint_probe = "samplez-dsp")]
pub fn lint_probe(m: &std::sync::Mutex<()>) {
    let _guard = m.lock();
    let _held = crate::PROBE_LOCK.lock();
    std::thread::sleep(std::time::Duration::from_millis(1));
}

/// The lint probe's lock on a static (CI), declared where the types may be
/// named: its users never name its type.
#[cfg(samplez_lint_probe = "samplez-dsp")]
#[allow(clippy::disallowed_types)]
pub static PROBE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

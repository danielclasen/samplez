//! DSP core of samplez: planar buffers, gain ramps, the mixing matrix,
//! meters, true peak and loudness.
//!
//! Every `process` function works on state and scratch allocated at
//! construction and does not allocate.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

#[cfg(not(target_arch = "x86_64"))]
compile_error!("samplez-dsp supports x86_64 only");

mod buffer;
mod kweight;
mod loudness;
mod matrix;
mod meter;
mod mxcsr;
mod ramp;
#[cfg(test)]
mod require;
mod simd;
#[cfg(test)]
mod test_util;
mod true_peak;

pub use buffer::{MAX_FRAMES, PlanarBuffer};
pub use kweight::KWeighting;
pub use loudness::{Loudness, SLICE, WINDOW, lufs, weight};
pub use matrix::{Matrix, Path};
pub use meter::Meters;
pub use mxcsr::{DAZ, FTZ, SavedMxcsr, mxcsr, restore_mxcsr, set_ftz_daz};
pub use ramp::{MAX_GAIN, MAX_RAMP_LEN, Ramp, valid_gain};
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

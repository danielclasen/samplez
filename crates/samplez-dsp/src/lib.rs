//! DSP core of samplez: planar buffers, gain ramps, the mixing matrix and
//! meters.
//!
//! Every `process` function works on state and scratch allocated at
//! construction and does not allocate.

#[cfg(not(target_arch = "x86_64"))]
compile_error!("samplez-dsp supports x86_64 only");

mod buffer;
mod matrix;
mod meter;
mod mxcsr;
mod ramp;
mod simd;
#[cfg(test)]
mod test_util;

pub use buffer::PlanarBuffer;
pub use matrix::{Matrix, Path};
pub use meter::Meters;
pub use mxcsr::{DAZ, FTZ, mxcsr, restore_mxcsr, set_ftz_daz};
pub use ramp::{MAX_RAMP_LEN, Ramp};

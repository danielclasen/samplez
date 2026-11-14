use crate::MAX_RAMP_LEN;

/// Largest buffer capacity in frames. Ramp positions within a block, up to
/// `MAX_RAMP_LEN + MAX_FRAMES`, stay within i32 for the SIMD kernels.
pub const MAX_FRAMES: usize = i32::MAX as usize - MAX_RAMP_LEN as usize;

/// Planar f32 audio: one contiguous run of `capacity` samples per channel.
#[derive(Clone, Debug)]
pub struct PlanarBuffer {
    data: Vec<f32>,
    channels: usize,
    capacity: usize,
}

impl PlanarBuffer {
    /// A zeroed buffer of `channels` channels holding up to `capacity`
    /// frames.
    ///
    /// # Panics
    ///
    /// If `capacity` exceeds [`MAX_FRAMES`], or `channels * capacity`
    /// overflows `usize`.
    pub fn new(channels: usize, capacity: usize) -> Self {
        assert!(
            capacity <= MAX_FRAMES,
            "capacity {capacity} exceeds MAX_FRAMES"
        );
        let len = channels
            .checked_mul(capacity)
            .unwrap_or_else(|| panic!("{channels} channels of {capacity} frames overflow usize"));
        Self {
            data: vec![0.0; len],
            channels,
            capacity,
        }
    }

    pub fn channels(&self) -> usize {
        self.channels
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// All `capacity` samples of channel `ch`.
    pub fn channel(&self, ch: usize) -> &[f32] {
        &self.data[ch * self.capacity..(ch + 1) * self.capacity]
    }

    /// All `capacity` samples of channel `ch`.
    pub fn channel_mut(&mut self, ch: usize) -> &mut [f32] {
        &mut self.data[ch * self.capacity..(ch + 1) * self.capacity]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "exceeds MAX_FRAMES")]
    fn capacity_is_capped() {
        PlanarBuffer::new(0, MAX_FRAMES + 1);
    }

    #[test]
    #[should_panic(expected = "overflow usize")]
    fn sample_count_overflow_is_named() {
        PlanarBuffer::new(usize::MAX, 2);
    }

    #[test]
    fn capacity_cap_keeps_ramp_positions_in_i32() {
        assert_eq!(MAX_FRAMES + MAX_RAMP_LEN as usize, i32::MAX as usize);
        assert_eq!(PlanarBuffer::new(0, MAX_FRAMES).capacity(), MAX_FRAMES);
    }
}

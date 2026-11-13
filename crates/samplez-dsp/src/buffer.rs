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
    pub fn new(channels: usize, capacity: usize) -> Self {
        Self {
            data: vec![0.0; channels * capacity],
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

use crate::PlanarBuffer;

/// Sample peak and sum of squares per channel, accumulated over blocks
/// until [`Meters::reset`].
#[derive(Clone, Debug)]
pub struct Meters {
    peak: Vec<f32>,
    sum_sq: Vec<f64>,
    frames: u64,
}

impl Meters {
    pub fn new(channels: usize) -> Self {
        Self {
            peak: vec![0.0; channels],
            sum_sq: vec![0.0; channels],
            frames: 0,
        }
    }

    /// Adds the first `frames` frames of `buf`.
    pub fn process(&mut self, buf: &PlanarBuffer, frames: usize) {
        assert_eq!(buf.channels(), self.peak.len());
        for (ch, (peak, sum_sq)) in self.peak.iter_mut().zip(&mut self.sum_sq).enumerate() {
            for &s in &buf.channel(ch)[..frames] {
                *peak = peak.max(s.abs());
                *sum_sq += f64::from(s) * f64::from(s);
            }
        }
        self.frames += frames as u64;
    }

    /// Largest absolute sample value of channel `ch`.
    pub fn peak(&self, ch: usize) -> f32 {
        self.peak[ch]
    }

    /// Sum of squares of channel `ch`, accumulated in f64.
    pub fn sum_sq(&self, ch: usize) -> f64 {
        self.sum_sq[ch]
    }

    /// Frames accumulated.
    pub fn frames(&self) -> u64 {
        self.frames
    }

    /// Root mean square of channel `ch`, 0 before any frame.
    pub fn rms(&self, ch: usize) -> f64 {
        if self.frames == 0 {
            0.0
        } else {
            (self.sum_sq[ch] / self.frames as f64).sqrt()
        }
    }

    pub fn reset(&mut self) {
        self.peak.fill(0.0);
        self.sum_sq.fill(0.0);
        self.frames = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::Rng;

    #[test]
    fn accumulates_over_blocks_like_a_reference() {
        let mut rng = Rng::new(19);
        let (channels, cap) = (5, 512);
        let mut buf = PlanarBuffer::new(channels, cap);
        let mut meters = Meters::new(channels);
        let mut all: Vec<Vec<f32>> = vec![Vec::new(); channels];
        for _ in 0..7 {
            let frames = 1 + rng.below(cap);
            for (ch, hist) in all.iter_mut().enumerate() {
                for s in &mut buf.channel_mut(ch)[..frames] {
                    *s = rng.sample();
                    hist.push(*s);
                }
            }
            meters.process(&buf, frames);
        }
        // A known peak on channel 2, negative so the absolute value counts.
        buf.channel_mut(2)[0] = -9.5;
        for (ch, hist) in all.iter_mut().enumerate() {
            hist.push(buf.channel(ch)[0]);
        }
        meters.process(&buf, 1);
        for (ch, hist) in all.iter().enumerate() {
            let peak = hist.iter().fold(0.0f32, |p, s| p.max(s.abs()));
            let sum_sq = hist
                .iter()
                .fold(0.0f64, |a, &s| a + f64::from(s) * f64::from(s));
            assert_eq!(meters.peak(ch).to_bits(), peak.to_bits(), "channel {ch}");
            assert_eq!(
                meters.sum_sq(ch).to_bits(),
                sum_sq.to_bits(),
                "channel {ch}"
            );
        }
        assert_eq!(meters.peak(2), 9.5);
        let frames = meters.frames();
        assert_eq!(frames as usize, all[2].len());
        let rms = (meters.sum_sq(0) / frames as f64).sqrt();
        assert_eq!(meters.rms(0), rms);

        meters.reset();
        assert_eq!(
            (meters.peak(0), meters.sum_sq(0), meters.rms(0)),
            (0.0, 0.0, 0.0)
        );
    }
}

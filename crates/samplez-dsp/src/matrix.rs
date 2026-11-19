use crate::duck::{DEPTH_DB, MAX_DUCKS};
use crate::dynamics::GAIN_INTERVAL;
use crate::level::{OCTAVES_PER_DB, exp2};
use crate::ramp::valid_gain;
use crate::{MAX_RAMP_LEN, PlanarBuffer, Ramp, simd};

/// Instruction set a [`Matrix`] runs on. Every path gives bit-identical
/// output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Path {
    Scalar,
    Avx2,
}

impl Path {
    pub const ALL: [Path; 2] = [Path::Scalar, Path::Avx2];

    /// Whether the running CPU supports this path.
    pub fn is_supported(self) -> bool {
        match self {
            Path::Scalar => true,
            Path::Avx2 => is_x86_feature_detected!("avx2"),
        }
    }

    /// The widest path the running CPU supports.
    pub fn best() -> Path {
        if Path::Avx2.is_supported() {
            Path::Avx2
        } else {
            Path::Scalar
        }
    }
}

// A duck mask holds one bit per detector.
const _: () = assert!(MAX_DUCKS == u8::BITS as usize);

/// `1 / GAIN_INTERVAL`.
const INV: f32 = 1.0 / GAIN_INTERVAL as f32;

/// One nonzero or ramping coefficient of an output.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Term {
    pub(crate) input: usize,
    pub(crate) ramp: Ramp,
    /// The coefficient's duck mask, as the matrix's table holds it.
    pub(crate) mask: u8,
    /// Whether the term has a [`Duck`] in its output's.
    pub(crate) ducked: bool,
}

/// The duck of a term. Its gain moves linearly over the interval running
/// from `from` to `to`, the gains computed at the ends of the last two
/// intervals; `live` is the gain of `live_mask`, the term's mask then, at
/// the last end, before any crossfade. After a mask change it crossfades
/// over `len` samples, at `pos`, from the live gain of `old`, the mask
/// `live` was taken for, to the live gain of the new one. `bias` is what the gain at the change lay
/// off the old mask's live gain, the remainder of a crossfade the change
/// interrupted, and fades out with the crossfade, so no change steps.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Duck {
    pub(crate) from: f32,
    pub(crate) to: f32,
    live: f32,
    live_mask: u8,
    old: u8,
    bias: f32,
    pos: u32,
    len: u32,
}

impl Duck {
    /// No duck, crossfading from mask 0 to the term's over `len` samples.
    fn new(len: u32) -> Duck {
        Duck {
            from: 1.0,
            to: 1.0,
            live: 1.0,
            live_mask: 0,
            old: 0,
            bias: 0.0,
            pos: 0,
            len: len.min(MAX_RAMP_LEN),
        }
    }

    /// Starts a crossfade over `len` samples to the term's new mask from
    /// the mask `live` was taken for, so that, the bias added, it starts
    /// from the gain the term moves to, however many changes came since
    /// the end of the last interval.
    fn change(&mut self, len: u32) {
        self.bias = self.to - self.live;
        self.old = self.live_mask;
        self.pos = 0;
        self.len = len.min(MAX_RAMP_LEN);
    }

    /// The gain change per sample over the interval running.
    #[inline]
    pub(crate) fn step(&self) -> f32 {
        (self.to - self.from) * INV
    }

    /// Back at no duck with no crossfade running: a term of mask 0 with
    /// this duck gives the bits of the term without one.
    fn is_rest(&self) -> bool {
        self.from == 1.0 && self.to == 1.0 && self.pos == self.len
    }

    /// Gains within 0 and 1, which every duck gain is, exactly 1 for mask
    /// 0, a bias within the distance of two of them, and a crossfade
    /// position within a length within [`MAX_RAMP_LEN`].
    fn is_valid(&self) -> bool {
        let unit = |g: f32| (0.0..=1.0).contains(&g);
        unit(self.from)
            && unit(self.to)
            && unit(self.live)
            && (self.live_mask != 0 || self.live == 1.0)
            && (-1.0..=1.0).contains(&self.bias)
            && self.len <= MAX_RAMP_LEN
            && self.pos <= self.len
    }

    /// Moves the crossfade `frames` samples on. The gain holds: it moves
    /// to the crossfade's value only at the next end of an interval, so a
    /// skip across the end of a crossfade moves the gain to the new
    /// mask's over the interval after it.
    fn advance(&mut self, frames: usize) {
        let frames = u32::try_from(frames).unwrap_or(u32::MAX);
        self.pos = self.pos.saturating_add(frames).min(self.len);
    }

    /// Into the next interval without a gain computed for it: held at
    /// the last one computed.
    fn wrap(&mut self) {
        self.from = self.to;
    }
}

/// What the ducked terms of one block read: the position of its first
/// sample in its interval, the depths, none standing for every depth at
/// 0 dB, and the detectors' envelopes, channel after channel `stride`
/// samples apart, none standing for every envelope at 0.
pub(crate) struct Ducking<'a> {
    pub(crate) phase: u32,
    depths: &'a [Ramp],
    envelopes: Option<(&'a [f32], usize)>,
    /// The number of the block, for the [`Memo`]s.
    epoch: u64,
}

/// The attenuations of one input at one sample of one block, its depth
/// times the envelope of each detector, and what every term of the input
/// at that sample reads in place of computing it again, whatever its
/// mask: the detectors of a positive attenuation from the deepest down,
/// the lower detector first of two equal, byte `k` of `prefix` the bits
/// of the first `k + 1` of them, and the duck gain of each of them.
/// Scratch, not state: it gives the bits computing gives.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Memo {
    epoch: u64,
    n: usize,
    atten: [f32; MAX_DUCKS],
    order: [u8; MAX_DUCKS],
    prefix: u64,
    gains: [f32; MAX_DUCKS],
}

impl Memo {
    /// Of no block: the first block is 1.
    const NONE: Memo = Memo {
        epoch: 0,
        n: 0,
        atten: [0.0; MAX_DUCKS],
        order: [0; MAX_DUCKS],
        prefix: 0,
        gains: [1.0; MAX_DUCKS],
    };

    /// Orders the detectors of a positive attenuation, deepest first, and
    /// computes their gains. A NaN fails the comparison and is never
    /// taken; an infinite one gives a gain of 0, so the gain is within 0
    /// and 1 whatever the envelope.
    fn order(&mut self) {
        let mut len = 0;
        for (g, &a) in self.atten.iter().enumerate() {
            if a > 0.0 {
                let mut k = len;
                while k > 0 && self.atten[self.order[k - 1] as usize] < a {
                    self.order[k] = self.order[k - 1];
                    k -= 1;
                }
                self.order[k] = g as u8;
                len += 1;
            }
        }
        let mut bits = 0u8;
        for (k, &g) in self.order[..len].iter().enumerate() {
            bits |= 1 << g;
            self.prefix |= u64::from(bits) << (8 * k);
            let g = g as usize;
            self.gains[g] = exp2(-self.atten[g] * OCTAVES_PER_DB);
        }
    }

    /// The duck gain of `mask`: that of the deepest attenuation of its
    /// detectors, `exp2(-atten * OCTAVES_PER_DB)`, exactly 1 at none.
    #[inline]
    fn gain(&self, mask: u8) -> f32 {
        // The bytes of the prefixes that meet the mask, from the first
        // that holds one of its detectors, the deepest, on.
        let met = self.prefix & (u64::from(mask) * 0x0101_0101_0101_0101);
        if met == 0 {
            return 1.0;
        }
        self.gains[self.order[(met.trailing_zeros() / 8) as usize] as usize]
    }
}

impl Ducking<'_> {
    /// The position of sample `n` of the block in its interval.
    #[inline]
    pub(crate) fn position(&self, n: usize) -> u32 {
        ((self.phase as usize + n) % GAIN_INTERVAL as usize) as u32
    }

    /// Moves `d`, the duck of a term from `input` with mask `mask`, into
    /// the next interval at sample `n` of the block, the last of the
    /// interval running. The gain the next interval ends at is the live
    /// gain of the mask, that of the deepest attenuation of its
    /// detectors, each its depth times its envelope at sample `n`; while
    /// a crossfade runs, at sample `n` of it, the crossfade from the live
    /// gain of the old mask plus the bias to it. `memo` holds one
    /// [`Memo`] per input.
    #[inline]
    pub(crate) fn retarget(
        &self,
        memo: &mut [Memo],
        d: &mut Duck,
        input: usize,
        mask: u8,
        n: usize,
    ) {
        let m = &mut memo[input];
        if m.epoch != self.epoch || m.n != n {
            self.attenuations(m, input, n);
        }
        let live = m.gain(mask);
        let k = d.pos.saturating_add(n as u32);
        let to = if k < d.len {
            let old = (m.gain(d.old) + d.bias).clamp(0.0, 1.0);
            old + (k as f32 / d.len as f32) * (live - old)
        } else {
            live
        };
        d.live = live;
        d.live_mask = mask;
        d.from = d.to;
        d.to = to;
    }

    /// Fills `m` with the attenuations of `input` at sample `n`.
    fn attenuations(&self, m: &mut Memo, input: usize, n: usize) {
        *m = Memo {
            epoch: self.epoch,
            n,
            ..Memo::NONE
        };
        let (Some((env, stride)), false) = (self.envelopes, self.depths.is_empty()) else {
            return;
        };
        let depths = &self.depths[input * MAX_DUCKS..(input + 1) * MAX_DUCKS];
        let env = &env[n..];
        for (g, (a, r)) in m.atten.iter_mut().zip(depths).enumerate() {
            *a = r.value_at(r.pos.saturating_add(n as u32)) * env[g * stride];
        }
        m.order();
    }
}

/// Mixing matrix of N inputs and M outputs:
/// `out[j][n] = sum_i M[i][j] * in[i][n]`.
///
/// Each output sums its nonzero coefficients in ascending input order,
/// starting from +0.0, with a multiply and an add per term and no FMA. A
/// coefficient change ramps per [`Ramp`]. An output with no coefficient is
/// filled with +0.0; an output whose only coefficient is a settled 1.0 is a
/// copy of its input.
///
/// Skipping zero coefficients and copying routes gives the same bits as
/// the dense sum over all coefficients only for finite inputs: a NaN or
/// infinity on an input with coefficient 0 does not reach the output, and
/// a route passes -0.0, NaN payloads and, under DAZ, denormals unchanged
/// where the sum would not.
///
/// Ducks: each coefficient has a duck mask, bit `g` for detector `g` of
/// up to [`MAX_DUCKS`], kept when the coefficient goes to 0, and each
/// input a depth per detector within [`DEPTH_DB`], ramping as
/// coefficients do. A coefficient of mask 0 is computed as above, so a
/// matrix with no mask set gives the bits it gave before ducks existed.
/// Every other one is ducked: at the end of each interval of
/// [`GAIN_INTERVAL`] samples of the sample index, the largest depth times
/// envelope over the detectors of its mask, at the last sample of the
/// interval, gives its gain ([`duck_gain`](crate::duck_gain)) at the end
/// of the next interval, and the gain moves linearly over it; the sample
/// gain is the coefficient times the duck gain. A mask change crossfades
/// over the given length from the old mask's gain to the new one's, both
/// following their envelopes, so no change steps.
/// [`Matrix::process_ducked`] reads the envelopes; [`Matrix::process`]
/// takes every envelope at 0 and is for a matrix without ducks. Without
/// any duck, mask or depth set, the position in the interval is not part
/// of the matrix's state, its serde form or its equality: a user that
/// sets them [`align`](Matrix::align)s the matrix to the sample index.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(into = "MatrixWire", try_from = "MatrixWire")]
pub struct Matrix {
    inputs: usize,
    /// Per output, the nonzero or ramping coefficients sorted by input.
    columns: Vec<Vec<Term>>,
    /// Per output, the duck of each of its terms that has one, in the
    /// order of the terms.
    ducks: Vec<Vec<Duck>>,
    /// The duck mask of every coefficient, `input * outputs + output`, or
    /// none before a mask is set: a matrix without ducks copies nothing
    /// of them.
    masks: Vec<u8>,
    /// The depth of every input per detector, dB, `input * MAX_DUCKS +
    /// detector`, or none before a depth is set.
    depths: Vec<Ramp>,
    /// The position of the next sample in its interval.
    phase: u32,
    path: Path,
    /// The number of the last block mixed and a [`Memo`] per input:
    /// scratch, not compared, copied or serialised.
    epoch: u64,
    memo: Vec<Memo>,
}

impl PartialEq for Matrix {
    /// Equal coefficients, ramps, ducks, masks, depths and path, and,
    /// with any duck, mask or depth set, the position in the interval:
    /// without, it moves nothing. An empty mask or depth table equals one
    /// all at 0.
    fn eq(&self, o: &Matrix) -> bool {
        let rest = Ramp::new(0.0);
        let masks = self.masks == o.masks
            || (self.masks.iter().all(|&k| k == 0) && o.masks.iter().all(|&k| k == 0));
        let depths = self.depths == o.depths
            || (self.depths.iter().all(|r| *r == rest) && o.depths.iter().all(|r| *r == rest));
        self.inputs == o.inputs
            && self.columns == o.columns
            && self.ducks == o.ducks
            && masks
            && depths
            && (self.phase == o.phase || !self.has_ducks())
            && self.path == o.path
    }
}

/// The most coefficients a matrix holds, input channels times output
/// channels: an engine configuration beyond it is refused, and so is a
/// serialised matrix, before anything is allocated for it.
pub const MAX_COEFFICIENTS: usize = 1 << 18;

fn is_default<T: Default + PartialEq>(v: &T) -> bool {
    *v == T::default()
}

/// A term as [`Matrix`]'s serde form carries it: a mask of 0 and no duck
/// leave out their fields, so a matrix with no duck set has the form it
/// had before ducks existed.
#[derive(serde::Serialize, serde::Deserialize)]
struct WireTerm {
    input: usize,
    ramp: Ramp,
    #[serde(default, skip_serializing_if = "is_default")]
    mask: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    duck: Option<Duck>,
}

/// A matrix as [`Matrix`]'s serde form carries it: its coefficients,
/// ramps and ducks, and, once set, the masks other than 0 and the depths
/// other than a held 0 dB by index. Every path gives the same output, so
/// the receiver runs on its own [`Path::best`].
#[derive(serde::Serialize, serde::Deserialize)]
struct MatrixWire {
    inputs: usize,
    columns: Vec<Vec<WireTerm>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    masks: Option<Vec<(usize, u8)>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    depths: Option<Vec<(usize, Ramp)>>,
    #[serde(default, skip_serializing_if = "is_default")]
    phase: u32,
}

impl From<Matrix> for MatrixWire {
    fn from(m: Matrix) -> MatrixWire {
        let rest = Ramp::new(0.0);
        let masks: Vec<(usize, u8)> = (m.masks.iter().enumerate())
            .filter(|&(_, &k)| k != 0)
            .map(|(c, &k)| (c, k))
            .collect();
        let depths: Vec<(usize, Ramp)> = (m.depths.iter().enumerate())
            .filter(|&(_, r)| *r != rest)
            .map(|(c, &r)| (c, r))
            .collect();
        let phase = if m.has_ducks() { m.phase } else { 0 };
        let columns = m
            .columns
            .into_iter()
            .zip(m.ducks)
            .map(|(col, ducks)| {
                let mut ducks = ducks.into_iter();
                col.into_iter()
                    .map(|t| WireTerm {
                        input: t.input,
                        ramp: t.ramp,
                        mask: t.mask,
                        duck: if t.ducked { ducks.next() } else { None },
                    })
                    .collect()
            })
            .collect();
        MatrixWire {
            inputs: m.inputs,
            columns,
            masks: (!masks.is_empty()).then_some(masks),
            depths: (!depths.is_empty()).then_some(depths),
            phase,
        }
    }
}

/// Whether `indices` ascend strictly, each below `end`.
fn ascending_below(mut indices: impl Iterator<Item = usize>, end: usize) -> bool {
    let mut next = 0;
    indices.all(|c| {
        let ok = c >= next && c < end;
        next = c + 1;
        ok
    })
}

impl TryFrom<MatrixWire> for Matrix {
    type Error = String;

    /// Refuses a matrix of more than [`MAX_COEFFICIENTS`] coefficients or
    /// inputs, before allocating it, and a column with an input out of range,
    /// out of order or twice, or a ramp a matrix never holds
    /// ([`crate::Ramp::is_valid`]): a gain beyond [`crate::MAX_GAIN`] or
    /// not finite, a length beyond [`crate::MAX_RAMP_LEN`], a position
    /// past its length. Refuses as well what no matrix holds of ducks: a
    /// mask or depth out of range, out of order or twice, a mask of 0
    /// listed, a depth outside [`DEPTH_DB`], a term whose mask is not the
    /// table's or that has a mask and no duck, a duck gain outside 0 and
    /// 1, a crossfade past its length or longer than
    /// [`crate::MAX_RAMP_LEN`], and a position in the interval past it.
    fn try_from(w: MatrixWire) -> Result<Matrix, String> {
        let (inputs, outputs) = (w.inputs, w.columns.len());
        let size = inputs.checked_mul(outputs);
        // A matrix allocates per input as well as per coefficient.
        if !(inputs <= MAX_COEFFICIENTS
            && size.is_some_and(|n| n <= MAX_COEFFICIENTS)
            && inputs.checked_mul(MAX_DUCKS).is_some())
        {
            return Err(format!(
                "a matrix of {inputs} inputs and {outputs} outputs, at most \
                 {MAX_COEFFICIENTS} coefficients and inputs"
            ));
        }
        let (masks, depths) = (w.masks.as_deref(), w.depths.as_deref());
        let masks_ok = masks.is_none_or(|v| {
            ascending_below(v.iter().map(|m| m.0), inputs * outputs) && v.iter().all(|m| m.1 != 0)
        });
        let depths_ok = depths.is_none_or(|v| {
            ascending_below(v.iter().map(|d| d.0), inputs * MAX_DUCKS)
                && v.iter().all(|d| d.1.is_within(DEPTH_DB))
        });
        if !(masks_ok && depths_ok && w.phase < GAIN_INTERVAL) {
            return Err("matrix ducks out of range, order or finite values".to_owned());
        }
        let mut m = Matrix::new(inputs, outputs);
        if let Some(v) = w.masks {
            m.masks.resize(inputs * outputs, 0);
            for (c, k) in v {
                m.masks[c] = k;
            }
        }
        if let Some(v) = w.depths {
            m.depths.resize(inputs * MAX_DUCKS, Ramp::new(0.0));
            for (c, r) in v {
                m.depths[c] = r;
            }
        }
        m.phase = w.phase;
        let masks = &m.masks;
        let cols = m.columns.iter_mut().zip(&mut m.ducks);
        for (j, ((dst, ducks), col)) in cols.zip(w.columns).enumerate() {
            let ordered = col.windows(2).all(|t| t[0].input < t[1].input);
            let inside = col.iter().all(|t| t.input < inputs);
            let finite = col.iter().all(|t| t.ramp.is_valid());
            if !(ordered && inside && finite) {
                return Err("a matrix column out of range, order or finite values".to_owned());
            }
            for t in col {
                let mask = masks.get(t.input * outputs + j).copied().unwrap_or(0);
                let duck_ok = match t.duck {
                    Some(d) => d.is_valid(),
                    None => mask == 0,
                };
                if t.mask != mask || !duck_ok {
                    return Err("a matrix term with a mask or duck no matrix holds".to_owned());
                }
                dst.push(Term {
                    input: t.input,
                    ramp: t.ramp,
                    mask,
                    ducked: t.duck.is_some(),
                });
                ducks.extend(t.duck);
            }
        }
        Ok(m)
    }
}

impl Clone for Matrix {
    /// A copy whose columns reserve room for every input, as in
    /// [`Matrix::new`], so [`Matrix::set`] on it does not allocate.
    fn clone(&self) -> Self {
        fn reserved<T: Clone>(v: &[T], room: usize) -> Vec<T> {
            let mut copy = Vec::with_capacity(room);
            copy.extend_from_slice(v);
            copy
        }
        Self {
            inputs: self.inputs,
            columns: self
                .columns
                .iter()
                .map(|c| reserved(c, self.inputs))
                .collect(),
            ducks: self
                .ducks
                .iter()
                .map(|d| reserved(d, self.inputs))
                .collect(),
            masks: reserved(&self.masks, self.inputs * self.columns.len()),
            depths: reserved(&self.depths, self.inputs * MAX_DUCKS),
            phase: self.phase,
            path: self.path,
            epoch: 0,
            memo: vec![Memo::NONE; self.inputs],
        }
    }
}

/// The index in its output's ducks of the duck of term `k` of `col`, or
/// of the one it would have.
fn duck_index(col: &[Term], k: usize) -> usize {
    col[..k].iter().filter(|t| t.ducked).count()
}

impl Matrix {
    /// An all-zero matrix with every mask 0 and every depth 0 dB, running
    /// on [`Path::best`]. It reserves room for every coefficient, mask and
    /// depth, so the methods of a matrix do not allocate.
    pub fn new(inputs: usize, outputs: usize) -> Self {
        let size = inputs
            .checked_mul(outputs)
            .expect("inputs times outputs fits usize");
        Self {
            inputs,
            columns: (0..outputs).map(|_| Vec::with_capacity(inputs)).collect(),
            ducks: (0..outputs).map(|_| Vec::with_capacity(inputs)).collect(),
            masks: Vec::with_capacity(size),
            depths: Vec::with_capacity(inputs * MAX_DUCKS),
            phase: 0,
            path: Path::best(),
            epoch: 0,
            memo: vec![Memo::NONE; inputs],
        }
    }

    pub fn inputs(&self) -> usize {
        self.inputs
    }

    pub fn outputs(&self) -> usize {
        self.columns.len()
    }

    /// The path [`Matrix::process`] runs on.
    pub fn path(&self) -> Path {
        self.path
    }

    /// Whether any mask or depth is set or any term has a duck, a
    /// crossfade to mask 0 included.
    pub fn has_ducks(&self) -> bool {
        let rest = Ramp::new(0.0);
        self.masks.iter().any(|&k| k != 0)
            || self.depths.iter().any(|r| *r != rest)
            || self.ducks.iter().any(|d| !d.is_empty())
    }

    /// Whether any term of `output` has a duck: a mask other than 0, or
    /// a crossfade from one. False out of range.
    pub fn is_ducked(&self, output: usize) -> bool {
        self.ducks.get(output).is_some_and(|d| !d.is_empty())
    }

    /// Makes this matrix a copy of `src`, coefficients, ramps and ducks,
    /// without allocating: both have the same shape, and every column
    /// reserves room for every input, as [`Matrix::new`] and clones do.
    pub fn copy_from(&mut self, src: &Matrix) {
        assert!(
            self.inputs == src.inputs && self.columns.len() == src.columns.len(),
            "a copy between matrices of different shapes"
        );
        for (dst, col) in self.columns.iter_mut().zip(&src.columns) {
            dst.clear();
            dst.extend_from_slice(col);
        }
        for (dst, ducks) in self.ducks.iter_mut().zip(&src.ducks) {
            if !(dst.is_empty() && ducks.is_empty()) {
                dst.clear();
                dst.extend_from_slice(ducks);
            }
        }
        self.masks.clear();
        self.masks.extend_from_slice(&src.masks);
        self.depths.clear();
        self.depths.extend_from_slice(&src.depths);
        self.phase = src.phase;
        self.path = src.path;
    }

    /// Value the coefficient from `input` to `output` has on the next
    /// sample.
    pub fn gain(&self, input: usize, output: usize) -> f32 {
        let col = &self.columns[output];
        col.binary_search_by_key(&input, |t| t.input)
            .map_or(0.0, |k| col[k].ramp.current())
    }

    /// Whether the coefficient from `input` to `output` is ramping: its
    /// next sample differs from its target. False for a coefficient that
    /// holds its value, a removed one included.
    pub fn is_ramping(&self, input: usize, output: usize) -> bool {
        let Some(col) = self.columns.get(output) else {
            return false;
        };
        col.binary_search_by_key(&input, |t| t.input)
            .is_ok_and(|k| col[k].ramp.is_active())
    }

    /// Ramps the coefficient from `input` to `output` to `target` over
    /// `len` samples, per [`Ramp::set_target`]. A coefficient that comes
    /// back from 0 with a mask set is ducked from the start, with no
    /// crossfade: its ramp from silence hides the step.
    ///
    /// Returns false and changes nothing if `input` or `output` is out of
    /// range, or `target` is not finite or exceeds
    /// [`MAX_GAIN`](crate::MAX_GAIN) in magnitude.
    #[must_use]
    pub fn set(&mut self, input: usize, output: usize, target: f32, len: u32) -> bool {
        self.set_fade(input, output, target, 0, len)
    }

    /// [`Matrix::set`] after the coefficient held its value for `delay`
    /// samples, per [`Ramp::set_fade`]. Its value at a sample is a
    /// function of the sample's distance from the call and of the values
    /// the call was given alone, whichever blocks the samples fall in. A
    /// masked term the call inserts is ducked from the start, with no
    /// crossfade: the fade ramps it up from silence.
    #[must_use]
    pub fn set_fade(
        &mut self,
        input: usize,
        output: usize,
        target: f32,
        delay: u32,
        len: u32,
    ) -> bool {
        if input >= self.inputs || output >= self.columns.len() || !valid_gain(target) {
            return false;
        }
        let col = &mut self.columns[output];
        match col.binary_search_by_key(&input, |t| t.input) {
            Ok(k) => {
                // `target` passed valid_gain, so set_target accepts it.
                let _ = col[k].ramp.set_fade(target, delay, len);
                if !col[k].ramp.is_active() && target == 0.0 {
                    if col[k].ducked {
                        self.ducks[output].remove(duck_index(col, k));
                    }
                    col.remove(k);
                }
            }
            Err(k) if target != 0.0 => {
                let mut ramp = Ramp::new(0.0);
                let _ = ramp.set_fade(target, delay, len);
                let term = Term {
                    input,
                    ramp,
                    mask: 0,
                    ducked: false,
                };
                col.insert(k, term);
                if !self.masks.is_empty() {
                    self.duck_inserted(output, k);
                }
            }
            Err(_) => {}
        }
        true
    }

    /// Sets the coefficient from `input` to `output` to the ramp `ramp`
    /// as it is, whatever the coefficient held: its value at a sample is
    /// then the ramp's alone ([`Ramp::resume`]). A `None`, or a ramp that
    /// is over, jumps to `target`. Returns false and changes nothing for
    /// an input or output out of range, or a target that is not a valid
    /// gain.
    #[must_use]
    pub fn set_resumed(
        &mut self,
        input: usize,
        output: usize,
        target: f32,
        ramp: Option<Ramp>,
    ) -> bool {
        let Some(ramp) = ramp.filter(Ramp::is_active) else {
            return self.set(input, output, target, 0);
        };
        if input >= self.inputs || output >= self.columns.len() || !valid_gain(target) {
            return false;
        }
        let col = &mut self.columns[output];
        match col.binary_search_by_key(&input, |t| t.input) {
            Ok(k) => col[k].ramp = ramp,
            Err(k) => {
                col.insert(
                    k,
                    Term {
                        input,
                        ramp,
                        mask: 0,
                        ducked: false,
                    },
                );
                if !self.masks.is_empty() {
                    // The fade brings the term in from silence, so no
                    // duck crossfade is needed, and none is the state an
                    // install of the same fade builds.
                    self.duck_inserted(output, k);
                }
            }
        }
        true
    }

    /// Gives term `k` of `output`, just inserted, its mask, and a duck
    /// crossfading from no duck over `len` samples when it is not 0.
    fn duck_inserted(&mut self, output: usize, k: usize) {
        let col = &mut self.columns[output];
        let mask = self.masks[col[k].input * self.ducks.len() + output];
        if mask != 0 {
            col[k].mask = mask;
            col[k].ducked = true;
            self.ducks[output].insert(duck_index(col, k), Duck::new(0));
        }
    }

    /// The duck mask of the coefficient from `input` to `output`, 0 out
    /// of range.
    pub fn mask(&self, input: usize, output: usize) -> u8 {
        if input >= self.inputs || output >= self.columns.len() {
            return 0;
        }
        let c = input * self.columns.len() + output;
        self.masks.get(c).copied().unwrap_or(0)
    }

    /// Sets the duck mask of the coefficient from `input` to `output`:
    /// bit `g` ducks it by detector `g`. A coefficient that is not 0
    /// crossfades over `len` samples, clamped to [`MAX_RAMP_LEN`], from
    /// the live gain of the mask it had to that of the new one, both
    /// moving with their envelopes; a change during a crossfade starts
    /// from where that one has got to. The mask it has already leaves it
    /// unchanged.
    ///
    /// Returns false and changes nothing if `input` or `output` is out of
    /// range.
    #[must_use]
    pub fn set_mask(&mut self, input: usize, output: usize, mask: u8, len: u32) -> bool {
        let outputs = self.columns.len();
        if input >= self.inputs || output >= outputs {
            return false;
        }
        if self.masks.is_empty() {
            if mask == 0 {
                return true;
            }
            self.masks.resize(self.inputs * outputs, 0);
        }
        let held = &mut self.masks[input * outputs + output];
        if *held == mask {
            return true;
        }
        *held = mask;
        let (col, ducks) = (&mut self.columns[output], &mut self.ducks[output]);
        if let Ok(k) = col.binary_search_by_key(&input, |t| t.input) {
            let i = duck_index(col, k);
            let t = &mut col[k];
            t.mask = mask;
            if t.ducked {
                ducks[i].change(len);
            } else {
                t.ducked = true;
                ducks.insert(i, Duck::new(len));
            }
        }
        true
    }

    /// The depth of `input` by detector `duck` on the next sample, dB, 0
    /// out of range.
    pub fn depth(&self, input: usize, duck: usize) -> f32 {
        if input >= self.inputs || duck >= MAX_DUCKS {
            return 0.0;
        }
        let c = input * MAX_DUCKS + duck;
        self.depths.get(c).map_or(0.0, Ramp::current)
    }

    /// Ramps the depth of `input` by detector `duck` to `db` over `len`
    /// samples, per [`Ramp::set_target`].
    ///
    /// Returns false and changes nothing if `input` or `duck` is out of
    /// range, or `db` is not within [`DEPTH_DB`].
    #[must_use]
    pub fn set_depth(&mut self, input: usize, duck: usize, db: f32, len: u32) -> bool {
        if input >= self.inputs || duck >= MAX_DUCKS || !DEPTH_DB.contains(&db) {
            return false;
        }
        if self.depths.is_empty() {
            if db == 0.0 {
                return true;
            }
            self.depths.resize(self.inputs * MAX_DUCKS, Ramp::new(0.0));
        }
        self.depths[input * MAX_DUCKS + duck].set_target(db, len)
    }

    /// Takes the next sample processed to be sample index `index`: ducks
    /// move on at the multiples of [`GAIN_INTERVAL`] of the index. A
    /// position in the interval other than the current one holds the
    /// duck gains at the last ones computed, as [`Matrix::advance`]
    /// across the end of an interval does. A matrix starts at index 0 and
    /// counts the samples it processes and advances over.
    pub fn align(&mut self, index: u64) {
        let phase = (index % u64::from(GAIN_INTERVAL)) as u32;
        if phase != self.phase {
            self.ducks.iter_mut().flatten().for_each(Duck::wrap);
            self.phase = phase;
        }
    }

    /// Starts the duck of every ducked term at the gain a running matrix
    /// holds for it when the detectors sit at `envelopes`, one per
    /// detector, steady: `from`, `to` and the live gain all that gain, as
    /// they are after a few intervals at those envelopes, instead of at 1
    /// moving to it over the two intervals after a fill. A term of mask 0
    /// or with no depth set keeps gain 1.
    pub fn settle_ducks(&mut self, envelopes: &[f32]) {
        for (col, ducks) in self.columns.iter().zip(&mut self.ducks) {
            for (k, t) in col.iter().filter(|t| t.ducked).enumerate() {
                let mut memo = Memo::NONE;
                if !self.depths.is_empty() {
                    let depths = &self.depths[t.input * MAX_DUCKS..(t.input + 1) * MAX_DUCKS];
                    for (g, (a, r)) in memo.atten.iter_mut().zip(depths).enumerate() {
                        *a = r.current() * envelopes.get(g).copied().unwrap_or(0.0);
                    }
                    memo.order();
                }
                let gain = memo.gain(t.mask);
                let d = &mut ducks[k];
                d.from = gain;
                d.to = gain;
                d.live = gain;
                d.live_mask = t.mask;
            }
        }
    }

    /// Mixes the first `frames` frames of `input` into `output` on
    /// [`Matrix::path`], with every envelope at 0. A matrix whose terms
    /// have ducks is processed with [`Matrix::process_ducked`]: here,
    /// every duck releases within one interval.
    pub fn process(&mut self, input: &PlanarBuffer, output: &mut PlanarBuffer, frames: usize) {
        self.process_on(self.path, input, output, frames);
    }

    /// Mixes the first `frames` frames of `input` into `output` on `path`,
    /// with every envelope at 0, as [`Matrix::process`] does.
    ///
    /// # Panics
    ///
    /// If the CPU does not support `path`, or the buffer shapes do not
    /// match the matrix.
    pub fn process_on(
        &mut self,
        path: Path,
        input: &PlanarBuffer,
        output: &mut PlanarBuffer,
        frames: usize,
    ) {
        debug_assert!(
            self.ducks.iter().all(Vec::is_empty),
            "a matrix with ducks processed without envelopes"
        );
        self.mix(path, input, None, output, frames);
    }

    /// Mixes the first `frames` frames of `input` into `output` on
    /// [`Matrix::path`], ducked by `envelopes`: channel `g`, of
    /// [`MAX_DUCKS`], holds the envelope of detector `g` of each sample,
    /// as [`Detector::process`](crate::Detector::process) writes it.
    pub fn process_ducked(
        &mut self,
        input: &PlanarBuffer,
        envelopes: &PlanarBuffer,
        output: &mut PlanarBuffer,
        frames: usize,
    ) {
        self.mix(self.path, input, Some(envelopes), output, frames);
    }

    /// [`Matrix::process_ducked`] on `path`.
    ///
    /// # Panics
    ///
    /// If the CPU does not support `path`, or the buffer shapes do not
    /// match the matrix and [`MAX_DUCKS`].
    pub fn process_ducked_on(
        &mut self,
        path: Path,
        input: &PlanarBuffer,
        envelopes: &PlanarBuffer,
        output: &mut PlanarBuffer,
        frames: usize,
    ) {
        self.mix(path, input, Some(envelopes), output, frames);
    }

    fn mix(
        &mut self,
        path: Path,
        input: &PlanarBuffer,
        envelopes: Option<&PlanarBuffer>,
        output: &mut PlanarBuffer,
        frames: usize,
    ) {
        assert!(path.is_supported(), "{path:?} is not supported by this CPU");
        assert_eq!(input.channels(), self.inputs);
        assert_eq!(output.channels(), self.columns.len());
        assert!(frames <= input.capacity() && frames <= output.capacity());
        if let Some(e) = envelopes {
            assert!(e.channels() == MAX_DUCKS && frames <= e.capacity());
        }
        self.epoch += 1;
        let ducking = Ducking {
            phase: self.phase,
            depths: &self.depths,
            envelopes: envelopes.map(|e| (e.data(), e.capacity())),
            epoch: self.epoch,
        };
        let mut ducked = false;
        let cols = self.columns.iter_mut().zip(&mut self.ducks);
        for (j, (col, ducks)) in cols.enumerate() {
            if !ducks.is_empty() {
                ducked = true;
                continue;
            }
            let out = &mut output.channel_mut(j)[..frames];
            match col.as_slice() {
                [] => out.fill(0.0),
                [t] if !t.ramp.is_active() && t.ramp.target == 1.0 => {
                    out.copy_from_slice(&input.channel(t.input)[..frames]);
                }
                terms => match path {
                    Path::Scalar => mix_scalar(terms, input, out, 0),
                    // SAFETY: the CPU supports AVX2, asserted above.
                    Path::Avx2 => unsafe { simd::mix_avx2(terms, input, out) },
                },
            }
            advance_column(col, ducks, frames);
        }
        if ducked {
            let (columns, ducks) = (&self.columns, &mut self.ducks);
            mix_ducked(
                path,
                columns,
                ducks,
                &mut self.memo,
                &ducking,
                input,
                output,
                frames,
            );
            for (col, ducks) in self.columns.iter_mut().zip(&mut self.ducks) {
                if !ducks.is_empty() {
                    advance_column(col, ducks, frames);
                }
            }
        }
        self.advance_ducks(frames);
    }

    /// Replaces every coefficient with a ramp from silence to its value in
    /// `gains`, row-major (`input * outputs + output`), over `len`
    /// samples; an invalid gain counts as silence. Keeps each column's
    /// room and fills it in input order, so the work is one pass over
    /// `gains` whatever the matrix held. A coefficient with a mask set
    /// crossfades from no duck to its mask's gain over the same length.
    /// Whether any coefficient ramps.
    ///
    /// # Panics
    ///
    /// When `gains` does not hold one value per coefficient.
    pub fn fade_in(&mut self, gains: &[f32], len: u32) -> bool {
        self.fade_in_resuming(gains, len, &[])
    }

    /// [`Matrix::fade_in`] where each `(coefficient, ramp)` of `resumed`,
    /// ascending and unique by coefficient, takes `ramp` as it is, a
    /// coefficient at 0 included, instead of a ramp from silence: the
    /// coefficient continues a fade the matrix did not run itself.
    /// Whether any coefficient ramps.
    ///
    /// # Panics
    ///
    /// When `gains` does not hold one value per coefficient.
    pub fn fade_in_resuming(&mut self, gains: &[f32], len: u32, resumed: &[(usize, Ramp)]) -> bool {
        let outputs = self.columns.len();
        assert_eq!(
            gains.len(),
            self.inputs * outputs,
            "one gain per coefficient"
        );
        for (col, ducks) in self.columns.iter_mut().zip(&mut self.ducks) {
            col.clear();
            ducks.clear();
        }
        let mut ramping = false;
        let mut next = resumed.iter().peekable();
        for (c, &g) in gains.iter().enumerate() {
            let ramp = if resumed.is_empty() || next.peek().is_none_or(|e| e.0 != c) {
                if g == 0.0 || !valid_gain(g) {
                    continue;
                }
                let mut ramp = Ramp::new(0.0);
                let _ = ramp.set_target(g, len);
                ramp
            } else {
                next.next().expect("peeked").1
            };
            ramping |= ramp.is_active();
            self.columns[c % outputs].push(Term {
                input: c / outputs,
                ramp,
                mask: 0,
                ducked: false,
            });
        }
        if !self.masks.is_empty() {
            let cols = self.columns.iter_mut().zip(&mut self.ducks);
            for (j, (col, ducks)) in cols.enumerate() {
                for t in col {
                    t.mask = self.masks[t.input * outputs + j];
                    if t.mask != 0 {
                        t.ducked = true;
                        ducks.push(Duck::new(len));
                    }
                }
            }
        }
        ramping
    }

    /// [`Matrix::fade_in`] with every duck mask and depth replaced at
    /// once, with no crossfade or ramp: `masks` holds one mask per
    /// coefficient, row-major, or is empty for every mask 0, `depths` one
    /// depth per input and detector, `input * MAX_DUCKS + detector`, or is
    /// empty for every depth 0 dB; a depth outside [`DEPTH_DB`] counts as
    /// 0 dB. A coefficient with a mask starts undocked and moves to its
    /// mask's gain over the two intervals after the fill, as a mask set
    /// with no crossfade does. Masks and depths all 0 cost a scan of the
    /// two lists. Whether any coefficient ramps.
    ///
    /// # Panics
    ///
    /// When `gains`, or `masks` or `depths` when not empty, do not hold
    /// one value per coefficient, or per input and detector.
    pub fn fade_in_ducked(
        &mut self,
        gains: &[f32],
        masks: &[u8],
        depths: &[f32],
        len: u32,
    ) -> bool {
        self.fade_in_ducked_resuming(gains, masks, depths, len, &[])
    }

    /// [`Matrix::fade_in_ducked`] with the ramps of
    /// [`Matrix::fade_in_resuming`].
    pub fn fade_in_ducked_resuming(
        &mut self,
        gains: &[f32],
        masks: &[u8],
        depths: &[f32],
        len: u32,
        resumed: &[(usize, Ramp)],
    ) -> bool {
        self.masks.clear();
        self.depths.clear();
        let ramping = self.fade_in_resuming(gains, len, resumed);
        if masks.iter().any(|&k| k != 0) {
            assert_eq!(masks.len(), gains.len(), "one mask per coefficient");
            // Reserved at construction: nothing allocates.
            self.masks.extend_from_slice(masks);
            let outputs = self.columns.len();
            let cols = self.columns.iter_mut().zip(&mut self.ducks);
            for (j, (col, ducks)) in cols.enumerate() {
                for t in col {
                    t.mask = masks[t.input * outputs + j];
                    if t.mask != 0 {
                        t.ducked = true;
                        ducks.push(Duck::new(0));
                    }
                }
            }
        }
        if depths.iter().any(|&d| d != 0.0) {
            assert_eq!(
                depths.len(),
                self.inputs * MAX_DUCKS,
                "one depth per input and detector"
            );
            let depth = |&d: &f32| Ramp::new(if DEPTH_DB.contains(&d) { d } else { 0.0 });
            self.depths.extend(depths.iter().map(depth));
        }
        ramping
    }

    /// Moves every coefficient ramp, depth ramp and mask crossfade
    /// `frames` samples on without mixing, as [`Matrix::process`] over
    /// `frames` frames does. Keeps ramp positions on the sample timeline
    /// over frames that are not mixed. Across the end of an interval the
    /// duck gains hold at the last ones computed, a crossfade's included:
    /// one the skip ended moves the gain from the held one to the new
    /// mask's over the next interval.
    pub fn advance(&mut self, frames: usize) {
        let wraps = self.phase as usize + frames >= GAIN_INTERVAL as usize;
        for (col, ducks) in self.columns.iter_mut().zip(&mut self.ducks) {
            if wraps {
                ducks.iter_mut().for_each(Duck::wrap);
            }
            advance_column(col, ducks, frames);
        }
        self.advance_ducks(frames);
    }

    /// Moves the depth ramps and the position in the interval `frames`
    /// samples on.
    fn advance_ducks(&mut self, frames: usize) {
        for r in &mut self.depths {
            r.advance(frames);
        }
        let len = GAIN_INTERVAL as usize;
        self.phase = ((self.phase as usize + frames % len) % len) as u32;
    }
}

/// Moves the ramps and crossfades of one output `frames` samples on, and
/// drops the coefficients ramped to 0 and the ducks of mask 0 back at
/// rest.
fn advance_column(col: &mut Vec<Term>, ducks: &mut Vec<Duck>, frames: usize) {
    if ducks.is_empty() {
        for t in col.iter_mut() {
            t.ramp.advance(frames);
        }
        col.retain(|t| t.ramp.is_active() || t.ramp.target != 0.0);
        return;
    }
    let (mut read, mut write) = (0, 0);
    col.retain_mut(|t| {
        t.ramp.advance(frames);
        let kept = t.ramp.is_active() || t.ramp.target != 0.0;
        if t.ducked {
            let mut d = ducks[read];
            read += 1;
            d.advance(frames);
            if kept && (t.mask != 0 || !d.is_rest()) {
                ducks[write] = d;
                write += 1;
            } else {
                t.ducked = false;
            }
        }
        kept
    });
    ducks.truncate(write);
}

/// Scalar reference for one output, from sample `start` to the end of
/// `out`. The SIMD paths compute the same operations in the same order.
/// Out of line: inlined into the mix, it runs 8 % more instructions.
#[inline(never)]
pub(crate) fn mix_scalar(terms: &[Term], input: &PlanarBuffer, out: &mut [f32], start: usize) {
    for (n, o) in out.iter_mut().enumerate().skip(start) {
        let mut acc = 0.0f32;
        for t in terms {
            let g = t.ramp.value_at(t.ramp.pos.saturating_add(n as u32));
            acc += g * input.channel(t.input)[n];
        }
        *o = acc;
    }
}

/// Mixes the outputs with ducks on `path`, `ducks` the duck of each term
/// `ducked` in order per output: the samples up to the first interval
/// starting in the block, then each interval over all of them, so that
/// the terms of one input in several outputs move into the next interval
/// at the same sample one after another and read the attenuations the memo keeps.
#[allow(clippy::too_many_arguments)]
fn mix_ducked(
    path: Path,
    columns: &[Vec<Term>],
    ducks: &mut [Vec<Duck>],
    memo: &mut [Memo],
    ducking: &Ducking,
    input: &PlanarBuffer,
    output: &mut PlanarBuffer,
    frames: usize,
) {
    let len = GAIN_INTERVAL as usize;
    let (mut n, mut run) = (0, ((len - ducking.phase as usize) % len).min(frames));
    while n < frames {
        for (j, (terms, d)) in columns.iter().zip(ducks.iter_mut()).enumerate() {
            if d.is_empty() || run == 0 {
                continue;
            }
            let out = &mut output.channel_mut(j)[..n + run];
            if run == len && path == Path::Avx2 {
                // SAFETY: the caller asserted that the CPU supports AVX2.
                unsafe { simd::ducked_interval_avx2(terms, d, memo, ducking, input, out, n) };
            } else {
                mix_ducked_scalar(terms, d, memo, ducking, input, out, n);
            }
        }
        n += run;
        run = (frames - n).min(len);
    }
}

/// Scalar reference for one output with ducked terms, `ducks` the duck of
/// each term `ducked` in order, from sample `start` to the end of `out`:
/// as [`mix_scalar`], the gain of a ducked term the coefficient times its
/// duck gain, and after the last sample of an interval each duck moved
/// into the next. The SIMD paths compute the same operations in the same
/// order.
pub(crate) fn mix_ducked_scalar(
    terms: &[Term],
    ducks: &mut [Duck],
    memo: &mut [Memo],
    ducking: &Ducking,
    input: &PlanarBuffer,
    out: &mut [f32],
    start: usize,
) {
    for (n, o) in out.iter_mut().enumerate().skip(start) {
        let p = ducking.position(n);
        let k = (p + 1) as f32;
        let mut acc = 0.0f32;
        let mut i = 0;
        for t in terms {
            let mut g = t.ramp.value_at(t.ramp.pos.saturating_add(n as u32));
            if t.ducked {
                let d = &ducks[i];
                i += 1;
                g *= d.from + k * d.step();
            }
            acc += g * input.channel(t.input)[n];
        }
        *o = acc;
        if p == GAIN_INTERVAL - 1 {
            let ducked = terms.iter().filter(|t| t.ducked);
            for (t, d) in ducked.zip(ducks.iter_mut()) {
                ducking.retarget(memo, d, t.input, t.mask, n);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    /// A fade-in replaces a populated, ramping matrix whole: coefficients
    /// it leaves at silence are gone, the others ramp from silence.
    #[test]
    fn a_fade_in_replaces_every_coefficient_from_silence() {
        let mut m = Matrix::new(3, 2);
        for i in 0..3 {
            for o in 0..2 {
                assert!(m.set(i, o, 0.5, 480));
            }
        }
        let gains = [0.25, 0.0, 0.0, 1.0, 0.5, f32::NAN];
        assert!(m.fade_in(&gains, 100));
        let mut expect = Ramp::new(0.0);
        assert!(expect.set_target(0.25, 100));
        assert_eq!(m.gain(0, 0).to_bits(), expect.current().to_bits());
        for (i, o) in [(0, 1), (1, 0), (2, 1)] {
            assert_eq!(m.gain(i, o), 0.0);
            assert!(!m.is_ramping(i, o));
        }
        m.advance(40);
        assert!(m.is_ramping(1, 1) && m.is_ramping(2, 0));
        m.advance(60);
        assert_eq!((m.gain(0, 0), m.gain(1, 1), m.gain(2, 0)), (0.25, 1.0, 0.5));
        assert!(!m.fade_in(&[0.0; 6], 100));
        assert_eq!(m.gain(1, 1), 0.0);
    }

    /// A ducked fill replaces whatever a matrix held, masks, depths and
    /// crossfades included, with what a fresh matrix holds after a
    /// fade-in and every nonzero mask and depth set with no crossfade or
    /// ramp; all-zero masks and depths leave it without ducks.
    #[test]
    fn a_ducked_fill_replaces_every_mask_and_depth() {
        let gains = [0.25, 0.0, 0.5, 1.0, 0.5, 0.75];
        let masks = [1, 4, 0, 0, 0b11, 0];
        let mut depths = [0.0; 3 * MAX_DUCKS];
        depths[MAX_DUCKS + 2] = 12.0;
        depths[2 * MAX_DUCKS] = 41.0;
        let mut used = Matrix::new(3, 2);
        for i in 0..3 {
            for o in 0..2 {
                assert!(used.set(i, o, 0.5, 480));
                assert!(used.set_mask(i, o, 0b1000_0000, 480));
            }
            assert!(used.set_depth(i, 7, 30.0, 480));
        }
        used.advance(117);
        used.align(240);
        assert!(used.fade_in_ducked(&gains, &masks, &depths, 100));
        let mut fresh = Matrix::new(3, 2);
        fresh.align(240);
        assert!(fresh.fade_in(&gains, 100));
        for (c, &k) in masks.iter().enumerate().filter(|(_, k)| **k != 0) {
            assert!(fresh.set_mask(c / 2, c % 2, k, 0));
        }
        assert!(fresh.set_depth(1, 2, 12.0, 0));
        assert!(used == fresh, "{used:?}\n{fresh:?}");
        assert!(used.fade_in_ducked(&gains, &[0; 6], &[0.0; 3 * MAX_DUCKS], 100));
        assert!(!used.has_ducks());
        assert!(used.fade_in_ducked(&gains, &[], &[], 100));
        assert!(!used.has_ducks());
    }

    use super::*;
    use crate::require::path_runs;
    use crate::test_util::Rng;
    use crate::{restore_mxcsr, set_ftz_daz};

    const CAP: usize = 1024;

    fn fill(rng: &mut Rng, buf: &mut PlanarBuffer, frames: usize) {
        for ch in 0..buf.channels() {
            for s in &mut buf.channel_mut(ch)[..frames] {
                *s = rng.sample();
            }
        }
    }

    /// Sets a random subset of coefficients, with ramps of random length
    /// including none and lengths that end inside a block.
    fn change(rng: &mut Rng, m: &mut Matrix, count: usize) {
        for _ in 0..count {
            let i = rng.below(m.inputs());
            let j = rng.below(m.outputs());
            let len = match rng.below(4) {
                0 => 0,
                _ => rng.below(2000) as u32,
            };
            let c = rng.coefficient();
            assert!(m.set(i, j, c, len));
        }
    }

    fn assert_bits_eq(a: &PlanarBuffer, b: &PlanarBuffer, frames: usize, ctx: &str) {
        for ch in 0..a.channels() {
            let (x, y) = (&a.channel(ch)[..frames], &b.channel(ch)[..frames]);
            for n in 0..frames {
                assert_eq!(
                    x[n].to_bits(),
                    y[n].to_bits(),
                    "{ctx}: output {ch} sample {n}: {} vs {}",
                    x[n],
                    y[n]
                );
            }
        }
    }

    /// Runs random matrices, coefficient changes and blocks on `path` and
    /// on the scalar reference, with FTZ/DAZ off and on, and compares every
    /// output sample bit for bit.
    fn bit_exact_vs_scalar(path: Path, seed: u64) {
        if !path_runs("bit_exact_vs_scalar", path) {
            return;
        }
        for ftz in [false, true] {
            let saved = set_ftz_daz(ftz);
            let mut rng = Rng::new(seed);
            for trial in 0..24 {
                let (n, m) = if trial == 0 {
                    (64, 64)
                } else {
                    (1 + rng.below(64), 1 + rng.below(64))
                };
                let mut reference = Matrix::new(n, m);
                change(&mut rng, &mut reference, n * m);
                let mut under_test = reference.clone();
                let mut input = PlanarBuffer::new(n, CAP);
                let mut want = PlanarBuffer::new(m, CAP);
                let mut got = PlanarBuffer::new(m, CAP);
                for block in 0..4 {
                    let frames = if trial == 0 && block == 0 {
                        CAP
                    } else {
                        16 + rng.below(CAP - 15)
                    };
                    fill(&mut rng, &mut input, frames);
                    reference.process_on(Path::Scalar, &input, &mut want, frames);
                    under_test.process_on(path, &input, &mut got, frames);
                    let ctx = format!("ftz {ftz} trial {trial} {n}x{m} block {block}/{frames}");
                    assert_bits_eq(&want, &got, frames, &ctx);
                    assert_eq!(reference, under_test, "{ctx}: state");
                    let k = rng.below(n * m / 4 + 2);
                    change(&mut rng, &mut reference, k);
                    under_test.columns.clone_from(&reference.columns);
                }
            }
            restore_mxcsr(saved);
        }
    }

    #[test]
    fn advance_moves_ramps_as_process_does() {
        let mut rng = Rng::new(7);
        let mut a = Matrix::new(3, 2);
        assert!(a.set(0, 0, 0.8, 1000));
        assert!(a.set(2, 1, -0.5, 300));
        assert!(a.set(1, 1, 0.25, 0));
        let mut b = a.clone();
        let mut input = PlanarBuffer::new(3, CAP);
        let (mut out_a, mut out_b) = (PlanarBuffer::new(2, CAP), PlanarBuffer::new(2, CAP));
        fill(&mut rng, &mut input, CAP);
        a.process(&input, &mut out_a, 400);
        b.advance(400);
        assert_eq!(a, b);
        a.process(&input, &mut out_a, CAP);
        b.process(&input, &mut out_b, CAP);
        assert_bits_eq(&out_a, &out_b, CAP, "after advance");
    }

    /// A copy taken mid-ramp continues bit-identically, a coefficient the
    /// target had and the source has not included.
    #[test]
    fn a_copy_continues_the_ramps_bit_identically() {
        let mut rng = Rng::new(11);
        let mut src = Matrix::new(3, 2);
        assert!(src.set(0, 0, 0.8, 1000));
        assert!(src.set(2, 1, -0.5, 300));
        src.advance(123);
        let mut dst = Matrix::new(3, 2);
        assert!(dst.set(1, 0, 0.3, 0));
        dst.copy_from(&src);
        assert_eq!(dst, src);
        let mut input = PlanarBuffer::new(3, CAP);
        let (mut out_a, mut out_b) = (PlanarBuffer::new(2, CAP), PlanarBuffer::new(2, CAP));
        fill(&mut rng, &mut input, CAP);
        src.process(&input, &mut out_a, CAP);
        dst.process(&input, &mut out_b, CAP);
        assert_bits_eq(&out_a, &out_b, CAP, "after the copy");
    }

    #[test]
    fn is_ramping_only_while_a_ramp_runs() {
        let mut m = Matrix::new(2, 2);
        assert!(!m.is_ramping(0, 0), "absent coefficient");
        assert!(m.set(0, 0, 0.5, 10));
        assert!(m.is_ramping(0, 0));
        assert!(!m.is_ramping(1, 0) && !m.is_ramping(0, 1));
        m.advance(10);
        assert!(!m.is_ramping(0, 0), "ramp ended");
        // A target equal to the held value starts no ramp.
        assert!(m.set(0, 0, 0.5, 10));
        assert!(!m.is_ramping(0, 0));
        assert!(m.set(1, 1, 0.0, 10), "zero on an absent coefficient");
        assert!(!m.is_ramping(1, 1));
        assert!(m.set(0, 0, 0.0, 0), "jump");
        assert!(!m.is_ramping(0, 0));
        assert!(!m.is_ramping(5, 5), "out of range");
    }

    #[test]
    fn bit_exact_avx2() {
        bit_exact_vs_scalar(Path::Avx2, 0xa2);
    }

    /// Random coefficient changes, ramps and blocks on `matrix`, the
    /// output bits of every block folded into one value.
    fn fingerprint(path: Path, matrix: &mut Matrix, rng: &mut Rng) -> u64 {
        let (n, m) = (matrix.inputs(), matrix.outputs());
        let mut input = PlanarBuffer::new(n, CAP);
        let mut out = PlanarBuffer::new(m, CAP);
        let mut fold = 0u64;
        for _ in 0..12 {
            change(rng, matrix, n * m / 3);
            let frames = 1 + rng.below(CAP);
            fill(rng, &mut input, frames);
            matrix.process_on(path, &input, &mut out, frames);
            for ch in 0..m {
                for s in &out.channel(ch)[..frames] {
                    fold = fold.rotate_left(5) ^ u64::from(s.to_bits());
                }
            }
        }
        fold
    }

    /// The output of a matrix with no duck set carries the bits pinned
    /// here, computed by the matrix before ducks existed, on every path.
    #[test]
    fn a_matrix_without_ducks_keeps_its_bits() {
        for path in Path::ALL {
            if !path_runs("a_matrix_without_ducks_keeps_its_bits", path) {
                continue;
            }
            let mut rng = Rng::new(0x5eed);
            let mut m = Matrix::new(13, 9);
            let fold = fingerprint(path, &mut m, &mut rng);
            assert_eq!(fold, UNDUCKED, "{path:?}: {fold:#x}");
        }
    }

    const UNDUCKED: u64 = 0xb2ae_7ead_edfa_9a5b;

    /// The scalar reference equals the dense definition
    /// `sum_i M[i][j] * in[i][n]` over every coefficient, zeros included,
    /// for finite input without -0.0.
    #[test]
    fn scalar_equals_dense_sum() {
        let mut rng = Rng::new(7);
        for _ in 0..16 {
            let (n, m) = (1 + rng.below(64), 1 + rng.below(64));
            let frames = 16 + rng.below(CAP - 15);
            let mut coef = vec![0.0f32; n * m];
            let mut mat = Matrix::new(n, m);
            for i in 0..n {
                for j in 0..m {
                    coef[i * m + j] = rng.coefficient();
                    assert!(mat.set(i, j, coef[i * m + j], 0));
                }
            }
            let mut input = PlanarBuffer::new(n, CAP);
            let mut out = PlanarBuffer::new(m, CAP);
            fill(&mut rng, &mut input, frames);
            mat.process_on(Path::Scalar, &input, &mut out, frames);
            for j in 0..m {
                for s in 0..frames {
                    let mut acc = 0.0f32;
                    for i in 0..n {
                        acc += coef[i * m + j] * input.channel(i)[s];
                    }
                    assert_eq!(out.channel(j)[s].to_bits(), acc.to_bits());
                }
            }
        }
    }

    /// A coefficient change ramps each coefficient per `c0 + n * step`,
    /// retargets from the current value, and sums in input order.
    #[test]
    fn coefficient_ramp_follows_formula() {
        let ramp = |c0: f32, target: f32, len: u32, k: u32| {
            let step = (target - c0) / len as f32;
            if k < len {
                c0 + k as f32 * step
            } else {
                target
            }
        };
        for path in Path::ALL {
            if !path_runs("coefficient_ramp_follows_formula", path) {
                continue;
            }
            let mut rng = Rng::new(11);
            let mut mat = Matrix::new(3, 2);
            assert!(mat.set(0, 0, 0.5, 0));
            assert!(mat.set(1, 0, -0.25, 0));
            assert!(mat.set(2, 1, 1.0, 0));
            let mut input = PlanarBuffer::new(3, CAP);
            let mut out = PlanarBuffer::new(2, CAP);

            // Block 1: output 0 ramps input 0 from 0.5 to 0.9 over 300 and
            // input 2 in from 0 to 0.3 over 77; output 1 ramps its route
            // down to 0.125 over 200.
            assert!(mat.set(0, 0, 0.9, 300));
            assert!(mat.set(2, 0, 0.3, 77));
            assert!(mat.set(2, 1, 0.125, 200));
            let f1 = 131;
            fill(&mut rng, &mut input, f1);
            mat.process_on(path, &input, &mut out, f1);
            for n in 0..f1 {
                let k = n as u32;
                let x = |i: usize| input.channel(i)[n];
                let mut acc = 0.0f32;
                acc += ramp(0.5, 0.9, 300, k) * x(0);
                acc += -0.25 * x(1);
                acc += ramp(0.0, 0.3, 77, k) * x(2);
                assert_eq!(out.channel(0)[n].to_bits(), acc.to_bits(), "{path:?} n {n}");
                let want = 0.0 + ramp(1.0, 0.125, 200, k) * x(2);
                assert_eq!(
                    out.channel(1)[n].to_bits(),
                    want.to_bits(),
                    "{path:?} n {n}"
                );
            }

            // Block 2: retarget input 0 on output 0 mid-ramp.
            let c_retarget = ramp(0.5, 0.9, 300, f1 as u32);
            assert_eq!(mat.gain(0, 0).to_bits(), c_retarget.to_bits());
            assert!(mat.set(0, 0, 0.0, 100));
            let f2 = 203;
            fill(&mut rng, &mut input, f2);
            mat.process_on(path, &input, &mut out, f2);
            for n in 0..f2 {
                let k = (f1 + n) as u32;
                let x = |i: usize| input.channel(i)[n];
                let mut acc = 0.0f32;
                acc += ramp(c_retarget, 0.0, 100, n as u32) * x(0);
                acc += -0.25 * x(1);
                acc += ramp(0.0, 0.3, 77, k) * x(2);
                assert_eq!(out.channel(0)[n].to_bits(), acc.to_bits(), "{path:?} n {n}");
                let want = 0.0 + ramp(1.0, 0.125, 200, k) * x(2);
                assert_eq!(
                    out.channel(1)[n].to_bits(),
                    want.to_bits(),
                    "{path:?} n {n}"
                );
            }
            // The ramp to zero has ended, so the coefficient is gone.
            assert_eq!(mat.columns[0].len(), 2);
            assert_eq!(mat.gain(0, 0).to_bits(), 0);
            assert_eq!(mat.gain(2, 1).to_bits(), 0.125f32.to_bits());
        }
    }

    /// Bit patterns a multiply by 1.0 does not preserve under FTZ/DAZ or
    /// that an add to +0.0 changes.
    fn special(rng: &mut Rng) -> f32 {
        match rng.below(6) {
            0 => -0.0,
            1 => f32::from_bits(0x7fa0_1234),
            2 => f32::from_bits(0x8000_0001 | (rng.next_u64() as u32 & 0x007f_ffff)),
            3 => f32::NEG_INFINITY,
            _ => rng.sample(),
        }
    }

    #[test]
    fn route_is_bit_transparent() {
        for path in Path::ALL {
            if !path_runs("route_is_bit_transparent", path) {
                continue;
            }
            let saved = set_ftz_daz(true);
            let mut rng = Rng::new(13);
            let (n, m, frames) = (8, 10, 509);
            let mut input = PlanarBuffer::new(n, CAP);
            for ch in 0..n {
                for s in &mut input.channel_mut(ch)[..frames] {
                    *s = special(&mut rng);
                }
            }
            // A shuffle of all inputs, one input selected twice, and one
            // output that becomes a route once its crossfade from input 0
            // to input 5 has ended.
            let mut mat = Matrix::new(n, m);
            let src = [3, 0, 7, 1, 6, 2, 5, 4];
            for (j, &i) in src.iter().enumerate() {
                assert!(mat.set(i, j, 1.0, 0));
            }
            assert!(mat.set(3, 8, 1.0, 0));
            assert!(mat.set(0, 9, 1.0, 0));
            let mut out = PlanarBuffer::new(m, CAP);
            mat.process_on(path, &input, &mut out, frames);
            assert!(mat.set(0, 9, 0.0, 64));
            assert!(mat.set(5, 9, 1.0, 64));
            let mut fade = PlanarBuffer::new(m, CAP);
            mat.process_on(path, &input, &mut fade, 64);
            mat.process_on(path, &input, &mut out, frames);
            restore_mxcsr(saved);

            let routed = src.iter().enumerate().map(|(j, &i)| (j, i));
            for (j, i) in routed.chain([(8, 3), (9, 5)]) {
                for s in 0..frames {
                    assert_eq!(
                        out.channel(j)[s].to_bits(),
                        input.channel(i)[s].to_bits(),
                        "{path:?} output {j} sample {s}"
                    );
                }
            }
        }
    }

    #[test]
    fn silence_gives_exact_zeros() {
        for path in Path::ALL {
            if !path_runs("silence_gives_exact_zeros", path) {
                continue;
            }
            for ftz in [false, true] {
                let saved = set_ftz_daz(ftz);
                let mut rng = Rng::new(17);
                let (n, m) = (24, 20);
                let mut mat = Matrix::new(n, m);
                change(&mut rng, &mut mat, n * m);
                let input = PlanarBuffer::new(n, CAP);
                let mut out = PlanarBuffer::new(m, CAP);
                for _ in 0..6 {
                    for ch in 0..m {
                        out.channel_mut(ch).fill(f32::NAN);
                    }
                    let frames = 16 + rng.below(CAP - 15);
                    mat.process_on(path, &input, &mut out, frames);
                    for ch in 0..m {
                        for &s in &out.channel(ch)[..frames] {
                            assert_eq!(s.to_bits(), 0, "{path:?} ftz {ftz} output {ch}");
                        }
                    }
                    change(&mut rng, &mut mat, 40);
                }
                restore_mxcsr(saved);
            }
        }
    }

    #[test]
    fn best_path_is_supported() {
        assert!(Path::best().is_supported());
        assert_eq!(Matrix::new(1, 1).path(), Path::best());
    }

    /// A settled 1.0 is a copy, but a ramp towards 1.0 is not: the ramp is
    /// computed until it ends, and the block after it copies.
    #[test]
    fn ramp_to_unity_is_not_copied_until_it_ends() {
        for path in Path::ALL {
            if !path_runs("ramp_to_unity_is_not_copied_until_it_ends", path) {
                continue;
            }
            let saved = set_ftz_daz(true);
            let mut rng = Rng::new(23);
            let mut mat = Matrix::new(1, 1);
            let mut input = PlanarBuffer::new(1, CAP);
            let mut out = PlanarBuffer::new(1, CAP);
            assert!(mat.set(0, 0, 0.5, 0));
            fill(&mut rng, &mut input, 64);
            mat.process_on(path, &input, &mut out, 64);

            assert!(mat.set(0, 0, 1.0, 100));
            let frames = 150;
            fill(&mut rng, &mut input, frames);
            mat.process_on(path, &input, &mut out, frames);
            let step = (1.0f32 - 0.5) / 100.0;
            for n in 0..frames {
                let g = if n < 100 { 0.5 + n as f32 * step } else { 1.0 };
                let want = 0.0 + g * input.channel(0)[n];
                assert_eq!(
                    out.channel(0)[n].to_bits(),
                    want.to_bits(),
                    "{path:?} n {n}"
                );
            }

            let frames = 97;
            for s in &mut input.channel_mut(0)[..frames] {
                *s = special(&mut rng);
            }
            mat.process_on(path, &input, &mut out, frames);
            restore_mxcsr(saved);
            for n in 0..frames {
                assert_eq!(
                    out.channel(0)[n].to_bits(),
                    input.channel(0)[n].to_bits(),
                    "{path:?} n {n}"
                );
            }
        }
    }

    #[test]
    fn invalid_coefficients_are_rejected() {
        let mut mat = Matrix::new(2, 2);
        assert!(mat.set(0, 1, 0.5, 0));
        let before = mat.clone();
        for bad in [f32::NAN, f32::INFINITY, f32::MAX] {
            assert!(!mat.set(0, 1, bad, 10), "{bad}");
            assert!(!mat.set(1, 0, bad, 10), "{bad}");
        }
        assert!(!mat.set(2, 0, 0.5, 0));
        assert!(!mat.set(0, 2, 0.5, 0));
        assert_eq!(mat, before);
    }

    #[test]
    fn clone_reserves_every_input() {
        let mut mat = Matrix::new(16, 3);
        assert!(mat.set(4, 1, 0.5, 0));
        let copy = mat.clone();
        assert_eq!(copy, mat);
        assert!(copy.columns.iter().all(|c| c.capacity() >= 16));
    }

    // Ducks.

    use crate::duck_gain;

    /// Random envelopes, per detector: none, full, a step, a ramp or
    /// uniform noise within 0 and 1.
    fn envelopes(rng: &mut Rng, env: &mut PlanarBuffer, frames: usize) {
        for g in 0..MAX_DUCKS {
            let kind = rng.below(5);
            let (a, b) = ((rng.unit() + 1.0) * 0.5, (rng.unit() + 1.0) * 0.5);
            let at = rng.below(frames + 1);
            for (n, e) in env.channel_mut(g)[..frames].iter_mut().enumerate() {
                *e = match kind {
                    0 => 0.0,
                    1 => 1.0,
                    2 => {
                        if n < at {
                            a
                        } else {
                            b
                        }
                    }
                    3 => a + (b - a) * n as f32 / frames as f32,
                    _ => (rng.unit() + 1.0) * 0.5,
                };
            }
        }
    }

    /// Sets random masks, some 0, on random coefficients and random
    /// depths, with random lengths including none.
    fn duck_changes(rng: &mut Rng, m: &mut Matrix, count: usize) {
        for _ in 0..count {
            let (i, j) = (rng.below(m.inputs()), rng.below(m.outputs()));
            let len = match rng.below(4) {
                0 => 0,
                _ => rng.below(2000) as u32,
            };
            let mask = if rng.below(4) == 0 {
                0
            } else {
                rng.next_u64() as u8
            };
            assert!(m.set_mask(i, j, mask, len));
            let db = (rng.unit() + 1.0) * 20.0;
            assert!(m.set_depth(i, rng.below(MAX_DUCKS), db, len));
        }
    }

    /// Ducked terms in random matrices, under random masks, depths,
    /// envelopes and their changes, coefficient changes, blocks shorter
    /// and longer than an interval, skips and positions in the interval,
    /// on `path` and on the scalar reference, with FTZ/DAZ off and on:
    /// every output sample and the state bit for bit.
    fn ducked_bit_exact_vs_scalar(path: Path, seed: u64) {
        if !path_runs("ducked_bit_exact_vs_scalar", path) {
            return;
        }
        let mut ducked = 0;
        for ftz in [false, true] {
            let saved = set_ftz_daz(ftz);
            let mut rng = Rng::new(seed);
            for trial in 0..24 {
                let (n, m) = (1 + rng.below(24), 1 + rng.below(24));
                let mut reference = Matrix::new(n, m);
                change(&mut rng, &mut reference, n * m);
                duck_changes(&mut rng, &mut reference, n * m);
                reference.align(rng.next_u64());
                let mut under_test = reference.clone();
                let mut input = PlanarBuffer::new(n, CAP);
                let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
                let mut want = PlanarBuffer::new(m, CAP);
                let mut got = PlanarBuffer::new(m, CAP);
                for block in 0..6 {
                    let frames = match rng.below(3) {
                        0 => 1 + rng.below(9),
                        _ => 1 + rng.below(CAP),
                    };
                    fill(&mut rng, &mut input, frames);
                    envelopes(&mut rng, &mut env, frames);
                    reference.process_ducked_on(Path::Scalar, &input, &env, &mut want, frames);
                    under_test.process_ducked_on(path, &input, &env, &mut got, frames);
                    let ctx = format!("ftz {ftz} trial {trial} {n}x{m} block {block}/{frames}");
                    assert_bits_eq(&want, &got, frames, &ctx);
                    assert_eq!(reference, under_test, "{ctx}: state");
                    ducked += reference
                        .ducks
                        .iter()
                        .flatten()
                        .filter(|d| d.to < 1.0)
                        .count();
                    let k = rng.below(n * m / 4 + 2);
                    change(&mut rng, &mut reference, k);
                    duck_changes(&mut rng, &mut reference, k);
                    match rng.below(4) {
                        0 => reference.advance(rng.below(40)),
                        1 => reference.align(rng.next_u64()),
                        _ => {}
                    }
                    under_test.copy_from(&reference);
                }
            }
            restore_mxcsr(saved);
        }
        assert!(ducked > 1000, "only {ducked} ducked terms");
    }

    #[test]
    fn ducked_bit_exact_avx2() {
        ducked_bit_exact_vs_scalar(Path::Avx2, 0xd2);
    }

    /// A matrix whose masks are 0 gives the bits of one that never had a
    /// mask, depths and envelopes whatever they are, and a coefficient
    /// whose mask goes back to 0 returns to them once its crossfade has
    /// ended and its gain is back at no duck, with no duck left.
    #[test]
    fn mask_0_gives_the_bits_of_no_duck() {
        for path in Path::ALL {
            if !path_runs("mask_0_gives_the_bits_of_no_duck", path) {
                continue;
            }
            let mut rng = Rng::new(0x3a5c);
            let (n, m) = (12, 10);
            let mut plain = Matrix::new(n, m);
            change(&mut rng, &mut plain, n * m);
            let mut ducked = plain.clone();
            for i in 0..n {
                for g in 0..MAX_DUCKS {
                    assert!(ducked.set_depth(i, g, 40.0, 100));
                }
            }
            let mut b = Buffers::new(n, m);
            for _ in 0..8 {
                b.run(path, &mut plain, &mut ducked, &mut rng, 300, None);
            }
            assert!(ducked.ducks.iter().all(Vec::is_empty));
            for i in 0..n {
                for j in 0..m {
                    assert!(ducked.set_mask(i, j, 0xff, 50));
                }
            }
            for _ in 0..4 {
                b.run(path, &mut plain, &mut ducked, &mut rng, 300, Some(1.0));
            }
            for i in 0..n {
                for j in 0..m {
                    assert!(ducked.set_mask(i, j, 0, 70));
                }
            }
            // The crossfade and two intervals after it.
            let mut frames = 0;
            while frames < 70 + 16 {
                frames += b.run(path, &mut plain, &mut ducked, &mut rng, 30, Some(0.5));
            }
            assert!(ducked.ducks.iter().all(Vec::is_empty), "{path:?}");
            for _ in 0..8 {
                b.run(path, &mut plain, &mut ducked, &mut rng, 300, None);
            }
        }
    }

    /// The buffers of a matrix and its twin with ducks.
    struct Buffers {
        input: PlanarBuffer,
        env: PlanarBuffer,
        want: PlanarBuffer,
        got: PlanarBuffer,
    }

    impl Buffers {
        fn new(n: usize, m: usize) -> Buffers {
            Buffers {
                input: PlanarBuffer::new(n, CAP),
                env: PlanarBuffer::new(MAX_DUCKS, CAP),
                want: PlanarBuffer::new(m, CAP),
                got: PlanarBuffer::new(m, CAP),
            }
        }

        /// A random block of up to `max` samples through `plain` and
        /// `ducked`, the envelopes all at `level` or random; with random
        /// envelopes, both outputs bit-identical. Returns its length.
        fn run(
            &mut self,
            path: Path,
            plain: &mut Matrix,
            ducked: &mut Matrix,
            rng: &mut Rng,
            max: usize,
            level: Option<f32>,
        ) -> usize {
            let frames = 1 + rng.below(max);
            fill(rng, &mut self.input, frames);
            match level {
                Some(e) => (0..MAX_DUCKS).for_each(|g| self.env.channel_mut(g).fill(e)),
                None => envelopes(rng, &mut self.env, frames),
            }
            plain.process_on(path, &self.input, &mut self.want, frames);
            ducked.process_ducked_on(path, &self.input, &self.env, &mut self.got, frames);
            if level.is_none() {
                assert_bits_eq(&self.want, &self.got, frames, &format!("{path:?}"));
            }
            frames
        }
    }

    /// The gain of a ducked term moves linearly over each interval of the
    /// sample index to the duck gain of the envelope at the last sample
    /// of the interval before, whatever the split into blocks.
    #[test]
    fn a_duck_follows_the_envelope_an_interval_on() {
        for path in Path::ALL {
            if !path_runs("a_duck_follows_the_envelope_an_interval_on", path) {
                continue;
            }
            let mut rng = Rng::new(0x1e7);
            let (c, depth, start) = (0.75f32, 18.0f32, 5u64);
            let mut m = Matrix::new(1, 1);
            assert!(m.set(0, 0, c, 0));
            assert!(m.set_depth(0, 2, depth, 0));
            assert!(m.set_mask(0, 0, 0b100, 0));
            m.align(start);
            let mut input = PlanarBuffer::new(1, CAP);
            let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
            let mut out = PlanarBuffer::new(1, CAP);
            let (mut from, mut to) = (1.0f32, 1.0f32);
            let mut index = start;
            while index < start + 2000 {
                let frames = 1 + rng.below(70);
                fill(&mut rng, &mut input, frames);
                envelopes(&mut rng, &mut env, frames);
                m.process_ducked_on(path, &input, &env, &mut out, frames);
                for k in 0..frames {
                    let p = (index % 8) as u32;
                    let d = from + (p + 1) as f32 * ((to - from) * 0.125);
                    let want = 0.0 + (c * d) * input.channel(0)[k];
                    let got = out.channel(0)[k];
                    assert_eq!(got.to_bits(), want.to_bits(), "{path:?} index {index}");
                    if p == 7 {
                        from = to;
                        to = duck_gain(depth, env.channel(2)[k]);
                    }
                    index += 1;
                }
            }
        }
    }

    /// Skipped samples move the depths and crossfades on and hold the
    /// duck gain at the last one computed across the end of an interval;
    /// the next interval moves from it.
    #[test]
    fn a_skip_holds_the_duck_gain() {
        let mut m = Matrix::new(1, 1);
        assert!(m.set(0, 0, 1.0, 0));
        assert!(m.set_depth(0, 0, 20.0, 0));
        assert!(m.set_mask(0, 0, 1, 0));
        let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
        env.channel_mut(0).fill(1.0);
        let input = {
            let mut b = PlanarBuffer::new(1, CAP);
            b.channel_mut(0).fill(1.0);
            b
        };
        let mut out = PlanarBuffer::new(1, CAP);
        m.process_ducked(&input, &env, &mut out, 11);
        let (from, to) = (m.ducks[0][0].from, m.ducks[0][0].to);
        assert!(from == 1.0 && to < 1.0);
        m.advance(30);
        assert_eq!((m.ducks[0][0].from, m.ducks[0][0].to), (to, to));
        assert_eq!(m.phase, (11 + 30) % 8);
        // The rest of the interval holds, the next moves to no duck.
        env.channel_mut(0).fill(0.0);
        m.process_ducked(&input, &env, &mut out, 16);
        let rest = 8 - (11 + 30) % 8;
        assert!(out.channel(0)[..rest].iter().all(|&s| s == to));
        assert_eq!(out.channel(0)[rest + 7], to + 8.0 * ((1.0 - to) * 0.125));
        // A new position holds as well.
        let held = m.ducks[0][0].to;
        m.align(3);
        assert_eq!((m.ducks[0][0].from, m.phase), (held, 3));
    }

    /// The largest step between two samples of a DC input through a
    /// ducked coefficient over `blocks` blocks of 37 samples.
    fn largest_step(
        m: &mut Matrix,
        path: Path,
        env: &PlanarBuffer,
        prev: &mut Option<f32>,
        blocks: usize,
    ) -> f32 {
        let mut input = PlanarBuffer::new(1, CAP);
        input.channel_mut(0).fill(0.5);
        let mut out = PlanarBuffer::new(1, CAP);
        let mut step = 0.0f32;
        for _ in 0..blocks {
            m.process_ducked_on(path, &input, env, &mut out, 37);
            for &s in &out.channel(0)[..37] {
                if let Some(p) = *prev {
                    step = step.max((s - p).abs());
                }
                *prev = Some(s);
            }
        }
        step
    }

    /// A mask change while ducked crossfades: adding a target, removing
    /// it, moving it to another detector and changing it again in the
    /// middle of a crossfade step by no more than a linear crossfade
    /// over the length does, where the same change with no length steps
    /// by the whole difference within one interval.
    #[test]
    fn a_mask_change_while_ducked_does_not_step() {
        const LEN: u32 = 480;
        // (mask before, masks after, one after another half the length
        // apart)
        let cases: [(u8, &[u8]); 4] = [
            (0b01, &[0]),
            (0, &[0b01]),
            (0b01, &[0b10]),
            (0b01, &[0, 0b11]),
        ];
        for path in Path::ALL {
            if !path_runs("a_mask_change_while_ducked_does_not_step", path) {
                continue;
            }
            for (before, after) in cases {
                let run = |len: u32| {
                    let mut m = Matrix::new(1, 1);
                    assert!(m.set(0, 0, 1.0, 0));
                    assert!(m.set_depth(0, 0, 20.0, 0));
                    assert!(m.set_depth(0, 1, 30.0, 0));
                    assert!(m.set_mask(0, 0, before, 0));
                    m.align(3);
                    let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
                    env.channel_mut(0).fill(1.0);
                    env.channel_mut(1).fill(0.5);
                    let prev = &mut None;
                    largest_step(&mut m, path, &env, prev, 2);
                    let settled = largest_step(&mut m, path, &env, prev, 4);
                    assert_eq!(settled, 0.0);
                    let mut step = 0.0f32;
                    for &mask in after {
                        assert!(m.set_mask(0, 0, mask, len));
                        step = step.max(largest_step(&mut m, path, &env, prev, 7));
                    }
                    step.max(largest_step(&mut m, path, &env, prev, 20))
                };
                let (ramped, jumped) = (run(LEN), run(0));
                // The output's range over the case: detector 0 at 20 dB,
                // detector 1 at 15.
                let gain = |mask: u8| match mask {
                    0 => 1.0,
                    0b10 => duck_gain(30.0, 0.5),
                    _ => duck_gain(20.0, 1.0),
                };
                let gains = after.iter().map(|&k| gain(k)).chain([gain(before)]);
                let (lo, hi) = gains.fold((1.0f32, 0.0f32), |(l, h), g| (l.min(g), h.max(g)));
                let whole = 0.5 * (hi - lo);
                assert!(
                    ramped <= 1.1 * whole / LEN as f32,
                    "{path:?} {before:#b} -> {after:?}: step {ramped} when crossfaded"
                );
                assert!(
                    jumped >= 0.5 * whole / 8.0,
                    "{path:?} {before:#b} -> {after:?}: step {jumped} when jumped"
                );
            }
        }
    }

    /// Two mask changes on one coefficient before the end of an interval,
    /// with samples processed or skipped between them, step by no more
    /// than a linear crossfade over the length does: one back to the
    /// mask it started from does not move it at all.
    #[test]
    fn two_mask_changes_in_one_interval_do_not_step() {
        const LEN: u32 = 4800;
        let cases: [[u8; 3]; 4] = [
            [0b01, 0, 0b01],
            [0b01, 0b10, 0b01],
            [0, 0b01, 0],
            [0b01, 0b10, 0b10],
        ];
        let gain = |mask: u8| match mask {
            0 => 1.0,
            0b10 => duck_gain(20.0, 1.0),
            _ => duck_gain(40.0, 1.0),
        };
        for path in Path::ALL {
            if !path_runs("two_mask_changes_in_one_interval_do_not_step", path) {
                continue;
            }
            for [a, b, c] in cases {
                for skip in [false, true] {
                    let mut m = Matrix::new(1, 1);
                    assert!(m.set(0, 0, 1.0, 0));
                    assert!(m.set_depth(0, 0, 40.0, 0) && m.set_depth(0, 1, 20.0, 0));
                    assert!(m.set_mask(0, 0, a, 0));
                    let mut input = PlanarBuffer::new(1, CAP);
                    input.channel_mut(0).fill(0.5);
                    let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
                    env.channel_mut(0).fill(1.0);
                    env.channel_mut(1).fill(1.0);
                    let mut out = PlanarBuffer::new(1, CAP);
                    let mut prev: Option<f32> = None;
                    let mut step = 0.0f32;
                    let mut run = |m: &mut Matrix, frames: usize, step: &mut f32| {
                        m.process_ducked_on(path, &input, &env, &mut out, frames);
                        for &s in &out.channel(0)[..frames] {
                            if let Some(p) = prev {
                                *step = step.max((s - p).abs());
                            }
                            prev = Some(s);
                        }
                    };
                    // Settled, then to sample 2 of an interval.
                    run(&mut m, 64 + 2, &mut 0.0);
                    assert!(m.set_mask(0, 0, b, LEN));
                    if skip {
                        m.advance(3);
                    } else {
                        run(&mut m, 3, &mut step);
                    }
                    assert!(m.set_mask(0, 0, c, LEN));
                    for _ in 0..140 {
                        run(&mut m, 37, &mut step);
                    }
                    let whole = 0.5 * (gain(a) - gain(c)).abs().max((gain(b) - gain(c)).abs());
                    let ctx = format!("{path:?} {a:#b} -> {b:#b} -> {c:#b}, skip {skip}");
                    assert!(step <= 1.1 * whole / LEN as f32, "{ctx}: step {step}");
                    if a == c {
                        assert_eq!(step, 0.0, "{ctx}");
                    }
                    assert_eq!(prev, Some(0.5 * gain(c)), "{ctx}");
                }
            }
        }
    }

    /// A coefficient in several groups is ducked by the deepest of them,
    /// not by their sum or by the first, and by none outside its mask.
    #[test]
    fn overlapping_groups_take_the_deepest() {
        for path in Path::ALL {
            if !path_runs("overlapping_groups_take_the_deepest", path) {
                continue;
            }
            let mut rng = Rng::new(0x0d);
            let mut m = Matrix::new(2, 1);
            assert!(m.set(1, 0, 0.5, 0));
            for (g, db) in [(0, 6.0), (1, 12.0), (2, 30.0)] {
                assert!(m.set_depth(1, g, db, 0));
            }
            assert!(m.set_mask(1, 0, 0b011, 0));
            let mut input = PlanarBuffer::new(2, CAP);
            let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
            let mut out = PlanarBuffer::new(1, CAP);
            for (e1, gain) in [(0.75, duck_gain(12.0, 0.75)), (0.25, duck_gain(6.0, 1.0))] {
                env.channel_mut(0).fill(1.0);
                env.channel_mut(1).fill(e1);
                env.channel_mut(2).fill(1.0);
                // Two intervals to settle, then a block at the gain.
                fill(&mut rng, &mut input, 16);
                m.process_ducked_on(path, &input, &env, &mut out, 16);
                fill(&mut rng, &mut input, 100);
                m.process_ducked_on(path, &input, &env, &mut out, 100);
                for n in 0..100 {
                    let want = 0.0 + (0.5 * gain) * input.channel(1)[n];
                    assert_eq!(out.channel(0)[n].to_bits(), want.to_bits(), "{path:?} {e1}");
                }
            }
        }
    }

    /// The outputs of one input each take the gain of their own mask
    /// and depths, in every interval of every block.
    #[test]
    fn each_output_of_an_input_takes_its_own_mask() {
        for path in Path::ALL {
            if !path_runs("each_output_of_an_input_takes_its_own_mask", path) {
                continue;
            }
            let mut rng = Rng::new(0x77);
            let mut m = Matrix::new(1, 3);
            for (j, mask) in [0b01u8, 0b10, 0b11].into_iter().enumerate() {
                assert!(m.set(0, j, 0.5, 0));
                assert!(m.set_mask(0, j, mask, 0));
            }
            assert!(m.set_depth(0, 0, 10.0, 0) && m.set_depth(0, 1, 20.0, 0));
            let mut input = PlanarBuffer::new(1, CAP);
            let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
            let mut out = PlanarBuffer::new(3, CAP);
            for (e0, e1) in [(1.0, 0.25), (0.5, 1.0), (0.0, 0.0), (1.0, 1.0)] {
                env.channel_mut(0).fill(e0);
                env.channel_mut(1).fill(e1);
                fill(&mut rng, &mut input, 16);
                m.process_ducked_on(path, &input, &env, &mut out, 16);
                fill(&mut rng, &mut input, 40);
                m.process_ducked_on(path, &input, &env, &mut out, 40);
                let (a, b) = (duck_gain(10.0, e0), duck_gain(20.0, e1));
                let deepest = if 10.0 * e0 > 20.0 * e1 { a } else { b };
                for (j, gain) in [a, b, deepest].into_iter().enumerate() {
                    for n in 0..40 {
                        let want = 0.0 + (0.5 * gain) * input.channel(0)[n];
                        let got = out.channel(j)[n];
                        assert_eq!(got.to_bits(), want.to_bits(), "{path:?} output {j}");
                    }
                }
            }
        }
    }

    /// A matrix with ducks round-trips through its serde form mid-duck
    /// and continues bit-identically; a matrix without has the form it
    /// had before ducks existed.
    #[test]
    fn a_ducked_matrix_round_trips() {
        let mut rng = Rng::new(0x5e);
        let plain = {
            let mut m = Matrix::new(3, 2);
            change(&mut rng, &mut m, 6);
            m
        };
        let json = serde_json::to_string(&plain).unwrap();
        for key in ["mask", "duck", "depths", "phase"] {
            assert!(!json.contains(key), "{json}");
        }
        let mut m = Matrix::new(5, 4);
        change(&mut rng, &mut m, 20);
        assert!(m.set(1, 2, 0.5, 0) && m.set(3, 1, 0.25, 100));
        duck_changes(&mut rng, &mut m, 12);
        assert!(m.set_mask(1, 2, 0b101, 0) && m.set_depth(1, 0, 10.0, 0));
        let mut input = PlanarBuffer::new(5, CAP);
        let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
        let (mut a, mut b) = (PlanarBuffer::new(4, CAP), PlanarBuffer::new(4, CAP));
        fill(&mut rng, &mut input, 203);
        envelopes(&mut rng, &mut env, 203);
        m.process_ducked(&input, &env, &mut a, 203);
        assert!(m.set_mask(1, 2, 0b010, 300) && m.set_mask(3, 1, 0, 300));
        m.process_ducked(&input, &env, &mut a, 30);
        assert!(m.phase != 0 && m.ducks.iter().flatten().any(|d| d.pos < d.len));
        let json = serde_json::to_string(&m).unwrap();
        let mut copy: Matrix = serde_json::from_str(&json).unwrap();
        assert_eq!(copy, m);
        for _ in 0..4 {
            fill(&mut rng, &mut input, 150);
            envelopes(&mut rng, &mut env, 150);
            m.process_ducked(&input, &env, &mut a, 150);
            copy.process_ducked(&input, &env, &mut b, 150);
            assert_bits_eq(&a, &b, 150, "after the round trip");
        }
    }

    /// What no matrix holds of ducks is refused when decoded.
    #[test]
    fn a_duck_no_matrix_holds_is_refused() {
        use serde_json::{Value, json};
        let mut m = Matrix::new(2, 2);
        assert!(m.set(1, 0, 0.5, 0) && m.set(0, 1, 0.5, 0));
        assert!(m.set_mask(1, 0, 0b11, 100) && m.set_mask(0, 0, 0b100, 0));
        assert!(m.set_depth(1, 1, 10.0, 50));
        m.advance(3);
        let wire: Value = serde_json::to_value(&m).unwrap();
        assert!(serde_json::from_value::<Matrix>(wire.clone()).is_ok());
        fn term(w: &mut Value) -> &mut Value {
            &mut w["columns"][0][0]
        }
        let refused = |what: &str, f: &dyn Fn(&mut Value)| {
            let mut w = wire.clone();
            f(&mut w);
            assert!(serde_json::from_value::<Matrix>(w).is_err(), "{what}");
        };
        refused("a term mask other than the table's", &|w| {
            term(w)["mask"] = json!(1)
        });
        refused("a mask with no duck", &|w| {
            term(w).as_object_mut().unwrap().remove("duck");
        });
        refused("a duck gain above 1", &|w| {
            term(w)["duck"]["to"] = json!(1.5)
        });
        refused("a negative duck gain", &|w| {
            term(w)["duck"]["live"] = json!(-0.25)
        });
        refused("a live gain of mask 0 other than 1", &|w| {
            term(w)["duck"]["live_mask"] = json!(0);
            term(w)["duck"]["live"] = json!(0.5);
        });
        refused("a bias beyond the distance of two gains", &|w| {
            term(w)["duck"]["bias"] = json!(1.5)
        });
        refused("a crossfade past its length", &|w| {
            term(w)["duck"]["pos"] = json!(101)
        });
        refused("a crossfade too long", &|w| {
            term(w)["duck"]["len"] = json!(MAX_RAMP_LEN + 1);
            term(w)["duck"]["pos"] = json!(0);
        });
        refused("a mask of 0 listed", &|w| w["masks"][0][1] = json!(0));
        refused("a mask past the coefficients", &|w| {
            w["masks"][1][0] = json!(4)
        });
        refused("masks out of order", &|w| {
            w["masks"] = json!([[2, 3], [0, 4]]);
        });
        refused("a depth beyond 40 dB", &|w| {
            w["depths"][0][1]["target"] = json!(41.0)
        });
        refused("a depth past the inputs", &|w| {
            w["depths"][0][0] = json!(2 * MAX_DUCKS)
        });
        refused("a position past the interval", &|w| {
            w["phase"] = json!(GAIN_INTERVAL)
        });
    }

    /// A matrix the size of which a peer names beyond the limit in its
    /// inputs alone is refused before anything is allocated for it: with
    /// no output it has no coefficient, but a duck table per input.
    #[test]
    fn a_matrix_of_too_many_inputs_is_refused() {
        for inputs in [MAX_COEFFICIENTS + 1, 1 << 34, usize::MAX / 2] {
            let wire = format!("{{\"inputs\":{inputs},\"columns\":[]}}");
            assert!(serde_json::from_str::<Matrix>(&wire).is_err(), "{inputs}");
        }
        let wire = format!("{{\"inputs\":{MAX_COEFFICIENTS},\"columns\":[]}}");
        assert!(serde_json::from_str::<Matrix>(&wire).is_ok());
    }

    /// The top-level and term keys of `m`'s serde form.
    fn keys(m: &Matrix) -> (Vec<String>, Vec<String>) {
        let v = serde_json::to_value(m).unwrap();
        let names = |o: &serde_json::Value| o.as_object().unwrap().keys().cloned().collect();
        let term = v["columns"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|c| c.as_array().unwrap())
            .map(names)
            .fold(Vec::new(), |mut all: Vec<String>, k: Vec<String>| {
                all.extend(k);
                all.sort();
                all.dedup();
                all
            });
        (names(&v), term)
    }

    /// A matrix with no duck, processed or skipped over any number of
    /// samples, has the serde form a matrix had before ducks existed,
    /// and one that set masks and depths and cleared them again equals
    /// and encodes as one that never set any.
    #[test]
    fn a_matrix_without_ducks_keeps_its_form() {
        let mut rng = Rng::new(0xf0);
        let mut m = Matrix::new(3, 2);
        change(&mut rng, &mut m, 6);
        assert!(m.set(1, 1, 0.5, 0));
        let mut input = PlanarBuffer::new(3, CAP);
        let mut out = PlanarBuffer::new(2, CAP);
        fill(&mut rng, &mut input, 45);
        m.process(&input, &mut out, 45);
        m.advance(2);
        assert_eq!(m.phase, 47 % 8);
        let old = (
            vec!["columns".to_owned(), "inputs".to_owned()],
            vec!["input".to_owned(), "ramp".to_owned()],
        );
        assert_eq!(keys(&m), old);
        let copy: Matrix = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(copy, m);

        let fresh = m.clone();
        assert!(m.set_mask(1, 1, 0b11, 0) && m.set_depth(2, 3, 12.0, 0));
        assert_ne!(m, fresh);
        assert!(m.set_mask(1, 1, 0, 0) && m.set_depth(2, 3, 0.0, 0));
        m.process_ducked(&input, &PlanarBuffer::new(MAX_DUCKS, CAP), &mut out, 13);
        let mut fresh = fresh;
        fresh.process(&input, &mut out, 13);
        assert!(!m.has_ducks() && !m.masks.is_empty() && fresh.masks.is_empty());
        assert_eq!(m, fresh);
        assert_eq!(
            serde_json::to_string(&m).unwrap(),
            serde_json::to_string(&fresh).unwrap()
        );
        assert_eq!(keys(&m), old);
    }

    /// A matrix with ducks processed without envelopes would release
    /// every duck within one interval, so it is refused in debug builds.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "without envelopes")]
    fn process_refuses_a_matrix_with_ducks() {
        let mut m = Matrix::new(1, 1);
        assert!(m.set(0, 0, 1.0, 0) && m.set_mask(0, 0, 1, 0));
        let input = PlanarBuffer::new(1, 8);
        m.process(&input, &mut PlanarBuffer::new(1, 8), 8);
    }

    /// A fade-in brings a coefficient with a mask back ducked, from no
    /// duck to its mask's gain over the fade's length.
    #[test]
    fn a_fade_in_keeps_the_masks() {
        for path in Path::ALL {
            if !path_runs("a_fade_in_keeps_the_masks", path) {
                continue;
            }
            let mut rng = Rng::new(0xfade);
            let mut m = Matrix::new(3, 2);
            assert!(m.set_mask(0, 1, 0b1, 0) && m.set_mask(2, 1, 0b10, 0));
            assert!(m.set_depth(0, 0, 20.0, 0) && m.set_depth(2, 1, 10.0, 0));
            assert!(m.fade_in(&[0.0, 0.5, 0.25, 0.0, 0.0, 0.75], 100));
            assert_eq!(m.ducks[1].len(), 2);
            let mut input = PlanarBuffer::new(3, CAP);
            let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
            env.channel_mut(0).fill(1.0);
            env.channel_mut(1).fill(1.0);
            let mut out = PlanarBuffer::new(2, CAP);
            fill(&mut rng, &mut input, 120);
            m.process_ducked_on(path, &input, &env, &mut out, 120);
            fill(&mut rng, &mut input, 50);
            m.process_ducked_on(path, &input, &env, &mut out, 50);
            let (a, b) = (duck_gain(20.0, 1.0), duck_gain(10.0, 1.0));
            for n in 0..50 {
                let x = |i: usize| input.channel(i)[n];
                let want = 0.0 + (0.5 * a) * x(0) + (0.75 * b) * x(2);
                assert_eq!(out.channel(1)[n].to_bits(), want.to_bits(), "{path:?}");
                assert_eq!(out.channel(0)[n].to_bits(), (0.0 + 0.25 * x(1)).to_bits());
            }
        }
    }

    /// The duck gain of a ducked term with every input sample 1: its
    /// output divided by its coefficient.
    fn duck_of(out: &PlanarBuffer, n: usize, coefficient: f32) -> f32 {
        out.channel(0)[n] / coefficient
    }

    /// A coefficient that comes back from 0 with its mask set is ducked
    /// from the start, with no crossfade of the duck: its own ramp from
    /// silence hides the duck gain stepping in, and an install builds the
    /// same state.
    #[test]
    fn a_coefficient_back_from_0_is_ducked_from_the_start() {
        let mut m = Matrix::new(1, 1);
        assert!(m.set_depth(0, 0, 20.0, 0) && m.set_mask(0, 0, 1, 0));
        assert!(m.set(0, 0, 1.0, 0));
        let mut input = PlanarBuffer::new(1, CAP);
        input.channel_mut(0).fill(1.0);
        let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
        env.channel_mut(0).fill(1.0);
        let mut out = PlanarBuffer::new(1, CAP);
        m.process_ducked(&input, &env, &mut out, 64);
        let full = duck_gain(20.0, 1.0);
        assert_eq!(duck_of(&out, 63, 1.0), full);
        assert!(m.set(0, 0, 0.0, 0) && m.ducks[0].is_empty());
        m.process_ducked(&input, &env, &mut out, 64);
        assert!(m.set(0, 0, 1.0, 480));
        m.process_ducked(&input, &env, &mut out, 480);
        let mid = duck_of(&out, 240, 240.0 / 480.0);
        assert!((mid - full).abs() < 0.02, "{mid}, want about {full}");
    }

    /// A depth change while ducked ramps: the output does not step.
    #[test]
    fn a_depth_change_while_ducked_ramps() {
        for path in Path::ALL {
            if !path_runs("a_depth_change_while_ducked_ramps", path) {
                continue;
            }
            let run = |len: u32| {
                let mut m = Matrix::new(1, 1);
                assert!(m.set(0, 0, 1.0, 0) && m.set_mask(0, 0, 1, 0));
                let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
                env.channel_mut(0).fill(1.0);
                let prev = &mut None;
                largest_step(&mut m, path, &env, prev, 3);
                assert!(m.set_depth(0, 0, 40.0, len));
                let step = largest_step(&mut m, path, &env, prev, 20);
                assert_eq!(m.depth(0, 0), 40.0);
                step
            };
            let (ramped, jumped) = (run(480), run(0));
            assert!(
                ramped * 8.0 < jumped,
                "{path:?}: {ramped} ramped, {jumped} jumped"
            );
        }
    }

    /// A new position in the interval holds the duck gain where it is,
    /// also in the middle of a move.
    #[test]
    fn align_holds_the_gain_mid_interval() {
        let mut m = Matrix::new(1, 1);
        assert!(m.set(0, 0, 1.0, 0) && m.set_mask(0, 0, 1, 0));
        assert!(m.set_depth(0, 0, 20.0, 0));
        let mut input = PlanarBuffer::new(1, CAP);
        input.channel_mut(0).fill(1.0);
        let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
        env.channel_mut(0).fill(1.0);
        let mut out = PlanarBuffer::new(1, CAP);
        m.process_ducked(&input, &env, &mut out, 11);
        let d = m.ducks[0][0];
        assert!(d.from == 1.0 && d.to < 1.0);
        m.align(6);
        m.process_ducked(&input, &env, &mut out, 2);
        assert_eq!(out.channel(0)[..2], [d.to; 2]);
    }

    /// A mask changed while its old group releases follows the old group
    /// out: from the third interval after the change on, the term is never
    /// ducked deeper than the shallower of the old and the new mask's live
    /// gains, as it would be crossfading from the gain frozen at the
    /// change.
    #[test]
    fn a_mask_crossfade_follows_the_live_gains() {
        for path in Path::ALL {
            if !path_runs("a_mask_crossfade_follows_the_live_gains", path) {
                continue;
            }
            let mut m = Matrix::new(1, 1);
            assert!(m.set(0, 0, 1.0, 0) && m.set_mask(0, 0, 0b01, 0));
            assert!(m.set_depth(0, 0, 20.0, 0) && m.set_depth(0, 1, 10.0, 0));
            let mut input = PlanarBuffer::new(1, CAP);
            input.channel_mut(0).fill(1.0);
            let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
            env.channel_mut(0).fill(1.0);
            env.channel_mut(1).fill(1.0);
            let mut out = PlanarBuffer::new(1, CAP);
            m.process_ducked_on(path, &input, &env, &mut out, 64);
            assert!(m.set_mask(0, 0, 0b10, 4800));
            // The old group releases, by one step of its envelope per
            // block, while the crossfade runs.
            let new = duck_gain(10.0, 1.0);
            let mut index = 0;
            for b in 0..100 {
                let e = (1.0 - b as f32 / 20.0).max(0.0);
                env.channel_mut(0).fill(e);
                m.process_ducked_on(path, &input, &env, &mut out, 48);
                if b > 0 {
                    let old = duck_gain(20.0, (e + 1.0 / 20.0).min(1.0));
                    let floor = old.min(new);
                    for n in 0..48 {
                        let g = out.channel(0)[n];
                        assert!(g >= floor - 1e-6, "{path:?} sample {index}: {g} < {floor}");
                        index += 1;
                    }
                }
            }
            m.process_ducked_on(path, &input, &env, &mut out, 48);
            assert_eq!(out.channel(0)[47], new);
        }
    }

    /// Masks and depths set out of range or to values no matrix takes are
    /// refused and change nothing; a mask survives its coefficient going
    /// to 0 and coming back.
    #[test]
    fn masks_and_depths_are_checked_and_kept() {
        let mut m = Matrix::new(2, 3);
        assert!(!m.set_mask(2, 0, 1, 0) && !m.set_mask(0, 3, 1, 0));
        for (i, g, db) in [(2, 0, 1.0), (0, MAX_DUCKS, 1.0), (0, 0, -1.0), (0, 0, 40.5)] {
            assert!(!m.set_depth(i, g, db, 0), "{i} {g} {db}");
        }
        assert!(!m.set_depth(0, 0, f32::NAN, 0));
        assert_eq!(m, Matrix::new(2, 3));
        assert!(m.set_depth(1, 7, 40.0, 10));
        assert_eq!(m.depth(1, 7), 0.0);
        m.advance(10);
        assert_eq!(m.depth(1, 7), 40.0);
        assert!(m.set_mask(1, 2, 0b1000_0001, 0));
        assert_eq!(m.mask(1, 2), 0b1000_0001);
        assert!(m.set(1, 2, 0.5, 0) && m.set(1, 2, 0.0, 0));
        assert!(m.columns[2].is_empty() && m.ducks[2].is_empty());
        assert!(m.set(1, 2, 0.5, 0));
        assert_eq!(m.columns[2][0].mask, 0b1000_0001);
        assert_eq!(m.ducks[2].len(), 1);
        assert_eq!((m.mask(2, 0), m.depth(0, MAX_DUCKS)), (0, 0.0));
    }

    /// A delayed fade mixes to the same bits on the best path and the
    /// scalar one, and in any block sizes.
    #[test]
    fn a_delayed_fade_is_bit_exact_across_paths_and_block_sizes() {
        let frames = 1100;
        let mut rng = Rng::new(0x51de);
        let mut input = PlanarBuffer::new(3, frames);
        fill(&mut rng, &mut input, frames);
        let setup = || {
            let mut m = Matrix::new(3, 2);
            for i in 0..3 {
                for o in 0..2 {
                    assert!(m.set(i, o, 0.5 + 0.25 * i as f32, 0));
                }
            }
            assert!(m.set_fade(0, 0, 0.0, 150, 400));
            assert!(m.set_fade(1, 0, 2.0, 301, 17));
            assert!(m.set_fade(2, 1, 0.125, 37, 0));
            m
        };
        let run = |path: Path, sizes: &[usize]| {
            let mut m = setup();
            let mut out = PlanarBuffer::new(2, frames);
            let mut at = 0;
            let mut per = vec![Vec::new(), Vec::new()];
            for &n in sizes.iter().cycle() {
                if at + n > frames {
                    break;
                }
                let mut block = PlanarBuffer::new(3, n);
                for c in 0..3 {
                    block.channel_mut(c)[..n].copy_from_slice(&input.channel(c)[at..at + n]);
                }
                m.process_on(path, &block, &mut out, n);
                for (c, p) in per.iter_mut().enumerate() {
                    p.extend(out.channel(c)[..n].iter().map(|v| v.to_bits()));
                }
                at += n;
            }
            per
        };
        let want = run(Path::Scalar, &[1100]);
        assert_eq!(want[0].len(), frames);
        for path in [Path::Scalar, Path::Avx2] {
            if !path.is_supported() {
                continue;
            }
            for sizes in [&[1100][..], &[100, 100, 55], &[8, 13, 64], &[1]] {
                let got = run(path, sizes);
                for c in 0..2 {
                    assert_eq!(got[c], want[c][..got[c].len()], "{path:?} {sizes:?}");
                }
            }
        }
    }

    /// A masked coefficient a fade brings in is ducked as an install of
    /// the same fade builds it, with no crossfade of its own: the fade
    /// ramps it up from silence, so a replica rebuilt mid-fade holds the
    /// bits of one that applied the entry.
    #[test]
    fn a_fade_inserted_masked_term_matches_an_install_of_it() {
        let ramp = Ramp::resume(0.0, 0.25, 40, 400, 0).unwrap();
        let mut live = Matrix::new(1, 1);
        assert!(live.set_mask(0, 0, 0b1, 0));
        assert!(live.set_resumed(0, 0, 0.25, Some(ramp)));
        let mut built = Matrix::new(1, 1);
        assert!(built.fade_in_ducked_resuming(&[0.25], &[0b1], &[], 0, &[(0, ramp)]));
        assert!(live == built);
    }

    /// A matrix filled with its masked terms settled at steady envelopes
    /// mixes from its first sample the bits of one that has run at those
    /// envelopes for a while: the duck does not start at 1 and move to its
    /// gain over two intervals.
    #[test]
    fn a_settled_fill_mixes_the_bits_of_a_running_matrix() {
        for path in Path::ALL {
            if !path_runs("a_settled_fill_mixes_the_bits_of_a_running_matrix", path) {
                continue;
            }
            let mut rng = Rng::new(0x5e771e);
            let mut input = PlanarBuffer::new(1, CAP);
            let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
            env.channel_mut(0).fill(1.0);
            let mut out = PlanarBuffer::new(1, CAP);
            let mut live = Matrix::new(1, 1);
            assert!(live.set_depth(0, 0, 20.0, 0) && live.set_mask(0, 0, 1, 0));
            assert!(live.set(0, 0, 0.5, 0));
            fill(&mut rng, &mut input, 200);
            live.process_ducked_on(path, &input, &env, &mut out, 200);
            let mut depths = vec![0.0; MAX_DUCKS];
            depths[0] = 20.0;
            let mut built = Matrix::new(1, 1);
            built.fade_in_ducked(&[0.5], &[1], &depths, 0);
            built.settle_ducks(&[1.0]);
            fill(&mut rng, &mut input, 64);
            let mut want = PlanarBuffer::new(1, CAP);
            live.process_ducked_on(path, &input, &env, &mut want, 64);
            built.process_ducked_on(path, &input, &env, &mut out, 64);
            for n in 0..64 {
                assert_eq!(
                    out.channel(0)[n].to_bits(),
                    want.channel(0)[n].to_bits(),
                    "{path:?} sample {n}"
                );
            }
        }
    }
}

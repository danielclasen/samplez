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

    /// Whether an input of `inputs`, ascending ranges that do not
    /// overlap, feeds `output`: the target of its coefficient, the value
    /// the last change set, is other than 0. A target changes only with a
    /// change, so this holds from one change to the next however the
    /// coefficient ramps. False for an `output` out of range. It looks at
    /// each coefficient of the output at most once.
    pub fn feeds(
        &self,
        inputs: impl IntoIterator<Item = std::ops::Range<usize>>,
        output: usize,
    ) -> bool {
        let Some(col) = self.columns.get(output) else {
            return false;
        };
        let mut inputs = inputs.into_iter();
        let Some(mut range) = inputs.next() else {
            return false;
        };
        for t in col {
            while t.input >= range.end {
                match inputs.next() {
                    Some(r) => range = r,
                    None => return false,
                }
            }
            if t.input >= range.start && t.ramp.target() != 0.0 {
                return true;
            }
        }
        false
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

    /// Sets the depth of `input` by detector `duck` to the ramp `ramp`,
    /// dB, as it is: a depth ramp the matrix did not run itself continues
    /// where one that ran it is ([`Ramp::resume`]).
    ///
    /// Returns false and changes nothing if `input` or `duck` is out of
    /// range, or an end of the ramp is not within [`DEPTH_DB`].
    #[must_use]
    pub fn resume_depth(&mut self, input: usize, duck: usize, ramp: Ramp) -> bool {
        if input >= self.inputs || duck >= MAX_DUCKS || !ramp.is_within(DEPTH_DB) {
            return false;
        }
        if self.depths.is_empty() {
            self.depths.resize(self.inputs * MAX_DUCKS, Ramp::new(0.0));
        }
        self.depths[input * MAX_DUCKS + duck] = ramp;
        true
    }

    /// Gives the coefficient from `input` to `output` the duck a matrix
    /// holds at sample index `at`, the next sample, after the mask changes
    /// of `changes`, each `(index, mask)` in order at or after the first
    /// change's interval and before `at`, each crossfading over `len`
    /// samples, the first from mask `from` settled: the gains at each end
    /// of an interval from the one before the first change on, computed
    /// as a running matrix computes them, with every detector steady at
    /// its value in `envelopes` and detector `g`'s depth at index `n`
    /// `depth(g, n)`. A coefficient at 0 has no duck and keeps none; its
    /// mask stays as [`Matrix::set_mask`] or a fill set it, which should
    /// be the last change's.
    ///
    /// Returns false and changes nothing if `input` or `output` is out of
    /// range or `changes` is empty.
    #[must_use]
    pub fn resume_mask(
        &mut self,
        (input, output): (usize, usize),
        (from, changes): (u8, &[(u64, u8)]),
        len: u32,
        envelopes: &[f32],
        depth: impl Fn(usize, u64) -> f32,
        at: u64,
    ) -> bool {
        let outputs = self.columns.len();
        let Some(&(first, _)) = changes.first() else {
            return false;
        };
        if input >= self.inputs || output >= outputs {
            return false;
        }
        let len = len.min(MAX_RAMP_LEN);
        let gain = |mask: u8, n: u64| {
            let mut memo = Memo::NONE;
            for (g, a) in memo.atten.iter_mut().enumerate() {
                *a = depth(g, n) * envelopes.get(g).copied().unwrap_or(0.0);
            }
            memo.order();
            memo.gain(mask)
        };
        let interval = u64::from(GAIN_INTERVAL);
        // The end of the first change's interval. The duck is settled at
        // `from` at the two ends before it.
        let end = first - first % interval + interval - 1;
        let settled = end.saturating_sub(interval);
        let mut d = Duck {
            from: gain(from, settled.saturating_sub(interval)),
            to: gain(from, settled),
            live: gain(from, settled),
            live_mask: from,
            old: from,
            bias: 0.0,
            pos: len,
            len,
        };
        let (mut mask, mut start) = (from, None);
        let mut next = changes.iter().peekable();
        let mut e = end;
        loop {
            // The changes at or before `e`, before the end of its interval.
            while let Some(&&(c, m)) = next.peek().filter(|c| c.0 <= e && c.0 < at) {
                next.next();
                if m != mask {
                    mask = m;
                    d.change(len);
                    start = Some(c);
                }
            }
            if e >= at {
                break;
            }
            let live = gain(mask, e);
            let k = start.map_or(u64::MAX, |s| e - s);
            let to = if k < u64::from(len) {
                let old = (gain(d.old, e) + d.bias).clamp(0.0, 1.0);
                old + (k as f32 / len as f32) * (live - old)
            } else {
                live
            };
            d.live = live;
            d.live_mask = mask;
            d.from = d.to;
            d.to = to;
            e += interval;
        }
        d.pos = start.map_or(len, |s| u32::try_from(at - s).unwrap_or(u32::MAX).min(len));
        let (col, ducks) = (&mut self.columns[output], &mut self.ducks[output]);
        let Ok(k) = col.binary_search_by_key(&input, |t| t.input) else {
            return true;
        };
        let i = duck_index(col, k);
        let rest = col[k].mask == 0 && d.is_rest();
        let t = &mut col[k];
        match (t.ducked, rest) {
            (true, true) => {
                t.ducked = false;
                ducks.remove(i);
            }
            (true, false) => ducks[i] = d,
            (false, false) => {
                t.ducked = true;
                ducks.insert(i, d);
            }
            (false, true) => {}
        }
        true
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

    /// Starts the duck of every ducked term at the gains a running matrix
    /// holds for it when the detectors sit at `envelopes`, one per
    /// detector, steady: `from` and `to` the gains at the last two ends of
    /// an interval before the next sample, at the depths the depth ramps
    /// had there, and the live gain `to`, as they are after a few
    /// intervals at those envelopes, instead of at 1 moving to them over
    /// the two intervals after a fill. A depth ramp is taken to have held
    /// its start value before its start: one that started after the
    /// earlier of those two ends while another depth ramp still ran there
    /// gets other gains than a running matrix, which had the other ramp's
    /// value. A term of mask 0 or with no depth set keeps gain 1.
    pub fn settle_ducks(&mut self, envelopes: &[f32]) {
        // Samples back from the next one to the last end of an interval.
        let last = self.phase + 1;
        for (col, ducks) in self.columns.iter().zip(&mut self.ducks) {
            for (k, t) in col.iter().filter(|t| t.ducked).enumerate() {
                let gain = |back: u32| {
                    let mut memo = Memo::NONE;
                    if !self.depths.is_empty() {
                        let depths = &self.depths[t.input * MAX_DUCKS..(t.input + 1) * MAX_DUCKS];
                        for (g, (a, r)) in memo.atten.iter_mut().zip(depths).enumerate() {
                            let depth = r.value_at(r.pos.saturating_sub(back));
                            *a = depth * envelopes.get(g).copied().unwrap_or(0.0);
                        }
                        memo.order();
                    }
                    memo.gain(t.mask)
                };
                let d = &mut ducks[k];
                d.from = gain(last + GAIN_INTERVAL);
                d.to = gain(last);
                d.live = d.to;
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
mod tests;

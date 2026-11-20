//! Absolute engine targets and the entries between two of them: a product
//! that holds its engine's targets as values gives the engine only the
//! entries that move it from one set of targets to the next.

use samplez_dsp::{
    CompParams, DetectorParams, EqBand, GateParams, KeyFilter, LimiterParams, StripParam,
    ToneParams,
};

use crate::batch::strip_key;
use crate::ducks::{DuckSetting, DuckTargets};
use crate::engine::{Entry, Fade};

/// Every target an engine's log sets, as values.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Targets {
    /// The gain targets: one row per input channel, one column per output
    /// or meter column.
    pub gains: Vec<Vec<f32>>,
    /// The gain fades not over yet, shaped as `gains`, or empty for none:
    /// per coefficient the fade towards its gain in absolute sample
    /// indices, as [`Fade`] defines it. The one source of the fades:
    /// [`Targets::diff`] and [`Targets::assert`] turn it into entry fades
    /// for the index they are given, and an install of a product state
    /// resumes it (`ProductState::fading`).
    pub fading: Vec<Vec<Option<Fade>>>,
    /// Per input slot, the flow it reads; none for a detached slot.
    pub flows: Vec<Option<String>>,
    /// Per input slot, its strip parameters; of two with one key (one
    /// band, one leg, the gate), the later counts.
    pub strips: Vec<Vec<StripParam>>,
    /// Per output, its limiter parameters, for an output configured with
    /// `limiter`; none, or an output past the list, leaves the limiter as
    /// the engine holds it.
    pub limiters: Vec<Option<LimiterParams>>,
    /// Per input slot, its delay in samples; a slot past the list keeps
    /// the delay the engine holds.
    pub delays: Vec<u32>,
    /// Per input slot, its tone parameters, for a slot configured with
    /// `tone`; none, or a slot past the list, leaves the generator as the
    /// engine holds it.
    pub tones: Vec<Option<ToneParams>>,
    /// Per duck group, its settings.
    pub ducks: Vec<DuckTargets>,
    /// The duck mask of each coefficient, shaped as `gains`.
    pub masks: Vec<Vec<u8>>,
    /// Per output, whether it is enabled; an output past the list keeps
    /// its configured start state (`enabled`), as an install of a product
    /// state does. A product lists every output its engine has:
    /// [`Targets::assert`] sets only the outputs listed, and a diff emits
    /// an `Output` only for an output whose value changes, so a listed
    /// output that starts other than configured is set only by its first
    /// change.
    pub outputs: Vec<bool>,
    /// Per meter, the outputs then the configured meters, whether its
    /// loudness measurement runs ([`Entry::LoudnessRun`]); a meter past
    /// the list, every meter of an empty one, runs. A product lists every
    /// meter it pauses or ran again: [`Targets::assert`] sets only the
    /// meters listed.
    pub loudness_running: Vec<bool>,
    /// Per meter, shaped as `loudness_running`, its loudness reset epoch;
    /// a meter past the list, every meter of an empty one, is at 0. A
    /// product resets a meter's measurement by moving its epoch forward:
    /// [`Targets::diff`] gives one [`Entry::ResetLoudness`] for a meter
    /// whose epoch moved forward, however far. The epoch counts forward
    /// only and wraps: `next` is ahead of `held` when
    /// `next.wrapping_sub(held)` is in `1..=i32::MAX`, so a move back
    /// resets nothing. [`Targets::assert`] gives none, so a restamp, a
    /// void or a rebuild that re-asserts the targets never resets a meter.
    pub loudness_resets: Vec<u32>,
}

impl Targets {
    /// The entries that move an engine holding `self` to `next`: a `Gain`
    /// for each coefficient whose bit pattern differs, row by row; a
    /// `Strip` for each parameter that differs in any field's bit
    /// pattern, by slot and parameter; a `Limiter` output's
    /// parameters differ in any field's bit pattern; a `Delay` slot whose
    /// delay differs; a `Tone` for each slot whose tone parameters differ
    /// in any field's bit pattern; a `Duck` setting
    /// group that differs in any field's bit pattern, by group, its
    /// parameters, keys, mode and hold, then its depths by slot; a
    /// `DuckMask` for each coefficient whose mask differs, row by row, a
    /// mask `self` does not hold counting as 0, the mask an engine holds
    /// before any entry sets one; a
    /// `Route` for each slot whose flow differs, a `Detach` for one
    /// `next` holds none for; an `Output` for each output whose enabled
    /// state differs, an output past either list compared as enabled,
    /// whatever start the engine configures for it; a `LoudnessRun` for
    /// each meter whose running state differs, a meter past either list
    /// compared as running; a `ResetLoudness` for each meter whose reset
    /// epoch `next` holds ahead of `self`'s, one however many resets the
    /// epochs lie apart, as they would all apply at one index. Of the other
    /// targets, one `next` holds and `self` does not counts as
    /// different; one `self` holds and `next` does not gets no entry, so
    /// the engine keeps it.
    pub fn diff(&self, next: &Targets) -> Vec<Entry> {
        let mut entries = Vec::new();
        let any_fade = !(self.fading.is_empty() && next.fading.is_empty());
        for (input, row) in next.gains.iter().enumerate() {
            let was = self.gains.get(input);
            for (output, &gain) in row.iter().enumerate() {
                let held = was.and_then(|r| r.get(output));
                let fade = |t: &Targets| {
                    t.fading
                        .get(input)
                        .and_then(|r| r.get(output))
                        .copied()
                        .flatten()
                };
                let bits = |f: Option<Fade>| f.map(|f| (f.from.to_bits(), f.start, f.len));
                let fade_moved = any_fade && bits(fade(self)) != bits(fade(next));
                if held.is_none_or(|g| g.to_bits() != gain.to_bits()) || fade_moved {
                    entries.push(Entry::Gain {
                        input,
                        output,
                        gain,
                        fade: if any_fade { fade(next) } else { None },
                    });
                }
            }
        }
        strip_entries(&self.strips, &next.strips, &mut entries);
        for (output, params) in next.limiters.iter().enumerate() {
            let held = self.limiters.get(output).copied().flatten();
            if let Some(params) = *params
                && held.is_none_or(|h| !same_limiter(&h, &params))
            {
                entries.push(Entry::Limiter { output, params });
            }
        }
        for (slot, &samples) in next.delays.iter().enumerate() {
            if self.delays.get(slot) != Some(&samples) {
                entries.push(Entry::Delay { slot, samples });
            }
        }
        for (slot, params) in next.tones.iter().enumerate() {
            let held = self.tones.get(slot).copied().flatten();
            if let Some(p) = *params
                && held.is_none_or(|h| !h.same(&p))
            {
                entries.push(Entry::Tone {
                    slot,
                    frequency_hz: p.frequency_hz,
                    level_db: p.level_db,
                    ident: p.ident,
                });
            }
        }
        duck_entries(self, next, &mut entries);
        for (slot, flow) in next.flows.iter().enumerate() {
            if self.flows.get(slot) != Some(flow) {
                entries.push(flow_entry(slot, flow.as_deref()));
            }
        }
        let enabled = |t: &Targets, o: usize| t.outputs.get(o).copied().unwrap_or(true);
        for slot in 0..self.outputs.len().max(next.outputs.len()) {
            if enabled(self, slot) != enabled(next, slot) {
                let enabled = enabled(next, slot);
                entries.push(Entry::Output { slot, enabled });
            }
        }
        let runs = |t: &Targets, m: usize| t.loudness_running.get(m).copied().unwrap_or(true);
        for meter in 0..self.loudness_running.len().max(next.loudness_running.len()) {
            if runs(self, meter) != runs(next, meter) {
                let running = runs(next, meter);
                entries.push(Entry::LoudnessRun { meter, running });
            }
        }
        let epoch = |t: &Targets, m: usize| t.loudness_resets.get(m).copied().unwrap_or(0);
        for meter in 0..self.loudness_resets.len().max(next.loudness_resets.len()) {
            if ahead(epoch(self, meter), epoch(next, meter)) {
                entries.push(Entry::ResetLoudness { meter });
            }
        }
        entries
    }

    /// The entries that set every gain, strip, limiter and tone parameter
    /// and duck setting of `self`, whatever an engine holds, in the order of
    /// [`Targets::diff`], a `Route`, or a `Detach` for a slot without a
    /// flow, for each of `slots` in ascending order, an `Output` for
    /// each output `self` holds and a `LoudnessRun` for each meter it
    /// holds; never a `ResetLoudness`, a reset being an event and not a
    /// value. Of the duck masks,
    /// every one other than 0, and a 0 for each coefficient `masked`
    /// holds a mask other than 0 for: an engine holds
    /// a mask other than 0 only where an entry set one, so `masked`, every
    /// mask an engine may have been given ORed together, names every
    /// coefficient a 0 has to clear; none for every coefficient.
    pub fn assert(&self, slots: &[usize], masked: Option<&[Vec<u8>]>) -> Vec<Entry> {
        // Against no targets at all: every gain, zeros included, every
        // strip parameter, duck setting and mask other than 0.
        let mut entries = Targets::default().diff(&Targets {
            flows: Vec::new(),
            outputs: Vec::new(),
            loudness_running: Vec::new(),
            loudness_resets: Vec::new(),
            ..self.clone()
        });
        for (input, row) in self.masks.iter().enumerate() {
            for (output, &mask) in row.iter().enumerate() {
                let held = masked.is_none_or(|m| {
                    m.get(input)
                        .and_then(|r| r.get(output))
                        .is_some_and(|&k| k != 0)
                });
                if mask == 0 && held {
                    entries.push(Entry::DuckMask {
                        input,
                        output,
                        mask: 0,
                    });
                }
            }
        }
        let mut slots = slots.to_vec();
        slots.sort_unstable();
        slots.dedup();
        for slot in slots {
            if let Some(flow) = self.flows.get(slot) {
                entries.push(flow_entry(slot, flow.as_deref()));
            }
        }
        for (slot, &enabled) in self.outputs.iter().enumerate() {
            entries.push(Entry::Output { slot, enabled });
        }
        for (meter, &running) in self.loudness_running.iter().enumerate() {
            entries.push(Entry::LoudnessRun { meter, running });
        }
        entries
    }
}

/// Whether reset epoch `next` lies ahead of `held` on the wrapping
/// count: by 1 to `i32::MAX`.
fn ahead(held: u32, next: u32) -> bool {
    (1..=i32::MAX as u32).contains(&next.wrapping_sub(held))
}

/// The entry that has slot `slot` read `flow`: a `Route`, or a `Detach`
/// for none.
fn flow_entry(slot: usize, flow: Option<&str>) -> Entry {
    match flow {
        Some(flow_id) => Entry::Route {
            slot,
            flow_id: flow_id.to_owned(),
        },
        None => Entry::Detach { slot },
    }
}

/// The `Strip` entries of [`Targets::diff`], appended to `entries`. Kept
/// out of line: inlined into `diff`, it raises the instruction count of
/// a diff of gains alone.
#[inline(never)]
fn strip_entries(was: &[Vec<StripParam>], next: &[Vec<StripParam>], entries: &mut Vec<Entry>) {
    for (slot, params) in next.iter().enumerate() {
        let mut held = keyed(was.get(slot).map_or(&[], Vec::as_slice)).peekable();
        for (key, &param) in keyed(params) {
            while held.next_if(|&(k, _)| k < key).is_some() {}
            let w = held.next_if(|&(k, _)| k == key);
            if w.is_none_or(|(_, w)| !same_bits(w, &param)) {
                entries.push(Entry::Strip { slot, param });
            }
        }
    }
}

/// Whether two limiter settings agree in every field's bit pattern.
fn same_limiter(a: &LimiterParams, b: &LimiterParams) -> bool {
    a.enabled == b.enabled
        && a.ceiling_db.to_bits() == b.ceiling_db.to_bits()
        && a.release_ms.to_bits() == b.release_ms.to_bits()
}

/// The `Duck` and `DuckMask` entries of [`Targets::diff`], appended to
/// `entries`. Kept out of line, as the strips' are.
#[inline(never)]
fn duck_entries(was: &Targets, next: &Targets, entries: &mut Vec<Entry>) {
    let f = |x: f32, y: f32| x.to_bits() == y.to_bits() && x == y;
    for (group, d) in next.ducks.iter().enumerate() {
        let held = was.ducks.get(group);
        let mut push = |setting| entries.push(Entry::Duck { group, setting });
        if held.is_none_or(|h| !same_params(&h.params, &d.params)) {
            push(DuckSetting::Detector(Box::new(d.params)));
        }
        let same_keys = |h: &DuckTargets| {
            h.keys.len() == d.keys.len()
                && (h.keys.iter().zip(&d.keys)).all(|(a, b)| a.0 == b.0 && f(a.1, b.1))
        };
        if held.is_none_or(|h| !same_keys(h)) {
            push(DuckSetting::Keys(d.keys.clone()));
        }
        if held.is_none_or(|h| h.mode != d.mode) {
            push(DuckSetting::Mode(d.mode));
        }
        if held.is_none_or(|h| h.held != d.held) {
            push(DuckSetting::Hold { held: d.held });
        }
        for (slot, &db) in d.depths.iter().enumerate() {
            let was = held.and_then(|h| h.depths.get(slot));
            if was.is_none_or(|&w| !f(w, db)) {
                push(DuckSetting::Depth { slot, db });
            }
        }
    }
    for (input, row) in next.masks.iter().enumerate() {
        let was = was.masks.get(input);
        for (output, &mask) in row.iter().enumerate() {
            if was.and_then(|r| r.get(output)).copied().unwrap_or(0) != mask {
                entries.push(Entry::DuckMask {
                    input,
                    output,
                    mask,
                });
            }
        }
    }
}

/// Whether two detector parameter sets hold the same bit pattern in
/// every field, as [`same_bits`] compares strip parameters.
fn same_params(a: &DetectorParams, b: &DetectorParams) -> bool {
    let f = |x: f32, y: f32| x.to_bits() == y.to_bits() && x == y;
    let filter = |x: &KeyFilter, y: &KeyFilter| x.enabled == y.enabled && f(x.freq, y.freq);
    let DetectorParams {
        threshold_db,
        hysteresis_db,
        hpf,
        lpf,
        attack_ms,
        hold_ms,
        release_ms,
    } = a;
    f(*threshold_db, b.threshold_db)
        && f(*hysteresis_db, b.hysteresis_db)
        && filter(hpf, &b.hpf)
        && filter(lpf, &b.lpf)
        && f(*attack_ms, b.attack_ms)
        && f(*hold_ms, b.hold_ms)
        && f(*release_ms, b.release_ms)
}

/// Whether `a` and `b` hold the same bit pattern in every field: `f32`
/// fields by bits and as floats, so -0.0 and 0.0 differ as they do in a
/// core copy and a NaN, which no engine takes, differs from itself; the
/// others by value. Every pattern names every field, so a field added to
/// a parameter does not compile until it is compared here.
fn same_bits(a: &StripParam, b: &StripParam) -> bool {
    use StripParam as P;
    let f = |x: f32, y: f32| x.to_bits() == y.to_bits() && x == y;
    match (*a, *b) {
        (P::Trim { db }, P::Trim { db: y }) => f(db, y),
        (P::LegTrim { leg, db }, P::LegTrim { leg: l, db: y }) => leg == l && f(db, y),
        (
            P::Polarity { leg, inverted },
            P::Polarity {
                leg: l,
                inverted: i,
            },
        ) => leg == l && inverted == i,
        (
            P::Hpf { enabled, freq },
            P::Hpf {
                enabled: e,
                freq: y,
            },
        ) => enabled == e && f(freq, y),
        (P::Eq { enabled }, P::Eq { enabled: e }) => enabled == e,
        (P::EqBand { band, value }, P::EqBand { band: n, value: v }) => {
            let EqBand {
                kind,
                freq,
                gain_db,
                q,
            } = value;
            band == n && kind == v.kind && f(freq, v.freq) && f(gain_db, v.gain_db) && f(q, v.q)
        }
        (P::Gate(g), P::Gate(h)) => {
            let GateParams {
                enabled,
                threshold_db,
                range_db,
                attack_ms,
                release_ms,
            } = g;
            enabled == h.enabled
                && f(threshold_db, h.threshold_db)
                && f(range_db, h.range_db)
                && f(attack_ms, h.attack_ms)
                && f(release_ms, h.release_ms)
        }
        (P::Comp(c), P::Comp(d)) => {
            let CompParams {
                enabled,
                threshold_db,
                ratio,
                attack_ms,
                release_ms,
                knee_db,
                makeup_db,
            } = c;
            enabled == d.enabled
                && f(threshold_db, d.threshold_db)
                && f(ratio, d.ratio)
                && f(attack_ms, d.attack_ms)
                && f(release_ms, d.release_ms)
                && f(knee_db, d.knee_db)
                && f(makeup_db, d.makeup_db)
        }
        (P::DynamicsBeforeEq { enabled }, P::DynamicsBeforeEq { enabled: e }) => enabled == e,
        (
            P::Trim { .. }
            | P::LegTrim { .. }
            | P::Polarity { .. }
            | P::Hpf { .. }
            | P::Eq { .. }
            | P::EqBand { .. }
            | P::Gate(_)
            | P::Comp(_)
            | P::DynamicsBeforeEq { .. },
            _,
        ) => false,
    }
}

/// `params` by ascending key, the later of two with one key counting,
/// and nothing once it has ended, without allocating: in list order when the keys already ascend, as a
/// product lists them, by selection otherwise.
fn keyed(params: &[StripParam]) -> impl Iterator<Item = ((u8, usize), &StripParam)> {
    let ascending = params
        .windows(2)
        .all(|w| strip_key(&w[0]) < strip_key(&w[1]));
    let mut at = 0;
    let mut last = None;
    std::iter::from_fn(move || {
        if ascending {
            let p = params.get(at)?;
            at += 1;
            return Some((strip_key(p), p));
        }
        let mut least: Option<((u8, usize), &StripParam)> = None;
        for p in params {
            let key = strip_key(p);
            if last.is_none_or(|l| key > l) && least.is_none_or(|(k, _)| key <= k) {
                least = Some((key, p));
            }
        }
        let least = least?;
        last = Some(least.0);
        Some(least)
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use samplez_dsp::BandType;

    use super::*;

    fn gate() -> GateParams {
        GateParams {
            enabled: true,
            threshold_db: -40.0,
            range_db: -60.0,
            attack_ms: 1.0,
            release_ms: 100.0,
        }
    }

    fn targets() -> Targets {
        Targets {
            gains: vec![vec![0.5, 0.0], vec![0.25, 1.0]],
            flows: vec![Some("a".into()), Some("b".into())],
            strips: vec![vec![StripParam::Trim { db: -3.0 }], Vec::new()],
            ..Targets::default()
        }
    }

    #[test]
    fn equal_targets_need_no_entry() {
        assert_eq!(targets().diff(&targets()), Vec::new());
    }

    /// A VCA move as a product sees it: several coefficients, a strip
    /// parameter and a route change at once, each named once, in order.
    #[test]
    fn diff_names_each_changed_target_once_in_order() {
        let mut next = targets();
        next.gains[1][0] = 0.125;
        next.gains[0][1] = 0.75;
        next.strips[1].push(StripParam::Gate(gate()));
        next.flows[0] = Some("c".into());
        assert_eq!(
            targets().diff(&next),
            vec![
                Entry::Gain {
                    input: 0,
                    output: 1,
                    gain: 0.75,
                    fade: None
                },
                Entry::Gain {
                    input: 1,
                    output: 0,
                    gain: 0.125,
                    fade: None
                },
                Entry::Strip {
                    slot: 1,
                    param: StripParam::Gate(gate())
                },
                Entry::Route {
                    slot: 0,
                    flow_id: "c".into()
                },
            ]
        );
    }

    /// -0.0 equals 0.0 as a float but is another value to hold: replicas
    /// that compare bit patterns all name it.
    #[test]
    fn gains_compare_by_bit_pattern() {
        let mut next = targets();
        next.gains[0][1] = -0.0;
        assert_eq!(
            targets().diff(&next),
            vec![Entry::Gain {
                input: 0,
                output: 1,
                gain: -0.0,
                fade: None
            }]
        );
    }

    /// A strip field that moves from 0.0 to -0.0 is another bit pattern:
    /// every replica names it, as it names such a gain.
    #[test]
    fn strip_parameters_compare_by_bit_pattern() {
        let mut next = targets();
        next.strips[0] = vec![StripParam::Trim { db: -0.0 }];
        let mut base = targets();
        base.strips[0] = vec![StripParam::Trim { db: 0.0 }];
        assert_eq!(
            base.diff(&next),
            vec![Entry::Strip {
                slot: 0,
                param: StripParam::Trim { db: -0.0 }
            }]
        );
    }

    /// A whole parameter set is one entry carrying every field of the
    /// state, not only the field that moved.
    #[test]
    fn a_strip_parameter_changes_whole() {
        let gate = gate();
        let mut base = targets();
        base.strips[1].push(StripParam::Gate(gate));
        let mut next = base.clone();
        let moved = GateParams {
            threshold_db: gate.threshold_db - 6.0,
            ..gate
        };
        next.strips[1] = vec![StripParam::Gate(moved)];
        assert_eq!(
            base.diff(&next),
            vec![Entry::Strip {
                slot: 1,
                param: StripParam::Gate(moved)
            }]
        );
    }

    /// Two parameters of one key, two bands of one equaliser: the later
    /// of one key counts, and each band is its own.
    #[test]
    fn strip_parameters_are_keyed_by_band_and_leg() {
        let band = |gain_db| EqBand {
            kind: BandType::Peak,
            freq: 1000.0,
            gain_db,
            q: 1.0,
        };
        let mut next = targets();
        next.strips[0] = vec![
            StripParam::Trim { db: -3.0 },
            StripParam::EqBand {
                band: 0,
                value: band(1.0),
            },
            StripParam::EqBand {
                band: 1,
                value: band(2.0),
            },
            StripParam::EqBand {
                band: 0,
                value: band(3.0),
            },
        ];
        assert_eq!(
            targets().diff(&next),
            vec![
                Entry::Strip {
                    slot: 0,
                    param: StripParam::EqBand {
                        band: 0,
                        value: band(3.0)
                    }
                },
                Entry::Strip {
                    slot: 0,
                    param: StripParam::EqBand {
                        band: 1,
                        value: band(2.0)
                    }
                },
            ]
        );
    }

    /// A target the next set leaves out is not touched: the engine keeps
    /// what it holds.
    #[test]
    fn a_target_left_out_gets_no_entry() {
        let next = Targets {
            gains: vec![vec![0.5]],
            ..Targets::default()
        };
        assert_eq!(targets().diff(&next), Vec::new());
    }

    /// Asserting sets every gain, zeros included, and every strip
    /// parameter, and routes only the slots named.
    #[test]
    fn assert_sets_every_gain_and_parameter_and_the_routes_named() {
        let t = targets();
        let entries = t.assert(&[1, 1], None);
        let gains = entries
            .iter()
            .filter(|e| matches!(e, Entry::Gain { .. }))
            .count();
        assert_eq!(gains, 4);
        assert!(entries.contains(&Entry::Strip {
            slot: 0,
            param: StripParam::Trim { db: -3.0 }
        }));
        let routes: Vec<&Entry> = entries
            .iter()
            .filter(|e| matches!(e, Entry::Route { .. }))
            .collect();
        assert_eq!(
            routes,
            vec![&Entry::Route {
                slot: 1,
                flow_id: "b".into()
            }]
        );
        assert_eq!(t.assert(&[], None).len(), 5);
    }

    /// An output's enabled state is diffed, an output past a list counting
    /// as enabled, and asserted after the routes, one entry per output held.
    #[test]
    fn an_outputs_enabled_state_is_diffed_and_asserted() {
        let off = |slot| Entry::Output {
            slot,
            enabled: false,
        };
        let on = |slot| Entry::Output {
            slot,
            enabled: true,
        };
        let mut next = targets();
        next.outputs = vec![true, false];
        assert_eq!(targets().diff(&next), vec![off(1)]);
        assert_eq!(next.diff(&next), Vec::new());
        // Past the list an output is enabled: dropping a disabled one
        // enables it, dropping an enabled one changes nothing.
        assert_eq!(next.diff(&targets()), vec![on(1)]);
        next.outputs = vec![true];
        assert_eq!(targets().diff(&next), Vec::new());
        // After the routes; one per output held, both values.
        next.outputs = vec![false, true];
        let all = next.assert(&[0], None);
        let n = all.len();
        assert_eq!(all[n - 2..], [off(0), on(1)]);
        assert!(matches!(all[n - 3], Entry::Route { slot: 0, .. }));
    }

    /// A meter's running state is diffed, a meter past a list counting as
    /// running, after the outputs; asserted, one entry per meter held,
    /// last, and none twice.
    #[test]
    fn a_meters_running_state_is_diffed_and_asserted() {
        let run = |meter, running| Entry::LoudnessRun { meter, running };
        let mut next = targets();
        next.loudness_running = vec![true, false, true];
        assert_eq!(targets().diff(&next), vec![run(1, false)]);
        assert_eq!(next.diff(&next), Vec::new());
        // Past the list a meter runs: dropping a paused one runs it.
        assert_eq!(next.diff(&targets()), vec![run(1, true)]);
        next.loudness_running = vec![true];
        assert_eq!(targets().diff(&next), Vec::new());
        next.loudness_running = vec![false, true];
        next.outputs = vec![false];
        let changed = targets().diff(&next);
        let n = changed.len();
        assert_eq!(
            changed[n - 2..],
            [
                Entry::Output {
                    slot: 0,
                    enabled: false
                },
                run(0, false)
            ]
        );
        let all = next.assert(&[], None);
        let n = all.len();
        assert_eq!(all[n - 2..], [run(0, false), run(1, true)]);
        let runs = all
            .iter()
            .filter(|e| matches!(e, Entry::LoudnessRun { .. }))
            .count();
        assert_eq!(runs, 2);
    }

    /// A meter's reset epoch moving forward gives one `ResetLoudness`,
    /// however far it moved, a meter past a list at 0; an equal epoch or
    /// one moved back gives none, across the wrap too. Asserted, targets
    /// give no reset at all.
    #[test]
    fn a_reset_epoch_moving_forward_resets_once_and_assert_never() {
        let reset = |meter| Entry::ResetLoudness { meter };
        let at = |epochs: Vec<u32>| Targets {
            loudness_resets: epochs,
            ..targets()
        };
        assert_eq!(targets().diff(&at(vec![0, 1])), vec![reset(1)]);
        assert_eq!(at(vec![0, 1]).diff(&at(vec![0, 1])), Vec::new());
        assert_eq!(at(vec![0, 1]).diff(&at(vec![3, 1, 0])), vec![reset(0)]);
        assert_eq!(at(vec![2]).diff(&at(vec![1])), Vec::new());
        assert_eq!(at(vec![1]).diff(&targets()), Vec::new());
        assert_eq!(at(vec![u32::MAX]).diff(&at(vec![0])), vec![reset(0)]);
        assert_eq!(at(vec![0]).diff(&at(vec![u32::MAX])), Vec::new());
        let all = at(vec![5, 7]).assert(&[], None);
        assert!(
            !all.iter().any(|e| matches!(e, Entry::ResetLoudness { .. })),
            "{all:?}"
        );
    }

    /// A slot whose flow `next` holds none for is detached, also when
    /// `self` holds no flow for it; one already detached gets no entry.
    /// Asserted, a slot without a flow is detached.
    #[test]
    fn a_slot_without_a_flow_is_detached() {
        let mut next = targets();
        next.flows[1] = None;
        assert_eq!(targets().diff(&next), vec![Entry::Detach { slot: 1 }]);
        assert_eq!(next.diff(&next), Vec::new());
        let unknown = Targets {
            flows: Vec::new(),
            ..next.clone()
        };
        assert!(unknown.diff(&next).contains(&Entry::Detach { slot: 1 }));
        let flows: Vec<Entry> = (next.assert(&[1, 0], None).into_iter())
            .filter(|e| matches!(e, Entry::Route { .. } | Entry::Detach { .. }))
            .collect();
        assert_eq!(
            flows,
            vec![
                Entry::Route {
                    slot: 0,
                    flow_id: "a".into()
                },
                Entry::Detach { slot: 1 },
            ]
        );
    }

    /// For every parameter, pairs that differ in one field each: an `f32`
    /// field as 0.0 and -0.0, any other field by value.
    fn one_field_apart() -> Vec<(StripParam, StripParam)> {
        let band = |kind, freq, gain_db, q| EqBand {
            kind,
            freq,
            gain_db,
            q,
        };
        let eq = |b, value| StripParam::EqBand { band: b, value };
        let peak = BandType::Peak;
        let gate = |enabled, t, r, a, rel| {
            StripParam::Gate(GateParams {
                enabled,
                threshold_db: t,
                range_db: r,
                attack_ms: a,
                release_ms: rel,
            })
        };
        let comp = |enabled, [t, r, a, rel, k, m]: [f32; 6]| {
            StripParam::Comp(CompParams {
                enabled,
                threshold_db: t,
                ratio: r,
                attack_ms: a,
                release_ms: rel,
                knee_db: k,
                makeup_db: m,
            })
        };
        let mut pairs = vec![
            (StripParam::Trim { db: 0.0 }, StripParam::Trim { db: -0.0 }),
            (
                StripParam::LegTrim { leg: 0, db: 0.0 },
                StripParam::LegTrim { leg: 0, db: -0.0 },
            ),
            (
                StripParam::LegTrim { leg: 0, db: 1.0 },
                StripParam::LegTrim { leg: 1, db: 1.0 },
            ),
            (
                StripParam::Polarity {
                    leg: 0,
                    inverted: true,
                },
                StripParam::Polarity {
                    leg: 1,
                    inverted: true,
                },
            ),
            (
                StripParam::Polarity {
                    leg: 0,
                    inverted: false,
                },
                StripParam::Polarity {
                    leg: 0,
                    inverted: true,
                },
            ),
            (
                StripParam::Hpf {
                    enabled: true,
                    freq: 0.0,
                },
                StripParam::Hpf {
                    enabled: true,
                    freq: -0.0,
                },
            ),
            (
                StripParam::Hpf {
                    enabled: false,
                    freq: 80.0,
                },
                StripParam::Hpf {
                    enabled: true,
                    freq: 80.0,
                },
            ),
            (
                StripParam::Eq { enabled: false },
                StripParam::Eq { enabled: true },
            ),
            (
                eq(0, band(peak, 0.0, 1.0, 1.0)),
                eq(0, band(peak, -0.0, 1.0, 1.0)),
            ),
            (
                eq(0, band(peak, 1.0, 0.0, 1.0)),
                eq(0, band(peak, 1.0, -0.0, 1.0)),
            ),
            (
                eq(0, band(peak, 1.0, 1.0, 0.0)),
                eq(0, band(peak, 1.0, 1.0, -0.0)),
            ),
            (
                eq(0, band(peak, 1.0, 1.0, 1.0)),
                eq(0, band(BandType::LowShelf, 1.0, 1.0, 1.0)),
            ),
            (
                eq(0, band(peak, 1.0, 1.0, 1.0)),
                eq(1, band(peak, 1.0, 1.0, 1.0)),
            ),
            (
                gate(false, 1.0, 1.0, 1.0, 1.0),
                gate(true, 1.0, 1.0, 1.0, 1.0),
            ),
            (
                StripParam::DynamicsBeforeEq { enabled: false },
                StripParam::DynamicsBeforeEq { enabled: true },
            ),
            (comp(false, [1.0; 6]), comp(true, [1.0; 6])),
        ];
        for field in 0..4 {
            let mut x = [1.0; 4];
            x[field] = 0.0;
            let mut y = x;
            y[field] = -0.0;
            let [t, r, a, rel] = x;
            let [u, s, b, sel] = y;
            pairs.push((gate(true, t, r, a, rel), gate(true, u, s, b, sel)));
        }
        for field in 0..6 {
            let mut x = [1.0; 6];
            x[field] = 0.0;
            let mut y = x;
            y[field] = -0.0;
            pairs.push((comp(true, x), comp(true, y)));
        }
        pairs
    }

    /// Every field of every parameter takes part, by bit pattern: one
    /// field apart is different, an equal copy is the same.
    #[test]
    fn every_strip_field_compares_by_bit_pattern() {
        let mut seen = [false; 9];
        for (a, b) in one_field_apart() {
            seen[usize::from(strip_key(&a).0)] = true;
            assert!(same_bits(&a, &a.clone()), "{a:?}");
            assert!(same_bits(&b, &b.clone()), "{b:?}");
            assert!(!same_bits(&a, &b), "{a:?} {b:?}");
            assert!(!same_bits(&b, &a), "{b:?} {a:?}");
        }
        assert!(seen.iter().all(|&s| s), "a parameter without a pair");
        assert!(!same_bits(
            &StripParam::Trim { db: 0.0 },
            &StripParam::Eq { enabled: false }
        ));
    }

    /// `diff` before strip parameters were compared field by field: by
    /// their `Debug` forms, through a map per slot.
    fn reference_diff(was: &Targets, next: &Targets) -> Vec<Entry> {
        let keyed = |params: &[StripParam]| -> BTreeMap<(u8, usize), StripParam> {
            params.iter().map(|p| (strip_key(p), *p)).collect()
        };
        let same = |a: &StripParam, b: &StripParam| a == b && format!("{a:?}") == format!("{b:?}");
        let mut entries = Vec::new();
        for (input, row) in next.gains.iter().enumerate() {
            let held = was.gains.get(input);
            for (output, &gain) in row.iter().enumerate() {
                let held = held.and_then(|r| r.get(output));
                if held.is_none_or(|g| g.to_bits() != gain.to_bits()) {
                    entries.push(Entry::Gain {
                        input,
                        output,
                        gain,
                        fade: None,
                    });
                }
            }
        }
        for (slot, params) in next.strips.iter().enumerate() {
            let held = was.strips.get(slot).map(|p| keyed(p)).unwrap_or_default();
            for (key, param) in keyed(params) {
                if held.get(&key).is_none_or(|w| !same(w, &param)) {
                    entries.push(Entry::Strip { slot, param });
                }
            }
        }
        for (output, params) in next.limiters.iter().enumerate() {
            let held = was.limiters.get(output).copied().flatten();
            if let Some(params) = *params
                && held.is_none_or(|h| format!("{h:?}") != format!("{params:?}"))
            {
                entries.push(Entry::Limiter { output, params });
            }
        }
        for (slot, &samples) in next.delays.iter().enumerate() {
            if was.delays.get(slot) != Some(&samples) {
                entries.push(Entry::Delay { slot, samples });
            }
        }
        for (slot, flow) in next.flows.iter().enumerate() {
            if was.flows.get(slot) != Some(flow) {
                entries.push(match flow {
                    Some(flow_id) => Entry::Route {
                        slot,
                        flow_id: flow_id.clone(),
                    },
                    None => Entry::Detach { slot },
                });
            }
        }
        entries
    }

    /// A limiter's parameters get an entry when they differ in any
    /// field's bits, none when equal or not given, and every given one
    /// from `assert`.
    #[test]
    fn limiter_parameters_differ_by_bits() {
        let p = |enabled, ceiling_db| LimiterParams {
            enabled,
            ceiling_db,
            release_ms: 50.0,
        };
        let was = Targets {
            limiters: vec![Some(p(true, -1.0)), None, Some(p(true, -0.0))],
            ..Targets::default()
        };
        let next = Targets {
            limiters: vec![
                Some(p(true, -1.0)),
                Some(p(false, -1.0)),
                Some(p(true, 0.0)),
            ],
            ..Targets::default()
        };
        assert_eq!(
            was.diff(&next),
            [
                Entry::Limiter {
                    output: 1,
                    params: p(false, -1.0)
                },
                Entry::Limiter {
                    output: 2,
                    params: p(true, 0.0)
                },
            ]
        );
        assert_eq!(next.diff(&was).len(), 1, "output 1 none keeps the engine's");
        assert!(was.diff(&was).is_empty());
        let all = next.assert(&[], None);
        assert_eq!(
            all.iter()
                .filter(|e| matches!(e, Entry::Limiter { .. }))
                .count(),
            3
        );
    }

    /// A tone generator's parameters get an entry when they differ in any
    /// field's bits, none when equal or not given, and every given one
    /// from `assert`. Fails with the tones left out of `diff`.
    #[test]
    fn tone_parameters_differ_by_bits() {
        let p = |frequency_hz, level_db, ident| ToneParams {
            frequency_hz,
            level_db,
            ident,
        };
        let was = Targets {
            tones: vec![
                Some(p(1000, -18.0, false)),
                None,
                Some(p(1000, -0.0, false)),
            ],
            ..Targets::default()
        };
        let next = Targets {
            tones: vec![
                Some(p(1000, -18.0, false)),
                Some(p(997, -18.0, true)),
                Some(p(1000, 0.0, false)),
            ],
            ..Targets::default()
        };
        let tone = |slot, t: ToneParams| Entry::Tone {
            slot,
            frequency_hz: t.frequency_hz,
            level_db: t.level_db,
            ident: t.ident,
        };
        assert_eq!(
            was.diff(&next),
            [tone(1, p(997, -18.0, true)), tone(2, p(1000, 0.0, false))]
        );
        assert_eq!(next.diff(&was).len(), 1, "slot 1 none keeps the engine's");
        assert!(was.diff(&was).is_empty());
        let all = next.assert(&[], None);
        assert_eq!(
            all.iter()
                .filter(|e| matches!(e, Entry::Tone { .. }))
                .count(),
            3
        );
    }

    /// A slot's delay gets an entry when it differs, none when equal or
    /// past the list, and every listed one from `assert`.
    #[test]
    fn delays_differ_by_value() {
        let was = Targets {
            delays: vec![0, 480, 7],
            ..Targets::default()
        };
        let next = Targets {
            delays: vec![0, 960],
            ..Targets::default()
        };
        assert_eq!(
            was.diff(&next),
            [Entry::Delay {
                slot: 1,
                samples: 960
            }]
        );
        assert!(was.diff(&was).is_empty());
        let all = was.assert(&[], None);
        assert_eq!(
            all.iter()
                .filter(|e| matches!(e, Entry::Delay { .. }))
                .count(),
            3
        );
    }

    /// A xorshift generator, so a failing case reproduces.
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 % n as u64) as usize
        }

        fn value(&mut self) -> f32 {
            [0.0, -0.0, 1.0, -3.0, f32::NAN][self.below(5)]
        }

        fn param(&mut self) -> StripParam {
            let (leg, on) = (self.below(3), self.below(2) == 1);
            match self.below(9) {
                0 => StripParam::Trim { db: self.value() },
                1 => StripParam::LegTrim {
                    leg,
                    db: self.value(),
                },
                2 => StripParam::Polarity { leg, inverted: on },
                3 => StripParam::Hpf {
                    enabled: on,
                    freq: self.value(),
                },
                4 => StripParam::Eq { enabled: on },
                5 => StripParam::EqBand {
                    band: leg,
                    value: EqBand {
                        kind: [BandType::Peak, BandType::HighShelf][self.below(2)],
                        freq: self.value(),
                        gain_db: self.value(),
                        q: self.value(),
                    },
                },
                6 => StripParam::Gate(GateParams {
                    enabled: on,
                    threshold_db: self.value(),
                    range_db: self.value(),
                    attack_ms: self.value(),
                    release_ms: self.value(),
                }),
                7 => StripParam::Comp(CompParams {
                    enabled: on,
                    threshold_db: self.value(),
                    ratio: self.value(),
                    attack_ms: self.value(),
                    release_ms: self.value(),
                    knee_db: self.value(),
                    makeup_db: self.value(),
                }),
                _ => StripParam::DynamicsBeforeEq { enabled: on },
            }
        }

        /// Strip lists in key order, out of it and with repeated keys.
        fn targets(&mut self) -> Targets {
            let slots = self.below(4);
            let mut strips: Vec<Vec<StripParam>> = (0..slots)
                .map(|_| (0..self.below(12)).map(|_| self.param()).collect())
                .collect();
            for params in &mut strips {
                if self.below(2) == 0 {
                    params.sort_by_key(strip_key);
                    params.dedup_by_key(|p| strip_key(p));
                }
            }
            Targets {
                gains: (0..self.below(4))
                    .map(|_| (0..self.below(4)).map(|_| self.value()).collect())
                    .collect(),
                flows: (0..self.below(4))
                    .map(|_| [Some("a"), Some("b"), None][self.below(3)].map(Into::into))
                    .collect(),
                strips,
                limiters: (0..self.below(3))
                    .map(|_| {
                        (self.below(3) > 0).then(|| LimiterParams {
                            enabled: self.below(2) == 1,
                            ceiling_db: self.value(),
                            release_ms: self.value(),
                        })
                    })
                    .collect(),
                delays: (0..self.below(3))
                    .map(|_| self.below(3) as u32 * 100)
                    .collect(),
                ..Targets::default()
            }
        }
    }

    /// The same entries in the same order as the comparison by `Debug`
    /// forms, over random targets and over a target and its own edit.
    #[test]
    fn diff_matches_the_reference_on_random_targets() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for _ in 0..20_000 {
            let was = rng.targets();
            let mut next = if rng.below(2) == 0 {
                rng.targets()
            } else {
                was.clone()
            };
            if let Some(params) = next.strips.first_mut() {
                params.push(rng.param());
            }
            // By `Debug` forms, as a NaN gain or field never equals itself.
            assert_eq!(
                format!("{:?}", was.diff(&next)),
                format!("{:?}", reference_diff(&was, &next)),
                "{was:?} -> {next:?}"
            );
        }
    }

    /// Ended, `keyed` stays ended, in list order and by selection alike.
    #[test]
    fn keyed_stays_ended() {
        let ascending = [
            StripParam::Trim { db: 1.0 },
            StripParam::Eq { enabled: true },
        ];
        let unordered = [
            StripParam::Eq { enabled: true },
            StripParam::Trim { db: 1.0 },
        ];
        for params in [&ascending, &unordered] {
            let mut it = keyed(params);
            assert_eq!(it.by_ref().count(), 2);
            assert!(it.next().is_none());
            assert!(it.next().is_none());
        }
    }

    fn duck() -> DuckTargets {
        DuckTargets {
            keys: vec![(0, 1.0)],
            depths: vec![0.0, 12.0],
            ..DuckTargets::default()
        }
    }

    /// Duck settings are compared field by field by bit pattern, each
    /// setting its own entry, depths by slot, masks by coefficient; a
    /// group or mask row new to the targets is named whole.
    #[test]
    fn duck_settings_and_masks_compare_by_bit_pattern() {
        use samplez_dsp::DuckMode;
        let mut base = targets();
        base.ducks = vec![duck()];
        base.masks = vec![vec![0, 1], vec![0, 0]];
        assert_eq!(base.diff(&base.clone()), Vec::new());
        let mut next = base.clone();
        next.ducks[0].params.attack_ms = 10.0;
        next.ducks[0].keys[0].1 = 0.5;
        next.ducks[0].mode = DuckMode::On;
        next.ducks[0].held = true;
        next.ducks[0].depths[0] = -0.0;
        next.masks[1][0] = 3;
        let group = |setting| Entry::Duck { group: 0, setting };
        assert_eq!(
            base.diff(&next),
            vec![
                group(DuckSetting::Detector(Box::new(next.ducks[0].params))),
                group(DuckSetting::Keys(vec![(0, 0.5)])),
                group(DuckSetting::Mode(DuckMode::On)),
                group(DuckSetting::Hold { held: true }),
                group(DuckSetting::Depth { slot: 0, db: -0.0 }),
                Entry::DuckMask {
                    input: 1,
                    output: 0,
                    mask: 3
                },
            ]
        );
        // Every field of the parameters takes part.
        for field in 0..9 {
            let mut p = DetectorParams::DEFAULT;
            match field {
                0 => p.threshold_db = -29.0,
                1 => p.hysteresis_db = 3.0,
                2 => p.hpf.enabled = false,
                3 => p.hpf.freq = 121.0,
                4 => p.lpf.enabled = true,
                5 => p.lpf.freq = 4001.0,
                6 => p.attack_ms = 21.0,
                7 => p.hold_ms = 501.0,
                _ => p.release_ms = 1001.0,
            }
            assert!(!same_params(&DetectorParams::DEFAULT, &p), "field {field}");
        }
        let mut grown = base.clone();
        grown.ducks.push(DuckTargets::default());
        assert_eq!(base.diff(&grown).len(), 4, "a new group: every setting");
        let mut keyed = base.clone();
        keyed.ducks[0].keys.push((1, 1.0));
        assert_eq!(
            base.diff(&keyed),
            vec![group(DuckSetting::Keys(vec![(0, 1.0), (1, 1.0)]))]
        );
    }

    /// Asserting sets every duck setting and every mask other than 0,
    /// and a 0 where an engine may hold another mask: every coefficient
    /// without `masked`, those it names with it.
    #[test]
    fn assert_sets_every_duck_setting_and_clears_only_masks_given() {
        let mut t = targets();
        t.ducks = vec![duck(), duck()];
        t.masks = vec![vec![0, 1], vec![0, 0]];
        let masks = |entries: &[Entry]| -> Vec<(usize, usize, u8)> {
            (entries.iter())
                .filter_map(|e| match *e {
                    Entry::DuckMask {
                        input,
                        output,
                        mask,
                    } => Some((input, output, mask)),
                    _ => None,
                })
                .collect()
        };
        let entries = t.assert(&[], None);
        let ducks = entries
            .iter()
            .filter(|e| matches!(e, Entry::Duck { .. }))
            .count();
        // Parameters, keys, mode, hold and two depths per group.
        assert_eq!(ducks, 12);
        assert_eq!(
            masks(&entries),
            [(0, 1, 1), (0, 0, 0), (1, 0, 0), (1, 1, 0)]
        );
        let masked = [vec![0, 4], vec![2, 0]];
        assert_eq!(masks(&t.assert(&[], Some(&masked))), [(0, 1, 1), (1, 0, 0)]);
    }

    /// A diff names a mask only where it changes: a row new to the
    /// targets names only its masks other than 0, as an engine holds 0
    /// where no entry set one.
    #[test]
    fn a_diff_names_only_masks_that_change() {
        let mut next = targets();
        next.masks = vec![vec![0, 3], vec![0, 0]];
        assert_eq!(
            targets().diff(&next),
            vec![Entry::DuckMask {
                input: 0,
                output: 1,
                mask: 3
            }]
        );
        let mut cleared = next.clone();
        cleared.masks[0][1] = 0;
        assert_eq!(
            next.diff(&cleared),
            vec![Entry::DuckMask {
                input: 0,
                output: 1,
                mask: 0
            }]
        );
    }

    /// A diff and an assert give each gain the absolute fade of `next`,
    /// unchanged: one source, whatever the index they apply at.
    #[test]
    fn diff_and_assert_carry_the_absolute_fade() {
        let f = Fade {
            from: 1.0,
            start: 1000,
            len: 400,
        };
        let mut next = targets();
        next.gains[1][0] = 0.125;
        next.fading = vec![vec![None, None], vec![Some(f), None]];
        let want = Entry::Gain {
            input: 1,
            output: 0,
            gain: 0.125,
            fade: Some(f),
        };
        assert_eq!(targets().diff(&next), vec![want.clone()]);
        assert!(next.assert(&[], None).contains(&want));
        let mut plain = next.clone();
        plain.fading = Vec::new();
        let none = Entry::Gain {
            input: 1,
            output: 0,
            gain: 0.125,
            fade: None,
        };
        assert_eq!(targets().diff(&plain), vec![none]);
    }

    /// A fade changed, set or cleared while the gain keeps its bits is an
    /// entry too: peers and a rebuild would otherwise hold different
    /// ramps until the old one ended.
    #[test]
    fn a_changed_fade_alone_is_an_entry() {
        let f = Fade {
            from: 1.0,
            start: 10,
            len: 20,
        };
        let with = |fade| {
            let mut t = targets();
            t.fading = vec![vec![fade, None], vec![None, None]];
            t
        };
        let entry = |fade| Entry::Gain {
            input: 0,
            output: 0,
            gain: 0.5,
            fade,
        };
        assert_eq!(with(None).diff(&with(Some(f))), vec![entry(Some(f))]);
        assert_eq!(with(Some(f)).diff(&with(None)), vec![entry(None)]);
        let g = Fade { len: 21, ..f };
        assert_eq!(with(Some(f)).diff(&with(Some(g))), vec![entry(Some(g))]);
        assert_eq!(with(Some(f)).diff(&with(Some(f))), Vec::new());
    }
}

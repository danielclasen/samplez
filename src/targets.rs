//! Absolute engine targets and the entries between two of them: a product
//! that holds its engine's targets as values gives the engine only the
//! entries that move it from one set of targets to the next.

use samplez_dsp::{CompParams, EqBand, GateParams, StripParam};

use crate::batch::strip_key;
use crate::engine::Entry;

/// Every target an engine's log sets, as values.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Targets {
    /// The gain targets: one row per input channel, one column per output
    /// or meter column.
    pub gains: Vec<Vec<f32>>,
    /// Per input slot, the flow it reads.
    pub flows: Vec<String>,
    /// Per input slot, its strip parameters; of two with one key (one
    /// band, one leg, the gate), the later counts.
    pub strips: Vec<Vec<StripParam>>,
}

impl Targets {
    /// The entries that move an engine holding `self` to `next`: a `Gain`
    /// for each coefficient whose bit pattern differs, row by row; a
    /// `Strip` for each parameter that differs in any field's bit
    /// pattern, by slot and parameter; a
    /// `Route` for each slot whose flow differs. A target `next` holds and
    /// `self` does not counts as different; one `self` holds and `next`
    /// does not gets no entry, so the engine keeps it.
    pub fn diff(&self, next: &Targets) -> Vec<Entry> {
        let mut entries = Vec::new();
        for (input, row) in next.gains.iter().enumerate() {
            let was = self.gains.get(input);
            for (output, &gain) in row.iter().enumerate() {
                let held = was.and_then(|r| r.get(output));
                if held.is_none_or(|g| g.to_bits() != gain.to_bits()) {
                    entries.push(Entry::Gain {
                        input,
                        output,
                        gain,
                    });
                }
            }
        }
        for (slot, params) in next.strips.iter().enumerate() {
            let mut was = keyed(self.strips.get(slot).map_or(&[], Vec::as_slice)).peekable();
            for (key, &param) in keyed(params) {
                while was.next_if(|&(k, _)| k < key).is_some() {}
                let held = was.next_if(|&(k, _)| k == key);
                if held.is_none_or(|(_, w)| !same_bits(w, &param)) {
                    entries.push(Entry::Strip { slot, param });
                }
            }
        }
        for (slot, flow) in next.flows.iter().enumerate() {
            if self.flows.get(slot) != Some(flow) {
                entries.push(Entry::Route {
                    slot,
                    flow_id: flow.clone(),
                });
            }
        }
        entries
    }

    /// The entries that set every gain and strip parameter of `self`,
    /// whatever an engine holds, in the order of [`Targets::diff`], and a
    /// `Route` for each of `slots` in ascending order.
    pub fn assert(&self, slots: &[usize]) -> Vec<Entry> {
        // Against no targets at all: every gain, zeros included, and every
        // strip parameter.
        let mut entries = Targets::default().diff(&Targets {
            gains: self.gains.clone(),
            flows: Vec::new(),
            strips: self.strips.clone(),
        });
        let mut slots = slots.to_vec();
        slots.sort_unstable();
        slots.dedup();
        for slot in slots {
            if let Some(flow) = self.flows.get(slot) {
                entries.push(Entry::Route {
                    slot,
                    flow_id: flow.clone(),
                });
            }
        }
        entries
    }
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
/// without allocating: in list order when the keys already ascend, as a
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
        last = least.map(|(k, _)| k);
        least
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
            flows: vec!["a".into(), "b".into()],
            strips: vec![vec![StripParam::Trim { db: -3.0 }], Vec::new()],
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
        next.flows[0] = "c".into();
        assert_eq!(
            targets().diff(&next),
            vec![
                Entry::Gain {
                    input: 0,
                    output: 1,
                    gain: 0.75
                },
                Entry::Gain {
                    input: 1,
                    output: 0,
                    gain: 0.125
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
                gain: -0.0
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
            flows: Vec::new(),
            strips: Vec::new(),
        };
        assert_eq!(targets().diff(&next), Vec::new());
    }

    /// Asserting sets every gain, zeros included, and every strip
    /// parameter, and routes only the slots named.
    #[test]
    fn assert_sets_every_gain_and_parameter_and_the_routes_named() {
        let t = targets();
        let entries = t.assert(&[1, 1]);
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
        assert_eq!(t.assert(&[]).len(), 5);
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
        for (slot, flow) in next.flows.iter().enumerate() {
            if was.flows.get(slot) != Some(flow) {
                entries.push(Entry::Route {
                    slot,
                    flow_id: flow.clone(),
                });
            }
        }
        entries
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
                    .map(|_| ["a", "b"][self.below(2)].into())
                    .collect(),
                strips,
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
}

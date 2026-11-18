//! Absolute engine targets and the entries between two of them: a product
//! that holds its engine's targets as values gives the engine only the
//! entries that move it from one set of targets to the next.

use std::collections::BTreeMap;

use samplez_dsp::StripParam;

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
            let was = self.strips.get(slot).map(|p| keyed(p)).unwrap_or_default();
            for (key, param) in keyed(params) {
                if was.get(&key).is_none_or(|w| !same_bits(w, &param)) {
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

/// Whether `a` and `b` hold the same bit pattern in every field. Their
/// `Debug` forms tell every pair of `f32` bit patterns apart but NaNs,
/// which no parameter an engine takes holds: -0.0 and 0.0 differ there as
/// they do in a core copy.
fn same_bits(a: &StripParam, b: &StripParam) -> bool {
    a == b && format!("{a:?}") == format!("{b:?}")
}

/// `params` by key, the later of two with one key counting.
fn keyed(params: &[StripParam]) -> BTreeMap<(u8, usize), StripParam> {
    params.iter().map(|p| (strip_key(p), *p)).collect()
}

#[cfg(test)]
mod tests {
    use samplez_dsp::{BandType, EqBand, GateParams};

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
}

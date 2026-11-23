use samplez::{Entry, Targets};
use samplez_dsp::StripParam;
use serde::{Deserialize, Serialize};

use super::tests::{command, origin};
use super::*;

/// Two input slots of one channel into one output: each coefficient is
/// the slot's fader times the VCA, slot 1 silent while a session holds
/// solo on slot 0. Commands: `fader <slot> <gain>`, `vca <gain>`,
/// `trim <slot> <db>`, `route <slot> <flow>`, `pair <flow> <flow>`
/// (both slots), `pause <meter> <0|1>`, `reset <meter>`, `solo`,
/// `noop`, `stale`.
/// The input domain of each slot, as configured.
const DOMAINS: [u8; 2] = [0, 1];

#[derive(Serialize, Deserialize)]
struct Desk {
    faders: [f64; 2],
    vca: f64,
    trims: [f32; 2],
    flows: [String; 2],
    solo: Option<SessionKey>,
    /// Slot 1 ducked by group 0 on its coefficient.
    #[serde(default)]
    ducked: bool,
    /// Per meter, its loudness measurement paused.
    #[serde(default)]
    paused: [bool; 2],
    /// Per meter, its loudness reset epoch.
    #[serde(default)]
    resets: [u32; 2],
    /// Slot 0's tone parameters, once a command set them.
    #[serde(default)]
    tone: Option<(u32, f32, bool)>,
}

impl Desk {
    fn new() -> Desk {
        Desk {
            faders: [1.0, 1.0],
            vca: 1.0,
            trims: [0.0, 0.0],
            flows: ["a".into(), "b".into()],
            solo: None,
            ducked: false,
            paused: [false; 2],
            resets: [0; 2],
            tone: None,
        }
    }
}

impl Product for Desk {
    const DERIVATION_VERSION: u32 = 1;
    type Change = ();
    const DRIVES_ENGINE: bool = true;

    fn apply(&mut self, origin: &Origin, command: &[u8], ctx: &Apply) -> Effect<()> {
        let text = std::str::from_utf8(command).expect("utf-8");
        let words: Vec<&str> = text.split(' ').collect();
        match words[..] {
            ["fader", slot, gain] => {
                self.faders[slot.parse::<usize>().unwrap()] = gain.parse().unwrap();
            }
            ["vca", gain] => self.vca = gain.parse().unwrap(),
            ["duck", on] => self.ducked = on == "1",
            ["pause", meter, on] => self.paused[meter.parse::<usize>().unwrap()] = on == "1",
            ["reset", meter] => {
                let epoch = &mut self.resets[meter.parse::<usize>().unwrap()];
                *epoch = epoch.wrapping_add(1);
            }
            ["tone", hz, db, ident] => {
                self.tone = Some((hz.parse().unwrap(), db.parse().unwrap(), ident == "1"));
            }
            ["trim", slot, db] => self.trims[slot.parse::<usize>().unwrap()] = db.parse().unwrap(),
            // Ordered off the generation path, or across two input
            // domains: refused.
            ["route", ..] | ["pair", ..] if ctx.generation == Some(false) => {
                return Effect::Unchanged;
            }
            ["pair", ..] if DOMAINS[0] != DOMAINS[1] => return Effect::Unchanged,
            ["pair", a, b] => self.flows = [a.into(), b.into()],
            ["route", slot, flow] => self.flows[slot.parse::<usize>().unwrap()] = flow.into(),
            ["solo"] => self.solo = Some(origin.key()),
            ["noop"] => return Effect::Unchanged,
            _ => return Effect::Stale,
        }
        Effect::Changed(())
    }
    fn expire(&mut self, session: &SessionKey) -> Effect<()> {
        if self.solo.as_ref() != Some(session) {
            return Effect::Unchanged;
        }
        self.solo = None;
        Effect::Changed(())
    }
    fn forget(&mut self, _: &SessionKey) {}
    fn delta(&self, _: u64, _: &[Changed<'_, ()>]) -> String {
        String::new()
    }
    fn text(&self) -> String {
        String::new()
    }
    fn save(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("encodes")
    }
    fn load(&mut self, saved: &[u8]) -> Result<(), String> {
        *self = serde_json::from_slice(saved).map_err(|e| e.to_string())?;
        Ok(())
    }
    fn config_hash(&self) -> u64 {
        0
    }
    fn targets(&self) -> Option<Targets> {
        let gain = |slot: usize| {
            let muted = slot == 1 && self.solo.is_some();
            if muted {
                0.0
            } else {
                (self.faders[slot] * self.vca) as f32
            }
        };
        Some(Targets {
            gains: vec![vec![gain(0)], vec![gain(1)]],
            flows: self.flows.iter().cloned().map(Some).collect(),
            strips: self
                .trims
                .iter()
                .map(|&db| vec![StripParam::Trim { db }])
                .collect(),
            masks: vec![vec![0], vec![u8::from(self.ducked)]],
            pre_strip_meters: Vec::new(),
            // Listed only while a meter is paused.
            loudness_running: if self.paused.contains(&true) {
                self.paused.iter().map(|&p| !p).collect()
            } else {
                Vec::new()
            },
            loudness_resets: self.resets.to_vec(),
            // Listed only once set.
            tones: (self.tone.iter())
                .map(|&(frequency_hz, level_db, ident)| {
                    Some(samplez_dsp::ToneParams {
                        frequency_hz,
                        level_db,
                        ident,
                    })
                })
                .collect(),
            ..Targets::default()
        })
    }
    fn reroutes(&self, _: &Origin, command: &[u8]) -> Vec<u32> {
        let text = std::str::from_utf8(command).expect("utf-8");
        match text.split(' ').collect::<Vec<_>>()[..] {
            ["route", slot, _] => vec![slot.parse().unwrap()],
            ["pair", ..] => vec![0, 1],
            _ => Vec::new(),
        }
    }
}

fn machine() -> StateMachine<Desk> {
    StateMachine::new(Desk::new(), Limits::defaults(48_000))
}

/// Targets of one coefficient at 0.5 on flow `a`, unmasked, slot 0
/// ducked 10 dB by group 0.
fn held() -> Targets {
    Targets {
        gains: vec![vec![0.5]],
        flows: vec![Some("a".to_owned()), Some("b".to_owned())],
        ducks: vec![samplez::DuckTargets {
            depths: vec![10.0],
            ..samplez::DuckTargets::default()
        }],
        masks: vec![vec![0]],
        ..Targets::default()
    }
}

/// A fade started before the index its entry applies at is tracked
/// as the engine joins it, from the value the coefficient holds
/// there; one starting at its index as it is.
#[test]
fn the_tracker_joins_a_late_fade() {
    let mut sm = machine();
    let f = samplez::Fade {
        from: 0.2,
        start: 4800,
        len: 9600,
    };
    let gain = |fade| Entry::Gain {
        input: 0,
        output: 0,
        gain: 1.0,
        fade: Some(fade),
    };
    sm.follow(&[gain(f)], Some(&held()), 9600);
    let joined = f.joined(1.0, 0.5, 9600, 480);
    assert_ne!(joined, Some(f));
    assert_eq!(sm.ramps[0][0], joined);
    let on_time = samplez::Fade { start: 19_200, ..f };
    sm.follow(&[gain(on_time)], Some(&held()), 19_200);
    assert_eq!(sm.ramps[0][0], Some(on_time));
}

/// A depth setting ramps the slot's depth from the value it has at
/// the setting's index over the ramp length, a ramp still running
/// included, the value as the bridge admits it.
#[test]
fn the_tracker_follows_the_depth_ramps() {
    let mut sm = machine();
    let depth = |db| Entry::Duck {
        group: 0,
        setting: samplez::DuckSetting::Depth { slot: 0, db },
    };
    sm.follow(&[depth(30.0)], Some(&held()), 9600);
    let first = samplez::Fade {
        from: 10.0,
        start: 9600,
        len: 480,
    };
    assert_eq!(sm.in_flight.depths, vec![vec![Some(first)]]);
    let mut now = held();
    now.ducks[0].depths[0] = 30.0;
    // Clamped to 40 dB, from where the first ramp is.
    sm.follow(&[depth(99.0)], Some(&now), 9696);
    let second = samplez::Fade {
        from: first.value_at(30.0, 9696),
        start: 9696,
        len: 480,
    };
    assert_eq!(sm.in_flight.depths, vec![vec![Some(second)]]);
}

/// A mask change joins the coefficient's changes while the last of
/// them is within the ramp and two gain intervals, starts new ones
/// after that, and is no crossfade on a coefficient at 0.
#[test]
fn the_tracker_follows_the_mask_crossfades() {
    let mut sm = machine();
    let mask = |mask| Entry::DuckMask {
        input: 0,
        output: 0,
        mask,
    };
    let mut now = held();
    sm.follow(&[mask(1)], Some(&now), 9600);
    now.masks[0][0] = 1;
    sm.follow(&[mask(0)], Some(&now), 9696);
    now.masks[0][0] = 0;
    assert_eq!(
        sm.in_flight.masks,
        vec![(0, 0, 0, vec![(9600, 1), (9696, 0)])]
    );
    sm.follow(&[mask(1)], Some(&now), 20_016);
    now.masks[0][0] = 1;
    assert_eq!(sm.in_flight.masks, vec![(0, 0, 0, vec![(20_016, 1)])]);
    now.gains[0][0] = 0.0;
    sm.follow(&[mask(0)], Some(&now), 40_032);
    assert!(sm.in_flight.masks.is_empty());
}

/// An `InputUp` that raises a slot from down starts its fade-in, one
/// for a slot up starts none; a snapshot carries which slots are down.
#[test]
fn the_tracker_follows_the_input_fade_ins() {
    let mut sm = machine();
    let (down, up) = (
        |slot| Entry::InputDown { slot },
        |slot| Entry::InputUp { slot },
    );
    sm.follow_inputs(&[up(0)], 4800, 1, None);
    assert_eq!(sm.in_flight.fade_ins, vec![None]);
    sm.follow_inputs(&[down(1)], 4800, 2, None);
    let mut loaded = machine();
    loaded
        .load(sm.save(), Limits::defaults(48_000))
        .expect("loads");
    for m in [&mut sm, &mut loaded] {
        m.follow_inputs(&[up(1)], 9600, 3, None);
        assert_eq!(m.in_flight.fade_ins, vec![None, Some(9600)]);
    }
}

/// A restamp moves a fade-in only where the entry it moves started
/// it: a moved `InputUp` that raised nothing, its slot up already,
/// leaves the fade-in an earlier one started where it is, and one that
/// raised the slot moves its own fade-in to the restamp's index, also
/// restamped again and after a snapshot load.
#[test]
fn a_restamp_moves_only_the_fade_in_its_entry_started() {
    let mut sm = machine();
    let (down, up) = (
        |slot| Entry::InputDown { slot },
        |slot| Entry::InputUp { slot },
    );
    sm.follow_inputs(&[down(0)], 4800, 1, None);
    sm.follow_inputs(&[up(0)], 9600, 2, None);
    // Up already: raises nothing.
    sm.follow_inputs(&[up(0)], 9648, 3, None);
    assert_eq!(sm.in_flight.fade_ins, vec![Some(9600)]);
    // Entry 3 restamped as 4.
    sm.follow_inputs(&[up(0)], 9696, 4, Some(3));
    assert_eq!(sm.in_flight.fade_ins, vec![Some(9600)]);
    // Entry 2 restamped as 5, then 5 as 6 after a load.
    sm.follow_inputs(&[up(0)], 9744, 5, Some(2));
    assert_eq!(sm.in_flight.fade_ins, vec![Some(9744)]);
    let mut loaded = machine();
    loaded
        .load(sm.save(), Limits::defaults(48_000))
        .expect("loads");
    loaded.follow_inputs(&[up(0)], 9792, 6, Some(5));
    assert_eq!(loaded.in_flight.fade_ins, vec![Some(9792)]);
}

/// A void of an `InputUp` that raised a slot, which no engine
/// applies, leaves the slot down and its fade-in as before it: no
/// fade-in recorded, and the next `InputUp` raises the slot.
#[test]
fn a_voided_input_up_leaves_the_slot_as_before_it() {
    let mut sm = machine();
    let request = |body| Request { stamp: 0, body };
    let up = |at_index| Body::InputUp { slot: 0, at_index };
    sm.apply_at(
        1,
        1,
        &request(Body::InputDown {
            slot: 0,
            at_index: 4800,
        }),
    );
    sm.apply_at(2, 1, &request(up(9600)));
    assert_eq!(sm.in_flight.fade_ins, vec![Some(9600)]);
    let void = Body::Void {
        revision: 2,
        term: 1,
        at_index: None,
        generation: None,
    };
    sm.apply_at(3, 1, &request(void));
    assert_eq!(sm.in_flight.fade_ins, vec![None]);
    sm.apply_at(4, 1, &request(up(19_200)));
    assert_eq!(sm.in_flight.fade_ins, vec![Some(19_200)]);
}

/// A re-route or detach that changes the flow a slot reads starts a
/// crossfade from that flow; one to the flow read starts none.
#[test]
fn the_tracker_follows_the_route_crossfades() {
    let mut sm = machine();
    let route = |slot, flow: &str| Entry::Route {
        slot,
        flow_id: flow.to_owned(),
    };
    sm.follow(&[route(0, "a"), route(1, "c")], Some(&held()), 9600);
    let from_b = Some((9600, Some("b".to_owned())));
    assert_eq!(sm.in_flight.crossfades, vec![None, from_b]);
    sm.follow(&[Entry::Detach { slot: 0 }], Some(&held()), 19_200);
    let from_a = Some((19_200, Some("a".to_owned())));
    assert_eq!(sm.in_flight.crossfades[0], from_a);
}

/// A `Commands` entry of one command `body` of session `s`, stamped
/// `stamp`, asking for `at`, flagged `generation`.
fn one(id: &str, body: &str, stamp: u64, at: u64, generation: Option<bool>) -> Request {
    let mut c = command("s", id, body);
    c.at_index = Some(at);
    c.generation = generation;
    Request {
        stamp,
        body: Body::Commands(vec![c]),
    }
}

fn gains(r: &Record) -> Vec<(usize, usize, u32)> {
    r.entries
        .iter()
        .filter_map(|e| match *e {
            Entry::Gain {
                input,
                output,
                gain,
                ..
            } => Some((input, output, gain.to_bits())),
            _ => None,
        })
        .collect()
}

/// A command pausing a meter derives its `LoudnessRun`, and one
/// running it again another, at the command's index; the product state
/// carries the meter paused, so a rebuilt engine pauses it too.
#[test]
fn a_meter_paused_by_a_command_derives_its_entry_and_state() {
    let mut sm = machine();
    sm.apply_at(1, 1, &one("1", "pause 1 1", 0, 960, None));
    let run = |running| Entry::LoudnessRun { meter: 1, running };
    assert_eq!(sm.records[&1].entries, [run(false)]);
    assert_eq!(sm.records[&1].at_index, 960);
    assert_eq!(sm.product_state(1).loudness_running, [true, false]);
    sm.apply_at(2, 1, &one("2", "pause 1 0", 48, 1008, None));
    assert_eq!(sm.records[&2].entries, [run(true)]);
    assert!(sm.product_state(2).loudness_running.is_empty());
}

/// A restamp of entry `of` to `at_index`, stamped 96.
fn restamp_of(of: u64, at_index: u64) -> Request {
    Request {
        stamp: 96,
        body: Body::Restamp {
            revision: of,
            term: 1,
            at_index,
            paths: Vec::new(),
            generation: None,
        },
    }
}

/// The loudness resets of `sm`'s records, as (log index, record
/// index, meter), but for the records of the entries `decided` names:
/// the log restamped or voided those, so their records never apply.
fn resets_applied(sm: &StateMachine<Desk>, decided: &[u64]) -> Vec<(u64, u64, usize)> {
    (sm.records.iter())
        .filter(|(i, _)| !decided.contains(i))
        .flat_map(|(&i, r)| {
            (r.entries.iter()).filter_map(move |e| match *e {
                Entry::ResetLoudness { meter } => Some((i, r.at_index, meter)),
                _ => None,
            })
        })
        .collect()
}

/// A command moving a meter's reset epoch derives one reset at its
/// index. Restamped, and the restamp restamped again, the reset
/// moves with it: exactly one applies, at the last restamp's index,
/// though the restamps re-assert the targets, which hold no reset. A
/// later command derives none.
#[test]
fn a_restamped_reset_applies_once_at_the_restamp() {
    let mut sm = machine();
    sm.apply_at(10, 1, &one("1", "reset 1", 0, 3000, None));
    assert_eq!(resets_applied(&sm, &[]), [(10, 3000, 1)]);
    sm.apply_at(11, 1, &one("2", "fader 0 0.2", 0, 3000, None));
    sm.apply_at(12, 1, &restamp_of(10, 4000));
    assert_eq!(resets_applied(&sm, &[10]), [(12, 4000, 1)]);
    sm.apply_at(13, 1, &restamp_of(12, 5000));
    assert_eq!(resets_applied(&sm, &[10, 12]), [(13, 5000, 1)]);
    sm.apply_at(14, 1, &one("3", "fader 1 0.5", 96, 6000, None));
    assert_eq!(resets_applied(&sm, &[10, 12]), [(13, 5000, 1)]);
}

/// A command moving a meter's reset epoch that the log voids resets
/// nothing: the void re-asserts the state, which holds no reset, and
/// so does a restamp of the stamped void.
#[test]
fn a_voided_reset_applies_nowhere() {
    let mut sm = machine();
    sm.apply_at(10, 1, &one("1", "reset 0", 0, 3000, None));
    sm.apply_at(11, 1, &void(10, 96, Some(4800)));
    assert!(
        sm.records.contains_key(&11),
        "the void re-asserts the state"
    );
    assert!(resets_applied(&sm, &[10]).is_empty());
    sm.apply_at(12, 1, &restamp_of(11, 6000));
    assert!(sm.records.contains_key(&12), "the restamp re-asserts it");
    assert!(resets_applied(&sm, &[10, 11]).is_empty());
}

/// Two replicas applying one log derive the same resets at the same
/// indexes, one of them loaded from a snapshot taken after the reset
/// commands and before the log decided them: a restamped reset
/// applies at the restamp on both, a voided one on neither.
#[test]
fn replicas_derive_the_same_resets_from_one_log() {
    let log = [
        one("1", "reset 1", 0, 3000, None),
        one("2", "reset 0", 48, 3000, None),
        one("3", "fader 0 0.2", 96, 3000, None),
        restamp_of(1, 4000),
        void(2, 144, Some(4800)),
        one("4", "reset 1", 192, 5000, None),
    ];
    let mut whole = machine();
    for (n, r) in log.iter().enumerate().take(3) {
        whole.apply_at(n as u64 + 1, 1, r);
    }
    let mut loaded = machine();
    loaded.load(whole.save(), Limits::defaults(48_000)).unwrap();
    for (n, r) in log.iter().enumerate().skip(3) {
        whole.apply_at(n as u64 + 1, 1, r);
        loaded.apply_at(n as u64 + 1, 1, r);
    }
    let applied = |sm: &StateMachine<Desk>| -> Vec<(u64, u64, usize)> {
        (resets_applied(sm, &[1, 2]).into_iter())
            .filter(|&(i, ..)| i > 3)
            .collect()
    };
    assert_eq!(applied(&whole), [(4, 4000, 1), (6, 5000, 1)]);
    assert_eq!(applied(&loaded), applied(&whole));
}

/// The product state carries the ramp each plain gain entry left the
/// coefficient on, from the value the engine holds at the entry's
/// index, a ramp still running then included, so a rebuilt engine
/// continues them.
#[test]
fn the_product_state_carries_the_ramps_of_plain_gain_entries() {
    let mut sm = machine();
    let g0 = sm.product_state(0).gains[0][0];
    let ramp = Limits::defaults(48_000).ramp_samples;
    sm.apply_at(1, 1, &one("1", "fader 0 0.5", 0, 960, None));
    let first = samplez::Fade {
        from: g0,
        start: 960,
        len: ramp,
    };
    assert_eq!(sm.product_state(1).fading[0][0], Some(first));
    // The second entry lands inside the first one's ramp.
    sm.apply_at(2, 1, &one("2", "fader 0 0.75", 48, 1008, None));
    let second = samplez::Fade {
        from: first.value_at(0.5, 1008),
        start: 1008,
        len: ramp,
    };
    assert_ne!(second.from.to_bits(), 0.5f32.to_bits());
    assert_eq!(sm.product_state(2).fading[0][0], Some(second));
}

/// The product state carries the ramps the derived tone entries leave
/// slot 0's generator on, as a generator that took the same entries at
/// the block-aligned indices the bridge applies them at holds them: a
/// change inside the frequency crossfade waiting, and parameters out
/// of range taken as the bridge admits them, a level of +6 dBFS and a
/// frequency of 5 Hz clamped to 0 dBFS and 20 Hz, a NaN level the
/// default. A snapshot carries them, and one written before they were
/// kept loads with none. Fails with `.admitted()` dropped from
/// `follow_tone`, or with `tone_ramps: Vec::new()` in `save`.
#[test]
fn the_product_state_carries_the_tone_ramps() {
    let mut sm = machine();
    assert!(sm.product_state(0).tone_ramps.is_empty());
    let limits = Limits::defaults(48_000);
    let p = |frequency_hz, level_db, ident| samplez_dsp::ToneParams {
        frequency_hz,
        level_db,
        ident,
    };
    // (command, index, what the bridge applies)
    let log = [
        ("tone 440 -6 0", 970, p(440, -6.0, false)),
        ("tone 1000 -12 1", 1010, p(1000, -12.0, true)),
        ("tone 5 6 1", 1700, p(20, 0.0, true)),
        ("tone 440 NaN 0", 2500, samplez_dsp::ToneParams::DEFAULT),
        // Finite again, so the product saves and loads.
        ("tone 997 -3 1", 4000, p(997, -3.0, true)),
    ];
    let mut want = samplez_dsp::ToneRamps::default();
    for (n, &(body, at, applied)) in log.iter().enumerate() {
        let id = (n + 1).to_string();
        sm.apply_at(n as u64 + 1, 1, &one(&id, body, 48 * n as u64, at, None));
        let at = u64::next_multiple_of(at, limits.block);
        assert!(want.set(at, applied, limits.ramp_samples));
        if n == 2 {
            // At 1728 the crossfade to 1 kHz runs from 1488, the end
            // of the one to 440 Hz it waited for, and 20 Hz waits
            // behind it.
            let f = want.frequency;
            assert_eq!((f.from, f.to, f.start, f.next), (440, 1000, 1488, Some(20)));
        }
        let state = sm.product_state(n as u64 + 1);
        assert_eq!(state.tone_ramps, vec![Some(want)], "after {body}");
    }
    let state = sm.product_state(5);
    assert_eq!(state.tones, vec![Some(p(997, -3.0, true))]);
    let mut loaded = machine();
    loaded.load(sm.save(), limits).expect("loads");
    assert_eq!(loaded.product_state(5).tone_ramps, vec![Some(want)]);
    let legacy = Replicated {
        tone_ramps: Vec::new(),
        ..sm.save()
    };
    loaded.load(legacy, limits).expect("loads");
    assert!(loaded.product_state(5).tone_ramps.is_empty());
}

/// With any ramp running, `fading` has the shape of `gains`, which an
/// install requires: a change of one cell of a matrix of several rows
/// must not leave the other rows out. With none it is empty.
#[test]
fn the_ramps_of_a_product_state_are_shaped_as_its_gains() {
    let mut sm = machine();
    assert!(sm.product_state(0).fading.is_empty());
    // Only the second row changes.
    sm.apply_at(1, 1, &one("1", "fader 1 0.5", 960, 960, None));
    let state = sm.product_state(1);
    assert!(state.fading[1][0].is_some() && state.fading[0][0].is_none());
    assert_eq!(state.fading.len(), state.gains.len());
    assert!(
        state
            .fading
            .iter()
            .zip(&state.gains)
            .all(|(f, g)| f.len() == g.len())
    );
    // A ramp that has finished below the floor is pruned.
    sm.apply_at(2, 1, &one("2", "noop", 960 + 100_000, 960 + 100_000, None));
    assert!(sm.product_state(2).fading.is_empty());
}

/// A gain an engine refuses is derived as silence, and the product
/// state a rebuild reads holds the same: a live replica handed the
/// records and one rebuilt past them compute alike. A later valid gain
/// is derived over it.
#[test]
fn a_gain_an_engine_refuses_is_derived_as_silence_everywhere() {
    let mut sm = machine();
    sm.apply_at(1, 1, &one("1", "fader 0 0.5", 0, 960, None));
    sm.apply_at(2, 1, &one("2", "fader 0 inf", 48, 1008, None));
    assert_eq!(gains(&sm.records[&2]), [(0, 0, 0.0f32.to_bits())]);
    assert_eq!(sm.product_state(2).gains[0][0].to_bits(), 0.0f32.to_bits());
    // Silence already: nothing to derive.
    sm.apply_at(3, 1, &one("3", "fader 0 NaN", 96, 1056, None));
    assert!(!sm.records.contains_key(&3));
    assert_eq!(sm.product_state(3).gains[0][0].to_bits(), 0.0f32.to_bits());
    sm.apply_at(4, 1, &one("4", "fader 0 0.25", 144, 1104, None));
    assert_eq!(gains(&sm.records[&4]), [(0, 0, 0.25f32.to_bits())]);
    // Past MAX_GAIN, finite.
    sm.apply_at(5, 1, &one("5", "fader 0 2e6", 192, 1152, None));
    assert_eq!(gains(&sm.records[&5]), [(0, 0, 0.0f32.to_bits())]);
    assert_eq!(sm.product_state(5).gains[0][0].to_bits(), 0.0f32.to_bits());
}

/// A VCA move derives every member coefficient as fader times VCA,
/// which no translation of the VCA command alone could give, and two
/// replicas derive the same bits.
#[test]
fn a_vca_move_derives_every_member_coefficient() {
    let log = [
        one("1", "fader 0 0.5", 0, 960, None),
        one("2", "fader 1 0.25", 0, 960, None),
        one("3", "vca 0.5", 48, 1008, None),
    ];
    let (mut a, mut b) = (machine(), machine());
    for (n, r) in log.iter().enumerate() {
        a.apply(n as u64 + 1, r);
        b.apply(n as u64 + 1, r);
    }
    let vca = &a.records[&3];
    assert_eq!(
        gains(vca),
        [(0, 0, 0.25f32.to_bits()), (1, 0, 0.125f32.to_bits())]
    );
    assert_eq!(vca.at_index, 1008);
    assert_eq!(a.records, b.records);
}

/// A strip parameter is derived whole from the state.
#[test]
fn a_strip_parameter_is_derived_from_the_state() {
    let mut sm = machine();
    sm.apply(1, &one("1", "trim 1 -6", 0, 960, None));
    assert_eq!(
        sm.records[&1].entries,
        [Entry::Strip {
            slot: 1,
            param: StripParam::Trim { db: -6.0 }
        }]
    );
}

/// A stale, an unchanged and a duplicate command derive nothing, and
/// the floor still advances to their index.
#[test]
fn a_command_that_changes_nothing_derives_nothing() {
    let mut sm = machine();
    sm.apply(1, &one("1", "stale", 0, 960, None));
    sm.apply(2, &one("2", "noop", 0, 1440, None));
    sm.apply(3, &one("3", "fader 0 0.5", 0, 1920, None));
    sm.apply(4, &one("3", "fader 0 0.5", 0, 2400, None));
    assert_eq!(sm.records.keys().copied().collect::<Vec<_>>(), [3]);
    assert_eq!(sm.floor, 2400);
}

/// The index an unstamped lease expiry derives at is a block start for
/// the block sizes the bridge hands records at unrounded, so the index
/// contact lists, awaits and floors at is the one the engine applies.
#[test]
fn the_fallback_index_is_a_block_start_for_the_usual_block_sizes() {
    let blocks = (4..=10).map(|n| 1u64 << n).chain([48, 96, 192, 384, 768]);
    for block in blocks {
        let request = Request {
            stamp: 7 * block,
            body: Body::LeaseExpiry {
                session: origin("s").key(),
                renewed: 0,
                at_index: None,
            },
        };
        let at = floored(0, &request).expect("an expiry takes effect");
        assert_eq!(at % block, 0, "block {block}: index {at}");
    }
}

/// A lease expiry that releases what its session held derives the
/// coefficients it changes, [`FALLBACK_LEAD`] past its stamp when the
/// leader stamped no index, raised to the floor.
#[test]
fn a_lease_expiry_derives_through_the_same_hook() {
    let mut sm = machine();
    sm.apply(1, &one("1", "solo", 0, 960, None));
    assert_eq!(gains(&sm.records[&1]), [(1, 0, 0.0f32.to_bits())]);
    let expiry = |renewed| Request {
        stamp: 4800,
        body: Body::LeaseExpiry {
            session: origin("s").key(),
            renewed,
            at_index: None,
        },
    };
    sm.apply(2, &expiry(0));
    assert_eq!(gains(&sm.records[&2]), [(1, 0, 1.0f32.to_bits())]);
    assert_eq!(sm.records[&2].at_index, 4800 + FALLBACK_LEAD);
    // One the session outlived changes nothing.
    sm.apply(3, &one("3", "solo", 4848, 5808, None));
    sm.apply(4, &expiry(0));
    assert!(!sm.records.contains_key(&4));
}

/// An entry stamped earlier than the one before it in the log takes
/// effect at that one's index: the engine applies them in log order,
/// and the fader ends at the later entry's value.
#[test]
fn an_entry_never_takes_effect_before_the_one_before_it() {
    let mut sm = machine();
    sm.apply(1, &one("1", "route 0 c", 0, 3000, Some(true)));
    sm.apply(2, &one("2", "fader 0 0.5", 0, 1000, None));
    assert_eq!(sm.records[&1].at_index, 3000);
    assert!(sm.records[&1].generation);
    assert_eq!(sm.records[&2].at_index, 3000);
    assert_eq!(gains(&sm.records[&2]), [(0, 0, 0.5f32.to_bits())]);
}

/// A restamp re-asserts every target of the state at its own place in
/// the log, with the routes of the entry it names: a value set after
/// that entry is not undone, and an entry after the restamp applies at
/// or after it. A second decision on the entry derives nothing.
#[test]
fn a_restamp_reasserts_the_state_at_its_place() {
    let mut sm = machine();
    sm.apply_at(10, 1, &one("1", "route 0 c", 0, 3000, Some(true)));
    sm.apply_at(11, 1, &one("2", "fader 0 0.2", 0, 3000, None));
    let restamp = |at_index| Request {
        stamp: 96,
        body: Body::Restamp {
            revision: 10,
            term: 1,
            at_index,
            paths: Vec::new(),
            generation: None,
        },
    };
    sm.apply_at(12, 1, &restamp(4000));
    let r = &sm.records[&12];
    assert_eq!(r.at_index, 4000);
    assert!(r.generation);
    assert!(r.entries.contains(&Entry::Gain {
        input: 0,
        output: 0,
        gain: 0.2,
        fade: None
    }));
    assert!(r.entries.contains(&Entry::Route {
        slot: 0,
        flow_id: "c".into()
    }));
    assert!(
        !r.entries
            .iter()
            .any(|e| matches!(e, Entry::Route { slot: 1, .. }))
    );
    sm.apply_at(13, 1, &one("3", "fader 1 0.5", 96, 1000, None));
    assert_eq!(sm.records[&13].at_index, 4000);
    sm.apply_at(14, 1, &restamp(5000));
    assert!(!sm.records.contains_key(&14));
}

/// A restamp clears a duck mask only where one was derived: the
/// coefficient a command masked and another cleared gets its 0
/// again, one never masked gets none.
#[test]
fn a_restamp_clears_only_the_masks_derived() {
    let mut sm = machine();
    sm.apply_at(10, 1, &one("1", "fader 0 0.2", 0, 3000, None));
    sm.apply_at(11, 1, &one("2", "duck 1", 0, 3000, None));
    sm.apply_at(12, 1, &one("3", "duck 0", 0, 3000, None));
    let restamp = Request {
        stamp: 96,
        body: Body::Restamp {
            revision: 10,
            term: 1,
            at_index: 4000,
            paths: Vec::new(),
            generation: None,
        },
    };
    sm.apply_at(13, 1, &restamp);
    let masks: Vec<&Entry> = (sm.records[&13].entries.iter())
        .filter(|e| matches!(e, Entry::DuckMask { .. }))
        .collect();
    assert_eq!(
        masks,
        [&Entry::DuckMask {
            input: 1,
            output: 0,
            mask: 0
        }]
    );
}

/// A restamp of an entry that derived nothing derives nothing.
#[test]
fn a_restamp_of_an_entry_that_derived_nothing_derives_nothing() {
    let mut sm = machine();
    sm.apply_at(10, 1, &one("1", "noop", 0, 3000, None));
    sm.apply_at(
        11,
        1,
        &Request {
            stamp: 96,
            body: Body::Restamp {
                revision: 10,
                term: 1,
                at_index: 4000,
                paths: Vec::new(),
                generation: None,
            },
        },
    );
    assert!(!sm.records.contains_key(&11));
}

/// An entry asking for an index further than `AHEAD_MAX` past its
/// stamp, as a writer that kept a far client index orders it, moves
/// the floor to its stamp only.
#[test]
fn a_far_index_does_not_move_the_floor_past_its_stamp() {
    let mut sm = machine();
    sm.apply(1, &one("1", "fader 0 0.5", 960, 960 + 60 * 48_000, None));
    assert_eq!(sm.floor, 960);
    assert_eq!(sm.records[&1].at_index, 960);
}

/// A snapshot written before the floor was kept loads with the highest
/// index an entry still decidable takes effect at; one with the floor
/// loads it.
#[test]
fn a_snapshot_without_a_floor_loads_the_highest_decidable_index() {
    let mut sm = machine();
    sm.apply_at(1, 1, &one("1", "fader 0 0.5", 0, 3000, None));
    sm.apply_at(2, 1, &one("2", "fader 0 0.25", 0, 2000, None));
    let mut saved = sm.save();
    assert_eq!(saved.floor, Some(3000));
    let mut loaded = machine();
    loaded
        .load(saved.clone(), Limits::defaults(48_000))
        .unwrap();
    assert_eq!(loaded.floor, 3000);
    saved.floor = None;
    let mut legacy = machine();
    legacy.load(saved, Limits::defaults(48_000)).unwrap();
    assert_eq!(legacy.floor, 3000);
    // The next entry derives the same on both.
    let next = one("3", "vca 0.5", 96, 1000, None);
    loaded.apply_at(3, 1, &next);
    legacy.apply_at(3, 1, &next);
    assert_eq!(loaded.records, legacy.records);
}

/// A command that would re-route, ordered off the generation path, is
/// refused by the product, which reads the flag it was ordered with;
/// one from a writer that did not classify applies.
#[test]
fn the_product_reads_how_a_command_was_ordered() {
    let mut sm = machine();
    sm.apply(1, &one("1", "route 0 c", 0, 960, Some(false)));
    assert!(!sm.records.contains_key(&1));
    assert_eq!(sm.product.flows[0], "a");
    sm.apply(2, &one("2", "route 0 c", 0, 960, None));
    assert!(sm.records[&2].generation);
    assert_eq!(sm.product.flows[0], "c");
}

/// Floats survive a snapshot bit for bit: `serde_json` parses each one
/// correctly rounded.
#[test]
fn product_floats_survive_a_snapshot_bit_for_bit() {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let values: Vec<f64> = (0..20_000)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            f64::from_bits(x).abs() % 10.0
        })
        .filter(|v| v.is_finite())
        .collect();
    let text = serde_json::to_string(&values).unwrap();
    let back: Vec<f64> = serde_json::from_str(&text).unwrap();
    let differ = values
        .iter()
        .zip(&back)
        .filter(|(a, b)| a.to_bits() != b.to_bits())
        .count();
    assert_eq!(differ, 0, "{differ} of {} values moved", values.len());
}

/// A legacy snapshot whose decidable entries include a far client
/// index an older writer kept loads the floor a replica applying the
/// whole log holds, which counted that entry at its stamp.
#[test]
fn a_legacy_floor_skips_a_far_client_index() {
    let mut sm = machine();
    sm.apply_at(1, 1, &one("1", "fader 0 0.5", 0, 3000, None));
    sm.apply_at(2, 1, &one("2", "fader 0 0.25", 0, 60 * 48_000, None));
    assert_eq!(sm.floor, 3000);
    let mut saved = sm.save();
    saved.floor = None;
    let mut legacy = machine();
    legacy.load(saved, Limits::defaults(48_000)).unwrap();
    assert_eq!(legacy.floor, sm.floor);
}

/// An `InputDown` or `InputUp` derives the input's state at its index,
/// raised to the floor; only the `InputUp` builds a generation.
#[test]
fn input_down_and_up_derive_the_input_state() {
    let mut sm = machine();
    sm.apply(1, &one("1", "fader 0 0.5", 0, 3000, None));
    let input = |body| Request { stamp: 0, body };
    sm.apply(
        2,
        &input(Body::InputDown {
            slot: 1,
            at_index: 960,
        }),
    );
    let down = &sm.records[&2];
    assert_eq!(down.entries, [Entry::InputDown { slot: 1 }]);
    assert_eq!(down.at_index, 3000);
    assert!(!down.generation);
    sm.apply(
        3,
        &input(Body::InputUp {
            slot: 1,
            at_index: 4800,
        }),
    );
    let up = &sm.records[&3];
    assert_eq!(up.entries, [Entry::InputUp { slot: 1 }]);
    assert_eq!(up.at_index, 4800);
    assert!(up.generation);
}

/// A void or lease expiry from an older leader, which stamps none:
/// the void applies as that leader's binary applies it, dropping the
/// entry and its `derived` bit, deriving nothing and leaving the floor
/// where it was; the expiry derives [`FALLBACK_LEAD`] past its stamp,
/// so its record can still reach the engines in time. One the leader
/// stamped derives at its own index.
#[test]
fn an_unstamped_void_or_expiry_applies_as_an_older_binary_does() {
    let mut sm = machine();
    sm.apply_at(10, 1, &one("1", "route 0 c", 0, 3000, Some(true)));
    assert_eq!(sm.derived.get(&10), Some(&vec![0]));
    sm.apply_at(11, 1, &void(10, 4800, None));
    assert!(!sm.derived.contains_key(&10));
    assert!(!sm.terms.contains_key(&10));
    assert!(!sm.terms.contains_key(&11), "an unstamped void is final");
    assert!(!sm.records.contains_key(&11));
    assert_eq!(sm.floor, 3000);

    sm.apply_at(12, 1, &one("2", "solo", 0, 3000, None));
    let expiry = |stamp, at_index| Request {
        stamp,
        body: Body::LeaseExpiry {
            session: origin("s").key(),
            renewed: 0,
            at_index,
        },
    };
    sm.apply_at(13, 1, &expiry(4800, None));
    assert_eq!(sm.records[&13].at_index, 4800 + FALLBACK_LEAD);
    assert_eq!(sm.floor, 4800 + FALLBACK_LEAD);

    sm.apply_at(14, 1, &one("3", "route 1 d", 0, 7200, Some(true)));
    sm.apply_at(15, 1, &void(14, 7200, Some(9600)));
    assert_eq!(sm.records[&15].at_index, 9600);
    assert_eq!(sm.floor, 9600);
}

fn void(of: u64, stamp: u64, at_index: Option<u64>) -> Request {
    Request {
        stamp,
        body: Body::Void {
            revision: of,
            term: 1,
            at_index,
            generation: Some(true),
        },
    }
}

/// A void of a product entry re-asserts the state at the void, on the
/// generation path when the entry re-routed: the engine plays the
/// voided command's effect, which the state holds. One of an id not
/// held derives nothing.
#[test]
fn a_void_reasserts_the_state_at_its_index() {
    let mut sm = machine();
    sm.apply_at(10, 1, &one("1", "route 0 c", 0, 3000, Some(true)));
    sm.apply_at(11, 1, &one("2", "fader 0 0.2", 0, 3000, None));
    sm.apply_at(12, 1, &void(10, 96, Some(4800)));
    let r = &sm.records[&12];
    assert_eq!(r.at_index, 4800);
    assert!(r.generation);
    assert!(r.entries.contains(&Entry::Route {
        slot: 0,
        flow_id: "c".into()
    }));
    assert!(r.entries.contains(&Entry::Gain {
        input: 0,
        output: 0,
        gain: 0.2,
        fade: None
    }));
    assert!(void(10, 0, None).body.needs_generation());

    sm.apply_at(13, 1, &void(99, 96, Some(6000)));
    assert!(!sm.records.contains_key(&13));
}

/// A stamped void can go unresolved itself: a restamp of it re-asserts
/// again, and the restamps behind the voided entry count against it.
#[test]
fn a_stamped_void_is_decided_like_the_entry() {
    let mut sm = machine();
    sm.apply_at(10, 1, &one("1", "route 0 c", 0, 3000, Some(true)));
    sm.restamps.insert(10, (2, 3000));
    sm.apply_at(11, 1, &void(10, 96, Some(4800)));
    assert_eq!(sm.terms.get(&11), Some(&(1, 4800)));
    assert_eq!(sm.restamps.get(&11), Some(&(2, 4800)));
    let restamp = Request {
        stamp: 96,
        body: Body::Restamp {
            revision: 11,
            term: 1,
            at_index: 6000,
            paths: Vec::new(),
            generation: Some(true),
        },
    };
    sm.apply_at(12, 1, &restamp);
    assert_eq!(sm.records[&12].at_index, 6000);
    assert!(sm.records[&12].entries.contains(&Entry::Route {
        slot: 0,
        flow_id: "c".into()
    }));
    assert_eq!(sm.restamps.get(&12).map(|r| r.0), Some(3));
}

/// A void of a pending engine body drops it: it derives nothing and
/// is not decidable. A void with the most restamps behind its entry
/// re-asserts and is final.
#[test]
fn a_void_of_an_engine_body_or_a_late_entry_is_final() {
    let mut sm = machine();
    let up = Body::InputUp {
        slot: 1,
        at_index: 960,
    };
    sm.apply_at(10, 1, &Request { stamp: 0, body: up });
    sm.apply_at(11, 1, &void(10, 96, Some(4800)));
    assert!(!sm.terms.contains_key(&11));
    assert!(!sm.records.contains_key(&11));
    sm.apply_at(12, 1, &one("1", "route 0 c", 0, 4800, Some(true)));
    sm.restamps.insert(12, (crate::decide::RESTAMPS_MAX, 4800));
    sm.apply_at(13, 1, &void(12, 96, Some(6000)));
    assert_eq!(sm.records[&13].at_index, 6000);
    assert!(!sm.terms.contains_key(&13));
    assert!(!sm.restamps.contains_key(&13));
}

/// A restamp of an `InputUp` or `InputDown` derives it at the
/// restamp's index, so the engine raises the input there.
#[test]
fn a_restamped_input_state_derives_it_at_the_restamp() {
    let mut sm = machine();
    let restamp = |revision, at_index| Request {
        stamp: 96,
        body: Body::Restamp {
            revision,
            term: 1,
            at_index,
            paths: Vec::new(),
            generation: None,
        },
    };
    for (n, body) in [
        Body::InputUp {
            slot: 1,
            at_index: 960,
        },
        Body::InputDown {
            slot: 0,
            at_index: 960,
        },
    ]
    .into_iter()
    .enumerate()
    {
        let at = 10 + 2 * n as u64;
        sm.apply_at(at, 1, &Request { stamp: 0, body });
        sm.apply_at(at + 1, 1, &restamp(at, 2000));
        let r = &sm.records[&(at + 1)];
        assert_eq!(r.at_index, 2000);
        let want = match n {
            0 => Entry::InputUp { slot: 1 },
            _ => Entry::InputDown { slot: 0 },
        };
        assert_eq!(r.entries, [want]);
        assert_eq!(r.generation, n == 0);
    }
}

/// A lease expiry the leader stamped derives at its own index.
#[test]
fn a_stamped_lease_expiry_derives_at_its_index() {
    let mut sm = machine();
    sm.apply_at(10, 1, &one("1", "solo", 0, 960, None));
    let expiry = Request {
        stamp: 96,
        body: Body::LeaseExpiry {
            session: origin("s").key(),
            renewed: 0,
            at_index: Some(2400),
        },
    };
    sm.apply_at(11, 1, &expiry);
    assert_eq!(sm.records[&11].at_index, 2400);
    assert_eq!(sm.floor, 2400);
}

/// Two input domains: a command re-routing slots of both is refused
/// with any flag, and one re-routing off the generation path too; every
/// machine refuses alike, derives nothing and still moves the floor.
#[test]
fn a_rerouting_command_off_its_path_or_across_domains_is_refused_everywhere() {
    let log = [
        one("1", "pair c d", 0, 960, Some(true)),
        one("2", "route 0 c", 0, 1920, Some(false)),
    ];
    for mut sm in [machine(), machine()] {
        for (n, request) in log.iter().enumerate() {
            let response = sm.apply_at(10 + n as u64, 1, request);
            assert_eq!(response.0[0].result, Resolution::Unchanged);
        }
        assert!(sm.records.is_empty());
        assert_eq!(sm.floor, 1920);
        assert_eq!(sm.product.flows, ["a".to_owned(), "b".to_owned()]);
    }
}

/// The pending generation is the index of the latest decidable entry
/// that re-routed an input, under the restamp that moved it; one that
/// re-routed nothing needs none.
#[test]
fn the_pending_generation_is_the_latest_rerouting_entry() {
    let mut sm = machine();
    assert_eq!(sm.pending_generation(), None);
    sm.apply_at(10, 1, &one("1", "route 0 c", 0, 3000, Some(true)));
    sm.apply_at(11, 1, &one("2", "fader 0 0.2", 0, 5000, None));
    assert!(sm.derived.contains_key(&11));
    assert_eq!(sm.pending_generation(), Some(3000));
    let restamp = Request {
        stamp: 96,
        body: Body::Restamp {
            revision: 10,
            term: 1,
            at_index: 6000,
            paths: Vec::new(),
            generation: Some(true),
        },
    };
    sm.apply_at(12, 1, &restamp);
    assert_eq!(sm.pending_generation(), Some(6000));
}

/// `Body::Protocol` raises the replicated protocol, never lowers it,
/// and a snapshot carries it; one written before it was kept loads at
/// 0.
#[test]
fn the_protocol_marker_is_replicated() {
    let mut sm = machine();
    let marker = |p| Request {
        stamp: 0,
        body: Body::Protocol(p),
    };
    sm.apply_at(1, 1, &marker(2));
    sm.apply_at(2, 1, &marker(1));
    assert_eq!(sm.protocol, 2);
    let mut loaded = machine();
    loaded
        .load(sm.save(), Limits::defaults(48_000))
        .expect("loads");
    assert_eq!(loaded.protocol, 2);
    let mut legacy = serde_json::to_value(sm.save()).unwrap();
    legacy.as_object_mut().unwrap().remove("protocol");
    let legacy: Replicated = serde_json::from_value(legacy).unwrap();
    loaded
        .load(legacy, Limits::defaults(48_000))
        .expect("loads");
    assert_eq!(loaded.protocol, 0);
}

/// The ramps the derived gain entries left running are replicated state
/// of the log: a replica that loads a snapshot carries the same
/// `fading` in its product state, and one written before they were
/// kept loads with none.
#[test]
fn a_snapshot_carries_the_ramps_of_plain_gain_entries() {
    let mut sm = machine();
    sm.apply_at(1, 1, &one("1", "fader 0 0.5", 0, 960, None));
    sm.apply_at(2, 1, &one("2", "fader 0 0.75", 48, 1008, None));
    let want = sm.product_state(2).fading;
    assert!(want[0][0].is_some());
    let mut loaded = machine();
    loaded
        .load(sm.save(), Limits::defaults(48_000))
        .expect("loads");
    assert_eq!(loaded.product_state(2).fading, want);
    // Absent from a snapshot written before: the serde default.
    let legacy = Replicated {
        ramps: Vec::new(),
        ..sm.save()
    };
    loaded
        .load(legacy, Limits::defaults(48_000))
        .expect("loads");
    assert_ne!(loaded.product_state(2).fading, want);
}

/// A record is kept until its index lies [`RECORDS_KEPT`] below the
/// highest stamp applied, and one still ahead stays.
#[test]
fn records_are_kept_for_records_kept_past_their_index() {
    let mut sm = machine();
    sm.apply(1, &one("1", "fader 0 0.5", 0, 960, None));
    sm.apply(2, &one("2", "fader 1 0.5", 40_000, 88_000, None));
    let renew = |stamp| Request {
        stamp,
        body: Body::LeaseRenew(Vec::new()),
    };
    sm.apply(3, &renew(960 + RECORDS_KEPT));
    assert!(sm.records.contains_key(&1));
    sm.apply(4, &renew(960 + RECORDS_KEPT + 48));
    assert!(!sm.records.contains_key(&1));
    assert!(sm.records.contains_key(&2));
}

/// A lease expiry stamped below the floor derives at the floor, after
/// the entry before it.
#[test]
fn a_lease_expiry_below_the_floor_derives_at_the_floor() {
    let mut sm = machine();
    sm.apply(1, &one("1", "solo", 0, 9600, None));
    sm.apply(
        2,
        &Request {
            stamp: 4800,
            body: Body::LeaseExpiry {
                session: origin("s").key(),
                renewed: 0,
                at_index: None,
            },
        },
    );
    assert_eq!(sm.records[&2].at_index, 9600);
    assert_eq!(sm.floor, 9600);
}

/// A machine that loads a snapshot taken mid-log holds the targets of
/// the one that applied the whole log at that point, and derives the
/// same records from every later entry.
#[test]
fn a_machine_loaded_mid_log_derives_what_the_whole_log_derives() {
    let log = [
        one("1", "fader 0 0.5", 0, 960, None),
        one("2", "route 1 c", 48, 1008, None),
        one("3", "vca 0.5", 96, 1056, None),
        one("4", "solo", 144, 1104, None),
        one("5", "trim 0 -3.5", 192, 1152, None),
        one("6", "route 0 d", 240, 1200, Some(true)),
        Request {
            stamp: 288,
            body: Body::LeaseExpiry {
                session: origin("s").key(),
                renewed: 240,
                at_index: None,
            },
        },
    ];
    let mut whole = machine();
    for (n, r) in log.iter().enumerate().take(3) {
        whole.apply_at(n as u64 + 1, 1, r);
    }
    let mut loaded = machine();
    loaded.load(whole.save(), Limits::defaults(48_000)).unwrap();
    assert_eq!(loaded.product.targets(), whole.product.targets());
    for (n, r) in log.iter().enumerate().skip(3) {
        whole.apply_at(n as u64 + 1, 1, r);
        loaded.apply_at(n as u64 + 1, 1, r);
    }
    let later = |sm: &StateMachine<Desk>| {
        sm.records
            .range(4..)
            .map(|(&i, r)| (i, r.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(later(&loaded).len(), 4);
    assert_eq!(later(&loaded), later(&whole));
    assert_eq!(loaded.floor, whole.floor);
}

/// A re-routing command from a writer that did not classify it
/// applies and derives its route like one ordered on the generation
/// path: two machines, one fed each, agree on the product state.
#[test]
fn a_rerouting_command_flagged_none_applies_like_one_flagged_true() {
    let (mut a, mut b) = (machine(), machine());
    a.apply(1, &one("1", "route 0 c", 0, 960, None));
    b.apply(1, &one("1", "route 0 c", 0, 960, Some(true)));
    assert_eq!(a.product.save(), b.product.save());
    assert_eq!(a.records, b.records);
    assert!(a.records[&1].entries.contains(&Entry::Route {
        slot: 0,
        flow_id: "c".into()
    }));
}

/// A stamped lease expiry that derived engine entries is decided like
/// a command: a restamp of it re-asserts the state at the restamp's
/// index, so a replica that held it unresolved gets its effect.
#[test]
fn a_restamp_of_a_lease_expiry_reasserts_its_effect() {
    let mut sm = machine();
    sm.apply_at(10, 1, &one("1", "solo", 0, 960, None));
    let expiry = Request {
        stamp: 96,
        body: Body::LeaseExpiry {
            session: origin("s").key(),
            renewed: 0,
            at_index: Some(2400),
        },
    };
    sm.apply_at(11, 1, &expiry);
    assert!(sm.records.contains_key(&11));
    let restamp = Request {
        stamp: 192,
        body: Body::Restamp {
            revision: 11,
            term: 1,
            at_index: 4800,
            paths: Vec::new(),
            generation: None,
        },
    };
    sm.apply_at(12, 1, &restamp);
    let r = &sm.records[&12];
    assert_eq!(r.at_index, 4800);
    let reasserted = gains(r);
    assert!(
        gains(&sm.records[&11])
            .iter()
            .all(|g| reasserted.contains(g)),
        "the gains after it"
    );
}

/// The pending generation of the applied log names an `InputUp` the
/// state holds, and not a command flagged for its generation that
/// the product refused, which built none.
#[test]
fn the_pending_generation_names_what_built_one() {
    let mut sm = machine();
    sm.apply(1, &one("1", "pair c d", 0, 960, Some(true)));
    assert!(!sm.records.contains_key(&1), "refused");
    assert_eq!(sm.pending_generation(), None);
    let up = Request {
        stamp: 0,
        body: Body::InputUp {
            slot: 1,
            at_index: 4800,
        },
    };
    sm.apply(2, &up);
    assert_eq!(sm.pending_generation(), Some(4800));
}

/// The state a core is rebuilt from: the targets, each slot up as its
/// latest input state held says and up where none is, and the last
/// log id it folds in.
#[test]
fn the_product_state_holds_the_targets_and_the_input_states() {
    let mut sm = machine();
    sm.apply(1, &one("1", "fader 0 0.5", 0, 960, None));
    let input = |body| Request { stamp: 0, body };
    sm.apply(
        2,
        &input(Body::InputUp {
            slot: 1,
            at_index: 960,
        }),
    );
    sm.apply(
        3,
        &input(Body::InputDown {
            slot: 1,
            at_index: 1920,
        }),
    );
    let state = sm.product_state(3);
    let targets = sm.product.targets().unwrap();
    assert_eq!(state.gains, targets.gains);
    assert_eq!(state.flows, targets.flows);
    assert_eq!(state.strips, targets.strips);
    assert_eq!(state.up, [true, false]);
    assert_eq!(state.last_id, 3);
    assert!(state.pending.is_empty());
}

/// Records go once retention passes their index, oldest first, and
/// `records_from` names the last gone: a resync from before it cannot
/// be replayed.
#[test]
fn records_from_names_the_last_record_retention_dropped() {
    let mut sm = machine();
    sm.apply(1, &one("1", "fader 0 0.5", 0, 960, None));
    sm.apply(2, &one("2", "fader 0 0.25", 0, 1920, None));
    assert_eq!(sm.records_from, 0);
    sm.apply(
        3,
        &one(
            "3",
            "fader 0 0.75",
            1920 + RECORDS_KEPT,
            1920 + RECORDS_KEPT,
            None,
        ),
    );
    assert_eq!(sm.records.keys().copied().collect::<Vec<_>>(), [2, 3]);
    assert_eq!(sm.records_from, 1);
    // The record dropped is the newest at or below `records_from`: a
    // replay from before it misses it.
    assert_eq!(sm.record_floor, Some(1));
    assert!(!sm.records_whole(0));
    assert!(sm.records_whole(1));
}

/// A machine loaded from a snapshot taken after entries that derived
/// no record holds every record after the newest that did, though
/// `records_from` is the snapshot's index: a replay from that record
/// on misses nothing, one from before it does. A snapshot written
/// before the floor was kept leaves it unknown, and only
/// `records_from` counts.
#[test]
fn a_snapshot_carries_the_newest_record_as_the_floor() {
    let mut sm = machine();
    sm.apply(1, &one("1", "fader 0 0.5", 0, 960, None));
    for index in 2..=5 {
        sm.apply(
            index,
            &Request {
                stamp: 960 + 48 * index,
                body: Body::LeaseRenew(Vec::new()),
            },
        );
    }
    assert_eq!(sm.records.keys().copied().collect::<Vec<_>>(), [1]);
    let saved = sm.save();
    assert_eq!(saved.record_floor, Some(1));
    let load = |saved: Replicated| {
        let mut loaded = machine();
        loaded.load(saved, Limits::defaults(48_000)).expect("loads");
        // As the store sets it.
        loaded.records_from = 5;
        loaded
    };
    let loaded = load(saved.clone());
    assert_eq!(loaded.record_floor, Some(1));
    assert!(!loaded.records_whole(0), "the record at 1 is not held");
    for after in 1..=5 {
        assert!(loaded.records_whole(after), "after {after}");
    }
    // As bytes: a JSON value holds no u128 command id.
    let bytes = String::from_utf8(serde_json::to_vec(&saved).expect("encodes")).unwrap();
    let field = "\"record_floor\":1,";
    assert!(bytes.contains(field), "{bytes}");
    let legacy: Replicated = serde_json::from_str(&bytes.replace(field, "")).expect("decodes");
    assert_eq!(legacy.record_floor, None);
    let loaded = load(legacy);
    assert!(!loaded.records_whole(4), "an unknown floor");
    assert!(loaded.records_whole(5));
}

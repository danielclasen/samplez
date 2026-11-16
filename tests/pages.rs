//! Page hashes and entry fingerprints of replicated engines, against a
//! real MXL domain: what replicas compare to find out whether their
//! member flows are bit-identical.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::mpsc::Receiver;
use std::time::Duration;

use common::{B, Domain, MARGIN, Pilot, config, diagonal, new_id, wait_for};
use samplez::blocks::{HEALTHY, PAGE, PageHash, fingerprint};
use samplez::{BlockVerdict, Engine, Entry, InputState, Replication};

/// Samples of a gain ramp and a fade-in: half a second, so the pages a
/// ramp runs over differ from one another. The pilot repeats every three
/// pages.
const RAMP: u32 = 24_000;

struct Replica {
    e: Engine,
    hashes: Receiver<PageHash>,
    verdicts: Receiver<BlockVerdict>,
}

/// A replicated engine of `voters` voters, a two-channel input through
/// gain 1 to a two-channel output, whose page hashes go to a channel of
/// `room`.
fn start(domain: &Domain, src: &str, voters: usize, room: usize) -> Replica {
    let out = new_id();
    let mut config = config(
        &domain.0,
        &[(src, 2)],
        &[(&out, 2)],
        diagonal(2, 1.0),
        MARGIN,
    );
    config.ramp_samples = RAMP;
    let (report_tx, _reports) = std::sync::mpsc::sync_channel(64);
    let (hash_tx, hashes) = std::sync::mpsc::sync_channel(room);
    let (verdict_tx, verdicts) = std::sync::mpsc::sync_channel(1 << 16);
    let process = samplez::rt::setup_process(&config.rt);
    let replication = Replication {
        voters,
        reports: report_tx,
        statuses: None,
        hashes: Some(hash_tx),
    };
    let e = Engine::start_replicated(config, process, Some(verdict_tx), replication).unwrap();
    wait_for(Duration::from_secs(5), "attached and realised", || {
        let s = e.status();
        s.outputs[0].realised && s.inputs[0].state == InputState::Attached
    });
    Replica {
        e,
        hashes,
        verdicts,
    }
}

fn processed(e: &Engine) -> u64 {
    e.counters().processed.load(Acquire)
}

/// Waits until every engine processed block `k`.
fn wait_all(rs: &[&Replica], k: u64) {
    wait_for(Duration::from_secs(20), &format!("block {k}"), || {
        rs.iter().all(|r| processed(&r.e) > k)
    });
}

impl Replica {
    /// The page hashes received, by page; a page sent twice fails.
    fn pages(&self) -> BTreeMap<u64, PageHash> {
        let mut pages = BTreeMap::new();
        for h in self.hashes.try_iter() {
            assert!(
                pages.insert(h.page, h).is_none(),
                "page {} sent twice",
                h.page
            );
        }
        pages
    }

    /// The pages whose output depends on more than the input and the
    /// log: a block with an input silenced, by a late read or before the
    /// input was attached, and the fade-in after it.
    fn disturbed(&self) -> Vec<(u64, u64)> {
        self.verdicts
            .try_iter()
            .filter(|v| v.silenced != 0)
            .map(|v| {
                (
                    v.k * B / PAGE,
                    ((v.k + 1) * B + u64::from(RAMP)).div_ceil(PAGE),
                )
            })
            .collect()
    }
}

/// Appends gain entries `(id, at_index, input, gain)` to `e` and commits
/// them.
fn log(e: &Engine, entries: &[(u64, u64, usize, f32)]) {
    for &(id, at, input, gain) in entries {
        let entry = Entry::Gain {
            input,
            output: input,
            gain,
        };
        e.append(id, at, entry).unwrap();
    }
    e.commit(entries.last().unwrap().0);
}

/// The pages both engines hashed, outside the pages either one's output
/// was disturbed in, with both records of each.
fn both(a: &Replica, b: &Replica) -> Vec<(PageHash, PageHash)> {
    let (pa, pb) = (a.pages(), b.pages());
    let disturbed: Vec<(u64, u64)> = a.disturbed().into_iter().chain(b.disturbed()).collect();
    pa.iter()
        .filter_map(|(p, ha)| Some((*ha, *pb.get(p)?)))
        .filter(|(h, _)| {
            !disturbed
                .iter()
                .any(|&(from, to)| (from..to).contains(&h.page))
        })
        .collect()
}

/// Two engines on the same input and the same log hash every page both
/// computed alike, with equal fingerprints: whatever their commit mode
/// (one commits, the other commits nothing), across gain ramps, and with
/// blocks of 48 samples, so blocks straddle pages.
#[test]
fn two_engines_hash_the_pages_both_computed_alike() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let a = start(&domain, &src, 2, 1 << 16);
        let b = start(&domain, &src, 3, 1 << 16);
        a.e.set_fence(u64::MAX).unwrap();
        a.e.commit_from(0, processed(&a.e) + 2).unwrap();
        let k = processed(&a.e).max(processed(&b.e));
        // Past the fade-in: two ramps in different pages, the second page
        // holding two.
        let at = (k + u64::from(RAMP) / B + 100) * B;
        let at2 = ((at / PAGE + 4) * PAGE).next_multiple_of(B) + 7 * B;
        let entries = [(1, at, 0, 0.5), (2, at2, 1, 0.25), (3, at2 + 2 * B, 0, 1.0)];
        log(&a.e, &entries);
        log(&b.e, &entries);
        wait_all(&[&a, &b], (at2 + 20 * PAGE) / B);

        let pairs = both(&a, &b);
        for (ha, hb) in &pairs {
            assert_eq!(
                (ha.hash, ha.fingerprint),
                (hb.hash, hb.fingerprint),
                "page {}",
                ha.page
            );
        }
        let compared = |p: u64| pairs.iter().find(|h| h.0.page == p).map(|h| h.0);
        let (Some(p1), Some(p2)) = (compared(at / PAGE), compared(at2 / PAGE)) else {
            return Err("an entry's page not compared".to_owned());
        };
        if pairs.len() <= 20 {
            return Err(format!("{} pages compared", pairs.len()));
        }
        assert_eq!(p1.fingerprint, fingerprint(&mut [(1, at)]));
        assert_eq!(
            p2.fingerprint,
            fingerprint(&mut [(3, at2 + 2 * B), (2, at2)])
        );
        if !pairs.iter().any(|(ha, hb)| ha.committed && !hb.committed) {
            // An overrun skip withdraws the committing engine: it then
            // commits nothing without evidence.
            let skipped = a.e.counters().overrun_skipped_samples.load(Relaxed);
            assert!(
                skipped > 0,
                "no page committed by one engine and not by the other"
            );
            return Err(format!("{skipped} samples skipped"));
        }
        let other = pairs.iter().filter(|h| h.0.fingerprint == HEALTHY).count();
        assert!(other > 10, "{other} pages without entries");
        // Pages differ in content, so equal hashes say something.
        let mut hashes: Vec<u64> = pairs.iter().map(|h| h.0.hash).collect();
        hashes.sort_unstable();
        hashes.dedup();
        assert!(hashes.len() > 10, "{} distinct hashes", hashes.len());
        Ok(())
    });
}

/// Two engines on the same input and the same strip entries hash every
/// page both computed alike: the strips' ramps, filters and dynamics
/// compute the same bits on both, and the pages they change differ from
/// the input's. A skip or a silenced block changes a strip's filter and
/// envelope state beyond the ramp, so pages are compared up to the first
/// one; a run with fewer than six strip pages before it runs again.
#[test]
fn two_engines_with_strips_hash_the_pages_both_computed_alike() {
    use samplez_dsp::strip::{BandType, CompParams, EqBand, GateParams, StripParam};
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let a = start(&domain, &src, 2, 1 << 16);
        let b = start(&domain, &src, 3, 1 << 16);
        let k = processed(&a.e).max(processed(&b.e));
        let at = (k + u64::from(RAMP) / B + 100) * B;
        let at2 = ((at / PAGE + 3) * PAGE).next_multiple_of(B) + 5 * B;
        let params = [
            (at, StripParam::Trim { db: -3.0 }),
            (
                at,
                StripParam::Polarity {
                    leg: 1,
                    inverted: true,
                },
            ),
            (
                at,
                StripParam::Hpf {
                    enabled: true,
                    freq: 120.0,
                },
            ),
            (at, StripParam::Eq { enabled: true }),
            (
                at,
                StripParam::EqBand {
                    band: 2,
                    value: EqBand {
                        kind: BandType::Peak,
                        freq: 1250.0,
                        gain_db: 9.0,
                        q: 2.0,
                    },
                },
            ),
            (
                at2,
                StripParam::Gate(GateParams {
                    enabled: true,
                    threshold_db: -30.0,
                    range_db: 20.0,
                    attack_ms: 1.0,
                    release_ms: 50.0,
                }),
            ),
            (
                at2,
                StripParam::Comp(CompParams {
                    enabled: true,
                    threshold_db: -24.0,
                    ratio: 4.0,
                    attack_ms: 2.0,
                    release_ms: 80.0,
                    knee_db: 0.0,
                    makeup_db: 3.0,
                }),
            ),
            (at2 + 2 * B, StripParam::DynamicsBeforeEq { enabled: true }),
            (at2 + 2 * B, StripParam::LegTrim { leg: 0, db: 4.5 }),
        ];
        for r in [&a, &b] {
            for (id, &(at, param)) in params.iter().enumerate() {
                r.e.append(id as u64 + 1, at, Entry::Strip { slot: 0, param })
                    .unwrap();
            }
            r.e.commit(params.len() as u64);
        }
        wait_all(&[&a, &b], (at2 + 24 * PAGE) / B);
        // A skip or a silenced read leaves a strip's state without those
        // samples until it converges: pages are compared up to the first
        // one either engine had after the strip left its rest at `at`.
        let mut cut = u64::MAX;
        let mut disturbed = Vec::new();
        for r in [&a, &b] {
            let vs: Vec<BlockVerdict> = r.verdicts.try_iter().collect();
            for w in vs.windows(2) {
                if w[1].k > w[0].k + 1 && w[1].k * B > at {
                    cut = cut.min(((w[0].k + 1) * B).max(at));
                }
            }
            for v in vs.iter().filter(|v| v.silenced != 0) {
                let (from, to) = (v.k * B, (v.k + 1) * B + u64::from(RAMP));
                disturbed.push((from / PAGE, to.div_ceil(PAGE)));
                if to > at {
                    cut = cut.min(from.max(at));
                }
            }
        }
        let (pa, pb) = (a.pages(), b.pages());
        let pairs: Vec<_> = pa
            .iter()
            .filter_map(|(p, ha)| Some((*ha, *pb.get(p)?)))
            .filter(|(h, _)| (h.page + 1) * PAGE <= cut)
            .filter(|(h, _)| !disturbed.iter().any(|d| (d.0..d.1).contains(&h.page)))
            .collect();
        for (ha, hb) in &pairs {
            assert_eq!(
                (ha.hash, ha.fingerprint),
                (hb.hash, hb.fingerprint),
                "page {}, the first entry in page {}",
                ha.page,
                at / PAGE
            );
        }
        let strip_pages = pairs.iter().filter(|h| h.0.page > at / PAGE).count();
        if strip_pages < 6 {
            return Err(format!("{strip_pages} pages with the strip compared"));
        }
        // The pilot repeats every three pages: the strip changes them,
        // so no compared page after the entries hashes as the page three
        // before them.
        let hash = |p: u64| pa.get(&p).map(|h| h.hash);
        for (h, _) in pairs.iter().filter(|h| h.0.page > at / PAGE) {
            let p = h.page;
            if let Some(before) = hash(p - 3 * ((p - at / PAGE) / 3 + 1)) {
                assert_ne!(
                    Some(h.hash),
                    Some(before),
                    "page {p} unchanged by the strip"
                );
            }
        }
        Ok(())
    });
}

/// An entry that changes no sample changes the fingerprint of its page
/// and not its hash: one engine applies it at one index, the other at
/// another (restamped), each page holding it differs in fingerprint
/// alone, and every other page agrees in both.
#[test]
fn an_inaudible_entry_changes_the_fingerprint_not_the_hash() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let a = start(&domain, &src, 2, 1 << 16);
        let b = start(&domain, &src, 2, 1 << 16);
        let k = processed(&a.e).max(processed(&b.e));
        // Gain 1 again, past the fade-in: no sample changes.
        let at = (k + u64::from(RAMP) / B + 100) * B;
        let restamped = ((at / PAGE + 3) * PAGE).next_multiple_of(B);
        log(&a.e, &[(1, at, 0, 1.0)]);
        log(&b.e, &[(1, restamped, 0, 1.0)]);
        wait_all(&[&a, &b], (restamped + 4 * PAGE) / B);

        let pairs = both(&a, &b);
        let entry_pages = [at / PAGE, restamped / PAGE];
        for (ha, hb) in &pairs {
            assert_eq!(ha.hash, hb.hash, "page {}", ha.page);
            if entry_pages.contains(&ha.page) {
                assert_ne!(ha.fingerprint, hb.fingerprint, "page {}", ha.page);
            } else {
                assert_eq!(ha.fingerprint, hb.fingerprint, "page {}", ha.page);
            }
        }
        let seen = entry_pages
            .iter()
            .filter(|p| pairs.iter().any(|h| h.0.page == **p))
            .count();
        if seen < 2 {
            return Err(format!("{seen} of the entry pages compared"));
        }
        Ok(())
    });
}

/// An engine of one voter hashes no page. One of two voters does, and
/// counts the hashes its full channel has no room for.
#[test]
fn one_voter_hashes_no_page_and_a_full_channel_is_counted() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let one = start(&domain, &src, 1, 1 << 16);
    let two = start(&domain, &src, 2, 1);
    let k = processed(&one.e).max(processed(&two.e));
    wait_all(&[&one, &two], k + 20 * PAGE / B);
    assert_eq!(one.hashes.try_iter().count(), 0, "one voter hashed pages");
    let c = one.e.counters();
    assert_eq!(c.page_hashes_dropped.load(Relaxed), 0);
    assert_eq!(c.page_hashes_unsent.load(Relaxed), 0);
    assert_eq!(two.hashes.try_iter().count(), 1);
    let c = two.e.counters();
    assert!(
        c.page_hashes_unsent.load(Relaxed) >= 5,
        "{} unsent",
        c.page_hashes_unsent.load(Relaxed)
    );
    assert_eq!(c.page_hashes_dropped.load(Relaxed), 0);
}

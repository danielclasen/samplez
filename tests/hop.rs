//! The engine end to end on real libmxl flows: one hop, a gain change,
//! two replicas, a chain of three, a killed and restarted producer, an
//! input offset, two inputs with one pruned, a late producer, a vanished
//! domain, a foreign writer on the output and the `samplez-hop` binary.
//! Runs without the test harness, one scenario at a time so their timing
//! does not interfere, and re-executes itself as a producer process that
//! can be SIGKILLed (`hop child <domain> <id>`).

mod common;

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use common::{B, Checker, Domain, MARGIN, Pilot, config, diagonal, new_id, start, wait_for};
use samplez::{GainError, InputState};
use samplez_mxl::Instance;
use samplez_mxl::output::{self, Creation, OutputConfig};

const SECOND: Duration = Duration::from_secs(1);
/// Gain changes an engine holds outstanding.
const PENDING: u64 = 64;
/// An input offset that is not a multiple of the block size.
const OFFSET: i64 = 100;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("child") {
        let _pilot = Pilot::start(Path::new(&args[2]), &args[3], 2, B, 0);
        println!("ready");
        loop {
            std::thread::park();
        }
    }
    let filter = args.iter().skip(1).find(|a| !a.starts_with('-'));
    for (name, test) in [
        (
            "one_hop_output_is_the_gain_times_the_input_at_the_same_index",
            one_hop as fn(),
        ),
        ("gain_change_ramps_per_coefficient", gain_change),
        ("two_replicas_write_bit_identical_flows", replicas),
        (
            "chain_of_three_hops_stays_aligned_with_no_late_block",
            chain_of_three,
        ),
        (
            "killed_producer_is_silenced_pruned_and_re_added",
            killed_producer,
        ),
        ("offset_input_is_read_offset_samples_ahead", offset),
        ("two_inputs_prune_one_and_the_other_stays_exact", two_inputs),
        (
            "late_producer_is_silenced_and_gets_a_margin_warning",
            late_producer,
        ),
        (
            "input_whose_domain_vanished_is_re_added_once_it_is_back",
            domain_vanishes,
        ),
        ("output_held_by_a_foreign_writer_is_retried", foreign_writer),
        ("samplez_hop_runs_from_a_config_file", binary),
    ] {
        if filter.is_some_and(|f| !name.contains(f.as_str())) {
            continue;
        }
        let t = Instant::now();
        test();
        println!("test {name} ... ok ({:.1} s)", t.elapsed().as_secs_f64());
    }
}

fn one_hop() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let cfg = config(
        &domain.0,
        &[(&src, 2)],
        &[(&dst, 2)],
        diagonal(2, 0.5),
        MARGIN,
    );
    let (engine, rx) = start(cfg);
    let mut checker = Checker::new(&domain.0, &dst, 2, &[0.5]);
    checker.run(&[&rx], 2 * SECOND, |_, _| {});
    assert_eq!(engine.status().inputs[0].state, InputState::Attached);
    assert!(engine.status().outputs[0].realised);
    let before = checker.counts;
    checker.run(&[&rx], 5 * SECOND, |_, _| {});
    let c = checker.counts;
    assert!(c.clean - before.clean > 1_000, "{c:?}");
    assert!(before.ramp > 0, "the input fades in at start: {before:?}");
    // The producer commits each block at its index, well before the
    // deadline: the group wait returns early, never at the deadline.
    let counters = engine.counters();
    assert!(counters.wait_early.load(Relaxed) > 1_000);
    assert_eq!(counters.input_wait_timeout.load(Relaxed), 0);
    common::stop(engine);
}

fn gain_change() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let cfg = config(
        &domain.0,
        &[(&src, 1)],
        &[(&dst, 1)],
        vec![vec![0.5]],
        MARGIN,
    );
    let (engine, rx) = start(cfg);
    let mut checker = Checker::new(&domain.0, &dst, 1, &[0.5]);
    checker.run(&[&rx], 2 * SECOND, |_, _| {});
    let before = checker.counts;
    let k = checker.last_k().unwrap();

    assert!(
        engine.set_gain(0, 0, 0.2, (k + 100) * B + 1).is_err(),
        "unaligned index"
    );
    assert!(
        engine.set_gain(1, 0, 0.2, (k + 100) * B).is_err(),
        "no such input channel"
    );
    let at = (k + 100) * B;
    checker.expect_gain_change(0.2, at);
    engine.set_gain(0, 0, 0.2, at).unwrap();
    checker.run(&[&rx], 2 * SECOND, |_, _| {});
    let c = checker.counts;
    assert_eq!(
        c.ramp - before.ramp,
        10,
        "480 samples from at_index are 10 blocks: {c:?}"
    );
    assert_eq!(checker.gain(), 0.2, "the ramp ended");

    assert!(matches!(
        engine.set_gain(0, 0, f32::NAN, at + 100 * B),
        Err(GainError::Invalid(_))
    ));
    // A change at an index already produced is refused at submit; the
    // checker fails on any ramp it does not expect.
    let late = checker.last_k().unwrap() * B;
    assert!(matches!(
        engine.set_gain(0, 0, 0.9, late),
        Err(GainError::TooNear { .. })
    ));

    // 64 outstanding changes fit, the 65th is refused. They hold the gain,
    // so no block ramps.
    let first = (checker.last_k().unwrap() + 200) * B;
    for i in 0..PENDING {
        engine.set_gain(0, 0, 0.2, first + i * B).unwrap();
    }
    // The RT thread has moved them from the ring to its pending list, so
    // the ring has room: the outstanding count refuses the 65th.
    std::thread::sleep(SECOND / 20);
    assert_eq!(
        engine.set_gain(0, 0, 0.2, first + PENDING * B),
        Err(GainError::Full)
    );
    checker.run(&[&rx], SECOND, |_, _| {});
    let c = engine.counters();
    assert_eq!(c.gains_consumed.load(Relaxed), 1 + PENDING);
    let next = (checker.last_k().unwrap() + 100) * B;
    engine.set_gain(0, 0, 0.2, next).unwrap();
    checker.run(&[&rx], SECOND, |_, _| {});
    assert_eq!(c.gains_consumed.load(Relaxed), 2 + PENDING);
    assert_eq!(c.command_late.load(Relaxed), 0);
    assert_eq!(checker.gain(), 0.2);
    assert_eq!(engine.status().diverged, None);
    common::stop(engine);
}

/// Two engines on the same input, started at different times, given the
/// same gain change at the same index at different times: their output
/// flows are bit-identical at every index after both faded in.
fn replicas() {
    let domain = Domain::new();
    let (src, out_a, out_b) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let matrix = vec![vec![0.5, 0.25], vec![0.0, 0.75]];
    let (a, rx_a) = start(config(
        &domain.0,
        &[(&src, 2)],
        &[(&out_a, 2)],
        matrix.clone(),
        MARGIN,
    ));
    std::thread::sleep(SECOND / 3);
    let (b, rx_b) = start(config(
        &domain.0,
        &[(&src, 2)],
        &[(&out_b, 2)],
        matrix,
        MARGIN,
    ));
    for e in [&a, &b] {
        wait_for(5 * SECOND, "replica running", || {
            let s = e.status();
            s.outputs[0].realised && s.inputs[0].state == InputState::Attached
        });
    }
    std::thread::sleep(SECOND / 2);
    let now = rx_a.try_iter().last().unwrap().k;
    let at = (now + 100) * B;
    a.set_gain(1, 0, 0.6, at).unwrap();
    std::thread::sleep(SECOND / 20);
    b.set_gain(1, 0, 0.6, at).unwrap();
    // Past the compared range, which ends 150 ms after `now`, plus the
    // margin; well within the 1 s history.
    std::thread::sleep(SECOND / 2);
    let (va, vb): (Vec<_>, Vec<_>) = (rx_a.try_iter().collect(), rx_b.try_iter().collect());
    let applied =
        |vs: &[samplez::BlockVerdict], k| vs.iter().find(|v| v.k == k).unwrap().commands_applied;
    let range = now + 50..now + 150;
    for k in [range.start, at / B - 1] {
        assert_eq!((applied(&va, k), applied(&vb, k)), (0, 0), "block {k}");
    }
    assert_eq!((applied(&va, at / B), applied(&vb, at / B)), (1, 1));
    for v in va.iter().chain(&vb).filter(|v| range.contains(&v.k)) {
        assert_eq!(v.silenced | v.fading, 0, "{v:?}");
        assert_eq!(v.written, 1, "{v:?}");
    }
    let x = common::read_blocks(&domain.0, &out_a, 2, range.clone());
    let y = common::read_blocks(&domain.0, &out_b, 2, range.clone());
    let bits = |v: &[f32]| v.iter().map(|s| s.to_bits()).collect::<Vec<_>>();
    assert!(bits(&x) == bits(&y), "replica outputs differ");
    // The ramp is in the compared range: channel 0 of the block at
    // at_index differs from the block before at the same pilot phase.
    let ramp = (at / B - range.start) as usize * 2 * B as usize;
    assert_ne!(bits(&x[ramp..ramp + B as usize]), bits(&x[..B as usize]));
    common::stop(b);
    common::stop(a);
}

fn chain_of_three() {
    let domain = Domain::new();
    let ids: Vec<String> = (0..4).map(|_| new_id()).collect();
    let _pilot = Pilot::start(&domain.0, &ids[0], 2, B, 0);
    // Chain rule: each hop's margin covers the one before it, its
    // processing, its wake jitter and one block, here with 40 ms of
    // headroom per hop.
    let mut hops = Vec::new();
    for h in 0..3 {
        let margin = MARGIN + 2 * MARGIN * h as u64;
        let cfg = config(
            &domain.0,
            &[(&ids[h], 2)],
            &[(&ids[h + 1], 2)],
            diagonal(2, 0.5),
            margin,
        );
        let (engine, rx) = start(cfg);
        wait_for(5 * SECOND, "hop realised", || {
            let s = engine.status();
            s.outputs[0].realised && s.inputs[0].state == InputState::Attached
        });
        hops.push((engine, rx));
    }
    let last = &hops[2].0;
    let late = last.counters().input_late_blocks.load(Relaxed);
    let rxs: Vec<_> = hops.iter().map(|(_, rx)| rx).collect();
    let mut checker = Checker::new(&domain.0, &ids[3], 2, &[0.5, 0.5, 0.5]);
    checker.run(&rxs, 6 * SECOND, |_, _| {});
    let c = checker.counts;
    assert!(c.clean > 1_000, "{c:?}");
    assert_eq!(
        last.counters().input_late_blocks.load(Relaxed),
        late,
        "late at the last hop"
    );
    for (e, _) in hops.into_iter().rev() {
        common::stop(e);
    }
}

/// A producer process, SIGKILLed on drop so that a failing test leaves
/// none behind.
struct Producer(Child);

impl Producer {
    /// SIGKILL: the flow stays, its head freezes.
    fn kill(mut self) {
        // Child::kill sends SIGKILL.
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_producer(domain: &Domain, id: &str) -> Producer {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("child")
        .arg(&domain.0)
        .arg(id)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    assert_eq!(line, "ready\n");
    Producer(child)
}

fn killed_producer() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let producer = spawn_producer(&domain, &src);
    let cfg = config(
        &domain.0,
        &[(&src, 2)],
        &[(&dst, 2)],
        diagonal(2, 0.5),
        MARGIN,
    );
    let (engine, rx) = start(cfg);
    let mut checker = Checker::new(&domain.0, &dst, 2, &[0.5]);
    checker.run(&[&rx], 2 * SECOND, |_, _| {});
    assert_eq!(engine.status().inputs[0].state, InputState::Attached);
    let before = checker.counts;

    // Gain changes an hour ahead fill the pending list; generation swaps
    // still go through.
    let hour = (checker.last_k().unwrap() + 3_600_000) * B;
    for i in 0..PENDING {
        engine.set_gain(0, 0, 0.5, hour + i * B).unwrap();
    }
    assert_eq!(engine.set_gain(0, 0, 0.5, hour), Err(GainError::Full));

    producer.kill();
    let mut pruned = false;
    checker.run(&[&rx], 2 * SECOND, |_, _| {
        pruned |= engine.status().inputs[0].state == InputState::Pruned;
    });
    assert!(pruned, "input not pruned after its producer was killed");
    let gap = checker.counts;
    assert!(gap.silenced - before.silenced > 1_500, "{gap:?}");

    // The restarted producer collects the stale flow and creates the same
    // id; the poller finds it and the input is added back with a fade-in.
    let producer = spawn_producer(&domain, &src);
    let mut back = false;
    checker.run(&[&rx], 4 * SECOND, |_, _| {
        back |= engine.status().inputs[0].state == InputState::Attached;
    });
    let c = checker.counts;
    assert!(back, "input not re-added: {:?}", engine.status().inputs[0]);
    assert!(c.ramp - gap.ramp >= 10, "no fade-in after re-add: {c:?}");
    assert!(c.clean - gap.clean > 1_000, "{c:?}");

    // The re-added input is pruned again when its producer dies again.
    producer.kill();
    let mut pruned = false;
    checker.run(&[&rx], 2 * SECOND, |_, _| {
        pruned |= engine.status().inputs[0].state == InputState::Pruned;
    });
    assert!(
        pruned,
        "input not pruned after its second producer was killed"
    );
    common::stop(engine);
}

/// Output sample `n` is the gain times input sample `n + offsetSamples`,
/// with an offset that is not a multiple of the block size.
fn offset() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let mut cfg = config(
        &domain.0,
        &[(&src, 2)],
        &[(&dst, 2)],
        diagonal(2, 0.5),
        MARGIN,
    );
    cfg.inputs[0].offset_samples = OFFSET;
    let (engine, rx) = start(cfg);
    wait_for(5 * SECOND, "input attached and output realised", || {
        let s = engine.status();
        s.outputs[0].realised && s.inputs[0].state == InputState::Attached
    });
    std::thread::sleep(SECOND / 2);
    let now = rx.try_iter().last().unwrap().k;
    let range = now + 10..now + 110;
    // Past the range and the margin, within the 1 s history.
    std::thread::sleep(SECOND / 2);
    let verdicts: Vec<_> = rx.try_iter().filter(|v| range.contains(&v.k)).collect();
    assert_eq!(verdicts.len() as u64, range.end - range.start);
    for v in &verdicts {
        assert_eq!(v.verdict(), samplez::Verdict::Clean, "{v:?}");
        assert_eq!(v.written, 1, "{v:?}");
    }
    let out = common::read_blocks(&domain.0, &dst, 2, range.clone());
    for (j, k) in range.enumerate() {
        for ch in 0..2 {
            for i in 0..B {
                let n = k * B + i;
                let got = out[(j * 2 + ch) * B as usize + i as usize];
                let want = 0.5 * common::pilot(ch, n.checked_add_signed(OFFSET).unwrap());
                assert!(
                    exact(got, want),
                    "channel {ch} index {n}: got {got:e}, want {want:e}"
                );
            }
        }
    }
    common::stop(engine);
}

/// A clean sample equals `want` bit for bit; a zero is +0.0.
fn exact(got: f32, want: f32) -> bool {
    if want == 0.0 {
        got.to_bits() == 0
    } else {
        got == want
    }
}

/// Two inputs in one domain, each to its own output: pruning and
/// re-adding one, which swaps the domain's generation twice, leaves every
/// block of the other's output that the loop processes clean and exact.
fn two_inputs() {
    let domain = Domain::new();
    let (src_a, src_b, out_a, out_b) = (new_id(), new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src_a, 1, B, 0);
    let producer = spawn_producer(&domain, &src_b);
    let cfg = config(
        &domain.0,
        &[(&src_a, 1), (&src_b, 2)],
        &[(&out_a, 1), (&out_b, 2)],
        diagonal(3, 0.5),
        MARGIN,
    );
    let (engine, rx) = start(cfg);
    let state = |slot: usize| engine.status().inputs[slot].state;
    wait_for(5 * SECOND, "both inputs attached", || {
        let s = engine.status();
        s.outputs.iter().all(|o| o.realised)
            && s.inputs.iter().all(|i| i.state == InputState::Attached)
    });
    // Past input A's fade-in.
    std::thread::sleep(SECOND / 2);
    let _ = rx.try_iter().count();
    let mut a = OutputA {
        domain: &domain,
        flow: &out_a,
        next: None,
        checked: 0,
        skipped: 0,
    };
    let skipped = engine.counters().overrun_skipped_samples.load(Relaxed);

    producer.kill();
    let t = Instant::now();
    while state(1) != InputState::Pruned {
        assert!(t.elapsed() < 5 * SECOND, "input B not pruned");
        a.check(&rx);
        std::thread::sleep(SECOND / 10);
    }
    a.check(&rx);
    let _producer = spawn_producer(&domain, &src_b);
    let t = Instant::now();
    while state(1) != InputState::Attached {
        assert!(t.elapsed() < 10 * SECOND, "input B not re-added");
        a.check(&rx);
        std::thread::sleep(SECOND / 10);
    }
    let t = Instant::now();
    while t.elapsed() < SECOND {
        a.check(&rx);
        std::thread::sleep(SECOND / 10);
    }
    a.check(&rx);
    assert!(
        a.checked > 2_000,
        "{} blocks of output A checked",
        a.checked
    );
    // Every block not reported is an overrun skip.
    let skipped = engine.counters().overrun_skipped_samples.load(Relaxed) - skipped;
    assert_eq!(a.skipped * B, skipped);
    assert_eq!(state(0), InputState::Attached);
    common::stop(engine);
}

/// Checks the blocks of input A's output that the verdicts report since
/// the last call: every one written, input A copied without a fade, each
/// sample `0.5 pilot(0, n)`. Blocks between two reported ones are the
/// loop's overrun skips, zero-filled by libmxl.
struct OutputA<'a> {
    domain: &'a Domain,
    flow: &'a str,
    next: Option<u64>,
    checked: u64,
    skipped: u64,
}

impl OutputA<'_> {
    fn check(&mut self, rx: &std::sync::mpsc::Receiver<samplez::BlockVerdict>) {
        let vs: Vec<_> = rx.try_iter().collect();
        let (Some(first), Some(last)) = (vs.first(), vs.last()) else {
            return;
        };
        let mut processed = std::collections::HashSet::new();
        for v in &vs {
            let want = self.next.unwrap_or(v.k);
            assert!(v.k >= want, "block {} after block {want}", v.k);
            self.skipped += v.k - want;
            self.next = Some(v.k + 1);
            assert_eq!((v.silenced & 1, v.fading & 1), (0, 0), "{v:?}");
            assert_eq!(v.written & 1, 1, "{v:?}");
            processed.insert(v.k);
        }
        let out = common::read_blocks(&self.domain.0, self.flow, 1, first.k..last.k + 1);
        for (i, &got) in out.iter().enumerate() {
            let n = first.k * B + i as u64;
            let want = if processed.contains(&(n / B)) {
                0.5 * common::pilot(0, n)
            } else {
                0.0
            };
            assert!(exact(got, want), "index {n}: got {got:e}, want {want:e}");
        }
        self.checked += vs.len() as u64;
    }
}

/// The input's domain directory is removed: the poll instance cannot be
/// created when the input is pruned, so `poll` creates it once the
/// directory is back, and the input is re-added.
fn domain_vanishes() {
    let (a, b) = (Domain::new(), Domain::new());
    let (src, dst) = (new_id(), new_id());
    let pilot = Pilot::start(&a.0, &src, 1, B, 0);
    let mut cfg = config(&a.0, &[(&src, 1)], &[(&dst, 1)], vec![vec![1.0]], MARGIN);
    cfg.domains.push(samplez::config::Domain {
        name: "out".to_owned(),
        path: b.0.clone(),
    });
    cfg.outputs[0].domain = "out".to_owned();
    let (engine, _rx) = start(cfg);
    let attached = || engine.status().inputs[0].state == InputState::Attached;
    wait_for(5 * SECOND, "input attached", attached);

    drop(pilot);
    std::fs::remove_dir_all(&a.0).unwrap();
    wait_for(5 * SECOND, "input pruned", || !attached());
    std::thread::sleep(SECOND / 2);
    assert!(!attached());

    a.create();
    let _pilot = Pilot::start(&a.0, &src, 1, B, 0);
    wait_for(10 * SECOND, "input re-added", attached);
    assert!(engine.status().outputs[0].realised);
    common::stop(engine);
}

fn late_producer() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    // 25 ms commits against a 20 ms margin: the blocks in the first 5 ms
    // of each batch are late, the others on time.
    let batch = 1200;
    let _pilot = Pilot::start(&domain.0, &src, 1, batch, 0);
    let cfg = config(
        &domain.0,
        &[(&src, 1)],
        &[(&dst, 1)],
        vec![vec![0.5]],
        MARGIN,
    );
    let (engine, rx) = start(cfg);
    let mut checker = Checker::new(&domain.0, &dst, 1, &[0.5]);
    let mut warning = None;
    checker.run(&[&rx], 14 * SECOND, |_, _| {
        warning = warning.or(engine.status().inputs[0].margin_warning);
    });
    let s = engine.status();
    let input = &s.inputs[0];
    assert_eq!(
        input.state,
        InputState::Attached,
        "an advancing input is not pruned"
    );
    assert_eq!(
        input.margin_short,
        Some(batch + B),
        "attach check from the batch hint"
    );
    assert!(engine.counters().input_late_blocks.load(Relaxed) > 100);
    let c = checker.counts;
    assert!(c.silenced > 100 && c.ramp > 100 && c.clean > 100, "{c:?}");
    let required = warning.expect("no margin warning");
    // The worst late block ends 192 samples or more past a batch boundary.
    assert!(required >= MARGIN + 192 + B, "required margin {required}");
    common::stop(engine);
}

fn foreign_writer() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let api = common::api();
    let instance = Instance::new(&api, &domain.0).unwrap();
    let foreign = OutputConfig {
        flow_id: &dst,
        label: "foreign",
        group: "foreign",
        role: "Audio",
        channels: 1,
        block: B,
    };
    let Creation::Created { writer, .. } = output::create(&instance, &foreign).unwrap() else {
        panic!("foreign writer not created");
    };
    let cfg = config(
        &domain.0,
        &[(&src, 1)],
        &[(&dst, 1)],
        vec![vec![0.5]],
        MARGIN,
    );
    let (engine, rx) = start(cfg);
    wait_for(5 * SECOND, "a retry", || {
        engine.status().outputs[0].attempts >= 2
    });
    let out = &engine.status().outputs[0];
    assert!(!out.realised);
    assert!(out.reason.as_deref().unwrap().contains("live"), "{out:?}");

    // Releasing the foreign writer deletes its flow; the next retry
    // creates it.
    drop(writer);
    wait_for(10 * SECOND, "output realised", || {
        engine.status().outputs[0].realised
    });
    let mut checker = Checker::new(&domain.0, &dst, 1, &[0.5]);
    checker.run(&[&rx], 2 * SECOND, |_, _| {});
    assert!(checker.counts.clean > 1_000, "{:?}", checker.counts);
    common::stop(engine);
}

fn binary() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let file = domain.0.join("hop.json");
    let v = serde_json::json!({
        "libmxl": common::libmxl(),
        "domains": [{ "name": "d", "path": domain.0 }],
        "inputs": [{ "name": "in", "domain": "d", "flowId": src, "channels": 1 }],
        "outputs": [{ "name": "out", "domain": "d", "flowId": dst, "label": "hop out",
                      "group": "hop", "role": "Audio", "channels": 1 }],
        "matrix": [[1.0]],
        "rt": { "rtCpu": "none" },
    });
    std::fs::write(&file, v.to_string()).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_samplez-hop"))
        .arg("--config")
        .arg(&file)
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_for(5 * SECOND, "the output flow", || domain.has_flow(&dst));
    // SAFETY: kill with a live child pid and a valid signal.
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    assert!(child.wait().unwrap().success());
    assert!(
        !domain.has_flow(&dst),
        "the released writer deletes its flow"
    );
}

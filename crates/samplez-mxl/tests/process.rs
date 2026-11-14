//! Scenarios with a writer in another process that is SIGKILLed: the poll
//! reader of a pruned input, and output creation over a live, a stale and
//! an uncollectable flow. Runs without the test harness so that the binary
//! can re-execute itself as that writer (`process child <domain> <id>`).

mod common;

use std::io::{BufRead, BufReader, Write as _};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::process::{Child, Command, Stdio};

use common::{B, Domain, RawReader, RawWriter, api, new_id, sample};
use samplez_mxl::Instance;
use samplez_mxl::input::{Poll, PolledFlow};
use samplez_mxl::output::{self, Creation, OutputConfig, Write};

const K0: u64 = 3_000_000;
const BLOCKS: u64 = 20;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("child") {
        child(Path::new(&args[2]), &args[3]);
    }
    for (name, test) in [
        (
            "poller_recreates_its_reader_after_the_flow_is_replaced",
            poller_recreates_its_reader_after_the_flow_is_replaced as fn(),
        ),
        (
            "output_creation_over_a_live_stale_and_surviving_flow",
            output_creation_over_a_live_stale_and_surviving_flow,
        ),
    ] {
        test();
        println!("test {name} ... ok");
    }
}

/// Creates the flow, writes 0.5 over blocks K0 ..K0 + BLOCKS, says
/// "ready" and waits to be killed.
fn child(domain: &Path, id: &str) -> ! {
    let mut w = RawWriter::create(&api(), domain, id, 2);
    for k in K0..K0 + BLOCKS {
        w.write((k + 1) * B, B as usize, |_, _| 0.5);
    }
    let mut out = std::io::stdout();
    writeln!(out, "ready").unwrap();
    out.flush().unwrap();
    loop {
        std::thread::park();
    }
}

fn spawn_writer(domain: &Domain, id: &str) -> Child {
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
    child
}

fn kill(mut child: Child) {
    // Child::kill sends SIGKILL.
    child.kill().unwrap();
    child.wait().unwrap();
}

fn poller_recreates_its_reader_after_the_flow_is_replaced() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let child = spawn_writer(&domain, &id);
    let poll = Instance::new(&api, &domain.0).unwrap();
    let mut p = PolledFlow::new(&poll, &id).unwrap();
    let head = (K0 + BLOCKS) * B;
    assert!(matches!(p.poll(), Poll::Head(h) if h == head));

    kill(child);
    // A dead writer leaves a valid flow whose head stays.
    assert!(matches!(p.poll(), Poll::Head(h) if h == head));

    // libmxl's collection removes the stale flow and a new producer
    // creates the same id.
    let other = Instance::new(&api, &domain.0).unwrap();
    assert_eq!(other.is_flow_active(&id).unwrap(), Some(false));
    assert_eq!(other.garbage_collect(), mxl_sys::MXL_STATUS_OK);
    assert_eq!(other.is_flow_active(&id).unwrap(), None);
    let mut w = RawWriter::create(&api, &domain.0, &id, 2);
    w.blocks(K0 + BLOCKS..K0 + BLOCKS + 5);

    assert!(matches!(p.poll(), Poll::Invalid));
    assert!(matches!(p.poll(), Poll::Head(h) if h == head + 5 * B));
    w.blocks(K0 + BLOCKS + 5..K0 + BLOCKS + 6);
    assert!(matches!(p.poll(), Poll::Head(h) if h == head + 6 * B));
}

fn output_creation_over_a_live_stale_and_surviving_flow() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let child = spawn_writer(&domain, &id);
    let head = (K0 + BLOCKS) * B;
    let cfg = OutputConfig {
        flow_id: &id,
        label: "samplez test output",
        group: "samplez-test",
        role: "Audio",
        channels: 2,
        block: B,
    };

    // A live writer in another process: not realised, the flow untouched.
    let instance = Instance::new(&api, &domain.0).unwrap();
    assert_eq!(instance.is_flow_active(&id).unwrap(), Some(true));
    assert!(matches!(
        output::create(&instance, &cfg).unwrap(),
        Creation::Active
    ));
    assert!(domain.tmp_dirs().is_empty());
    {
        let r = RawReader::open(&api, &domain.0, &id);
        assert_eq!(r.head(), head);
        for chan in r.samples(head, (BLOCKS * B) as usize).unwrap() {
            assert!(chan.iter().all(|&v| v == 0.5));
        }
    }

    kill(child);
    assert_eq!(instance.is_flow_active(&id).unwrap(), Some(false));

    // A flow directory without write permission: libmxl's collection cannot
    // remove its files, so the stale flow survives. It is reported and no
    // create is attempted, which would leave a .mxl-tmp-* directory. Root
    // ignores the mode; the builder runs the tests as a non-root uid.
    assert_ne!(
        std::fs::metadata(&domain.0).unwrap().uid(),
        0,
        "needs a non-root uid"
    );
    let dir = domain.flow_dir(&id);
    let mode = |m| std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(m)).unwrap();
    mode(0o555);
    let survived = output::create(&instance, &cfg).unwrap();
    let tmp = domain.tmp_dirs();
    let active = instance.is_flow_active(&id).unwrap();
    mode(0o755);
    assert!(matches!(
        survived,
        Creation::Survived {
            gc: mxl_sys::MXL_STATUS_OK
        }
    ));
    assert!(tmp.is_empty(), "{tmp:?}");
    assert_eq!(active, Some(false));

    // Collectable again: removed by mxlGarbageCollectFlows, then created.
    let Creation::Created {
        writer: mut w,
        collected: true,
    } = output::create(&instance, &cfg).unwrap()
    else {
        panic!("stale flow not collected and created");
    };
    assert!(domain.tmp_dirs().is_empty(), "{:?}", domain.tmp_dirs());
    let k = K0 + BLOCKS + 3;
    let data: Vec<f32> = (0..2)
        .flat_map(|c| (k * B..(k + 1) * B).map(move |i| sample(c, i)))
        .collect();
    assert_eq!(w.write_block(k, &data), Write::Committed);
    let r = RawReader::open(&api, &domain.0, &id);
    assert_eq!(r.head(), (k + 1) * B);
    let got = r.samples((k + 1) * B, (4 * B) as usize).unwrap();
    for (c, chan) in got.iter().enumerate() {
        // The new flow: zeros where the killed writer had written 0.5.
        assert!(chan[..3 * B as usize].iter().all(|&v| v == 0.0));
        assert_eq!(
            chan[3 * B as usize..],
            data[c * B as usize..(c + 1) * B as usize]
        );
    }
}

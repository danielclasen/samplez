//! The output path, read back by an independent libmxl reader.

mod common;

use std::sync::Arc;

use common::{B, Domain, RawReader, RawWriter, api, new_id, sample};
use samplez_mxl::Instance;
use samplez_mxl::output::{self, Creation, FlowWriter, OutputConfig, Write};

const K0: u64 = 2_000_000;

fn config(flow_id: &str, channels: u32) -> OutputConfig<'_> {
    OutputConfig {
        flow_id,
        label: "samplez test output",
        group: "samplez-test",
        role: "Audio",
        channels,
        block: B,
    }
}

fn created(instance: &Arc<Instance>, flow_id: &str, channels: u32) -> FlowWriter {
    match output::create(instance, &config(flow_id, channels)).unwrap() {
        Creation::Created {
            writer,
            collected: false,
        } => writer,
        _ => panic!("flow {flow_id} not created"),
    }
}

fn write_blocks(w: &mut FlowWriter, ks: std::ops::Range<u64>) {
    let ch = w.channels();
    let n = B as usize;
    let mut buf = vec![0.0; ch * n];
    for k in ks {
        for c in 0..ch {
            for i in 0..n {
                buf[c * n + i] = sample(c, k * B + i as u64);
            }
        }
        assert_eq!(w.write_block(k, &buf), Write::Committed);
    }
}

#[test]
fn blocks_are_read_back_at_the_indices_written() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let instance = Instance::new(&api, &domain.0).unwrap();
    let mut w = created(&instance, &id, 3);
    assert_eq!(w.hints(), (B as u32, B as u32));
    write_blocks(&mut w, K0..K0 + 10);

    let r = RawReader::open(&api, &domain.0, &id);
    assert_eq!(r.head(), (K0 + 10) * B);
    for k in K0..K0 + 10 {
        let got = r.samples((k + 1) * B, B as usize).unwrap();
        for (ch, chan) in got.iter().enumerate() {
            let want: Vec<f32> = (k * B..(k + 1) * B).map(|i| sample(ch, i)).collect();
            assert_eq!(chan, &want, "block {k} ch {ch}");
        }
    }
}

#[test]
fn a_new_flow_reads_as_silence_before_the_first_block() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let instance = Instance::new(&api, &domain.0).unwrap();
    let mut w = created(&instance, &id, 2);
    assert_eq!(w.last_commit(), None);
    write_blocks(&mut w, K0..K0 + 1);
    // libmxl created the ring zero-filled; nothing was primed.
    let r = RawReader::open(&api, &domain.0, &id);
    let half = (w.buffer_length() / 2) as usize;
    for chan in r.samples(K0 * B, half - B as usize).unwrap() {
        assert!(chan.iter().all(|&v| v == 0.0));
    }
}

#[test]
fn blocks_below_the_last_commit_are_held() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let instance = Instance::new(&api, &domain.0).unwrap();
    let mut w = created(&instance, &id, 1);
    let data = vec![1.0; B as usize];
    assert_eq!(w.write_block(K0, &data), Write::Committed);
    assert_eq!(w.write_block(K0, &data), Write::Held);
    assert_eq!(w.write_block(K0 - 7, &data), Write::Held);
    assert_eq!(w.held_blocks(), 2);
    // A skip forward is written; libmxl zero-fills the gap.
    assert_eq!(w.write_block(K0 + 5, &data), Write::Committed);
    assert_eq!(w.last_commit(), Some((K0 + 6) * B));
    let r = RawReader::open(&api, &domain.0, &id);
    let got = r.samples((K0 + 6) * B, 6 * B as usize).unwrap();
    let n = B as usize;
    assert!(got[0][..n].iter().all(|&v| v == 1.0));
    assert!(got[0][n..5 * n].iter().all(|&v| v == 0.0));
    assert!(got[0][5 * n..].iter().all(|&v| v == 1.0));
    assert_eq!(
        w.write_block(K0 + 6, &data[..1]),
        Write::Failed(mxl_sys::MXL_ERR_INVALID_ARG)
    );
}

#[test]
fn release_deletes_the_flow() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let instance = Instance::new(&api, &domain.0).unwrap();
    let w = created(&instance, &id, 2);
    assert_eq!(instance.is_flow_active(&id).unwrap(), Some(true));
    drop(w);
    assert_eq!(instance.is_flow_active(&id).unwrap(), None);
}

#[test]
fn a_live_writer_of_another_instance_is_left_alone() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let mut other = RawWriter::create(&api, &domain.0, &id, 2);
    other.blocks(K0..K0 + 2);

    let instance = Instance::new(&api, &domain.0).unwrap();
    assert!(matches!(
        output::create(&instance, &config(&id, 2)).unwrap(),
        Creation::Active
    ));
    assert!(domain.tmp_dirs().is_empty());
    other.blocks(K0 + 2..K0 + 3);
    let r = RawReader::open(&api, &domain.0, &id);
    assert_eq!(r.head(), (K0 + 3) * B);
    assert_eq!(
        r.samples((K0 + 3) * B, 1).unwrap()[1][0],
        sample(1, (K0 + 3) * B - 1)
    );
}

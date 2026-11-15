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
        batch: B,
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
    // Blocks around a ring wrap that falls inside a block, so libmxl hands
    // out a second fragment for it.
    let len = w.buffer_length();
    let wrap = (K0 * B / len + 1..)
        .map(|m| m * len)
        .find(|w| !w.is_multiple_of(B))
        .unwrap();
    let k0 = wrap / B - 5;
    write_blocks(&mut w, k0..k0 + 10);

    let r = RawReader::open(&api, &domain.0, &id);
    assert_eq!(r.head(), (k0 + 10) * B);
    for k in k0..k0 + 10 {
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

/// A range of several blocks and a partial block, from a planar buffer
/// with a longer stride, is read back at its indices in one commit; the
/// batch hints stay at B.
#[test]
fn a_range_is_written_in_one_commit() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let instance = Instance::new(&api, &domain.0).unwrap();
    let cfg = OutputConfig {
        batch: 20 * B,
        ..config(&id, 2)
    };
    let Creation::Created { writer: mut w, .. } = output::create(&instance, &cfg).unwrap() else {
        panic!("flow {id} not created");
    };
    assert_eq!(w.hints(), (B as u32, B as u32));
    let stride = 20 * B as usize;
    let start = K0 * B + 7;
    let len = 3 * B as usize + 5;
    let mut buf = vec![f32::NAN; 2 * stride];
    for c in 0..2 {
        for i in 0..len {
            buf[c * stride + i] = sample(c, start + i as u64);
        }
    }
    assert_eq!(w.write(start, len, &buf, stride), Write::Committed);
    assert_eq!(w.last_commit(), Some(start + len as u64));
    let r = RawReader::open(&api, &domain.0, &id);
    assert_eq!(r.head(), start + len as u64);
    let got = r.samples(start + len as u64, len).unwrap();
    for (c, chan) in got.iter().enumerate() {
        let want: Vec<f32> = (start..start + len as u64).map(|i| sample(c, i)).collect();
        assert_eq!(chan, &want, "ch {c}");
    }
    // A range longer than its stride is refused.
    assert!(matches!(
        w.write(start + len as u64, stride + 1, &buf, stride),
        Write::Failed(_)
    ));
}

/// A batch of exactly half the flow's ring is accepted and a write that
/// long commits; one sample more is refused at creation, and the flow is
/// released with its writer.
#[test]
fn a_batch_above_half_the_ring_is_refused() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let instance = Instance::new(&api, &domain.0).unwrap();
    let len = created(&instance, &new_id(), 1).buffer_length();
    let half = OutputConfig {
        batch: len / 2,
        ..config(&id, 1)
    };
    let Creation::Created { writer: mut w, .. } = output::create(&instance, &half).unwrap() else {
        panic!("a batch of half the ring was refused");
    };
    let data = vec![0.5f32; (len / 2) as usize];
    assert_eq!(
        w.write(K0 * B, data.len(), &data, data.len()),
        Write::Committed
    );
    drop(w);
    let cfg = OutputConfig {
        batch: len / 2 + 1,
        ..config(&id, 1)
    };
    let Err(samplez_mxl::Error::Batch {
        batch,
        buffer_length,
    }) = output::create(&instance, &cfg)
    else {
        panic!("a batch of {} samples was accepted", cfg.batch);
    };
    assert_eq!((batch, buffer_length), (cfg.batch, len));
    assert_eq!(instance.is_flow_active(&id).unwrap(), None);
}

/// A range across the ring's end, from a buffer whose stride is longer
/// than the range, is read back at its indices.
#[test]
fn a_range_across_the_ring_end_is_read_back() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let instance = Instance::new(&api, &domain.0).unwrap();
    let cfg = OutputConfig {
        batch: 20 * B,
        ..config(&id, 2)
    };
    let Creation::Created { writer: mut w, .. } = output::create(&instance, &cfg).unwrap() else {
        panic!("flow {id} not created");
    };
    let ring = w.buffer_length();
    let stride = 20 * B as usize;
    let len = 3 * B as usize + 5;
    // Starts 50 samples before a ring boundary.
    let start = 5_000 * ring - 50;
    let mut buf = vec![f32::NAN; 2 * stride];
    for c in 0..2 {
        for i in 0..len {
            buf[c * stride + i] = sample(c, start + i as u64);
        }
    }
    assert_eq!(w.write(start, len, &buf, stride), Write::Committed);
    let r = RawReader::open(&api, &domain.0, &id);
    let got = r.samples(start + len as u64, len).unwrap();
    for (c, chan) in got.iter().enumerate() {
        let want: Vec<f32> = (start..start + len as u64).map(|i| sample(c, i)).collect();
        assert_eq!(chan, &want, "ch {c}");
    }
}

/// An empty range and a range longer than the batch are refused and move
/// nothing: libmxl would commit an empty range, zero-filling the gap
/// before it and moving the head over it.
#[test]
fn an_empty_or_too_long_range_is_refused() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let instance = Instance::new(&api, &domain.0).unwrap();
    let cfg = OutputConfig {
        batch: 2 * B,
        ..config(&id, 2)
    };
    let Creation::Created { writer: mut w, .. } = output::create(&instance, &cfg).unwrap() else {
        panic!("flow {id} not created");
    };
    let data = vec![0.5f32; 2 * 3 * B as usize];
    assert_eq!(w.write_block(K0, &data[..2 * B as usize]), Write::Committed);
    let end = w.last_commit().unwrap();
    let r = RawReader::open(&api, &domain.0, &id);
    let stride = 3 * B as usize;
    for (start, len) in [(end + 1_000, 0), (end, 3 * B as usize)] {
        let result = w.write(start, len, &data, stride);
        assert!(matches!(result, Write::Failed(_)), "{len}: {result:?}");
        assert_eq!((w.last_commit(), r.head()), (Some(end), end), "{len}");
    }
}

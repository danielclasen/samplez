//! Writing MXL audio flows: the flow definition, creation on the
//! supervisor side and the RT block write.

use std::ffi::CString;
use std::sync::Arc;

use crate::{Error, Instance, Result, SAMPLE_RATE, Status, check};

#[derive(Clone, Copy, Debug)]
pub struct OutputConfig<'a> {
    pub flow_id: &'a str,
    /// Names the samplez instance and the output.
    pub label: &'a str,
    /// The group hint's group name and role, `<group>:<role>`.
    pub group: &'a str,
    pub role: &'a str,
    pub channels: u32,
    /// Block size B in samples, also both batch size hints.
    pub block: u64,
}

/// The IS-04 Flow definition with the fields libmxl's flow parser reads for
/// an audio flow, including the group hint tag it requires.
pub fn flow_definition(c: &OutputConfig) -> Result<String> {
    crate::validate_id(c.flow_id)?;
    let bad = |s: &str| s.is_empty() || s.contains(':');
    if c.label.is_empty() || bad(c.group) || bad(c.role) || c.channels == 0 {
        return Err(Error::Format(format!(
            "output {}: label, group and role must be non-empty, group and role \
             without ':', channels at least 1",
            c.flow_id
        )));
    }
    Ok(serde_json::json!({
        "id": c.flow_id,
        "label": c.label,
        "format": "urn:x-nmos:format:audio",
        "media_type": "audio/float32",
        "sample_rate": { "numerator": SAMPLE_RATE },
        "channel_count": c.channels,
        "bit_depth": 32,
        "tags": { "urn:x-nmos:tag:grouphint/v1.0": [format!("{}:{}", c.group, c.role)] },
    })
    .to_string())
}

/// The outcome of [`create`].
pub enum Creation {
    /// A new flow and its writer. `collected` is true when a stale flow
    /// with the id was there and `mxlGarbageCollectFlows` removed it.
    Created { writer: FlowWriter, collected: bool },
    /// Another writer holds the flow; nothing was created.
    Active,
    /// libmxl opened an existing flow instead of creating one
    /// (`was_created == false`): another writer won the race between the
    /// check and the create. The writer was released. libmxl v1.1.0 leaves
    /// a `.mxl-tmp-*` directory in the domain in this case (upstream #776).
    Raced,
    /// A stale flow survived `mxlGarbageCollectFlows`, which returned
    /// `gc`, for instance one of another uid that the collection cannot
    /// open. No create was attempted: a create on an existing flow leaks a
    /// temporary directory in libmxl v1.1.0 (#776).
    Survived { gc: Status },
}

/// Supervisor side. Creates the output flow, never adopting an existing
/// one:
/// - `mxlIsFlowActive` reports no flow: create, requiring `was_created`;
/// - an active writer: [`Creation::Active`];
/// - an inactive (stale) flow: `mxlGarbageCollectFlows`, then
///   `mxlIsFlowActive` again; create only if the flow is gone now,
///   otherwise [`Creation::Active`] or [`Creation::Survived`].
///
/// samplez itself deletes and edits no file in the domain. The writer's
/// create options set `maxCommitBatchSizeHint` and `maxSyncBatchSizeHint`
/// to B; a new flow is zero-filled by libmxl, so nothing is written before
/// the first block.
///
/// libmxl v1.1.0's `mxlIsFlowActive` reports `MXL_ERR_FLOW_NOT_FOUND` for
/// any failure to open the flow's data file. An existing flow this process
/// cannot open therefore reads as absent, and the create that follows
/// fails on it: `Err(Error::Mxl { call: "mxlCreateFlowWriter", .. })`,
/// which the caller counts as a create failure.
pub fn create(instance: &Arc<Instance>, c: &OutputConfig) -> Result<Creation> {
    let def = flow_definition(c)?;
    if c.block == 0 {
        return Err(Error::Format(format!("output {}: block size 0", c.flow_id)));
    }
    let mut collected = false;
    match instance.is_flow_active(c.flow_id)? {
        None => {}
        Some(true) => return Ok(Creation::Active),
        Some(false) => {
            let gc = instance.garbage_collect();
            match instance.is_flow_active(c.flow_id)? {
                None => collected = true,
                Some(true) => return Ok(Creation::Active),
                Some(false) => return Ok(Creation::Survived { gc }),
            }
        }
    }
    create_writer(instance, c, def, collected)
}

/// `mxlCreateFlowWriter` with the batch size hints at B; an existing flow
/// it opens (`was_created == false`) is released as [`Creation::Raced`].
fn create_writer(
    instance: &Arc<Instance>,
    c: &OutputConfig,
    def: String,
    collected: bool,
) -> Result<Creation> {
    let c_def = CString::new(def).expect("JSON from serde_json has no NUL");
    let options = serde_json::json!({
        "maxCommitBatchSizeHint": c.block,
        "maxSyncBatchSizeHint": c.block,
    })
    .to_string();
    let c_options = CString::new(options).expect("JSON from serde_json has no NUL");
    let mut raw = std::ptr::null_mut();
    let mut info = mxl_sys::FlowConfigInfo::default();
    let mut created = false;
    // SAFETY: both strings are NUL-terminated and outlive the call; `raw`,
    // `info` and `created` are writable.
    check("mxlCreateFlowWriter", unsafe {
        instance.api().create_flow_writer(
            instance.raw(),
            c_def.as_ptr(),
            c_options.as_ptr(),
            &mut raw,
            &mut info,
            &mut created,
        )
    })?;
    // SAFETY: both union members are structs of integers, so any bytes are
    // a valid value; libmxl fills `continuous` for the audio flow created.
    let cont = unsafe { info.__bindgen_anon_1.continuous };
    let writer = FlowWriter {
        instance: Arc::clone(instance),
        raw,
        buffer_length: u64::from(cont.bufferLength),
        channels: cont.channelCount as usize,
        block: c.block,
        commit_hint: info.common.maxCommitBatchSizeHint,
        sync_hint: info.common.maxSyncBatchSizeHint,
        last_commit: None,
        held_blocks: 0,
    };
    if !created {
        drop(writer);
        return Ok(Creation::Raced);
    }
    if c.block > writer.buffer_length / 2 {
        return Err(Error::Block {
            block: c.block,
            buffer_length: writer.buffer_length,
        });
    }
    Ok(Creation::Created { writer, collected })
}

/// The outcome of one [`FlowWriter::write_block`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Write {
    Committed,
    /// The block starts below the last commit and is not written.
    Held,
    /// libmxl refused the open or the commit, or `data` is shorter than
    /// channels x B (`MXL_ERR_INVALID_ARG`).
    Failed(Status),
}

/// A libmxl writer on a flow samplez created. Dropping it releases the
/// writer, and libmxl deletes the flow when no other writer holds it. The
/// RT thread hands a writer back before it is dropped; it must not be
/// dropped on the RT thread.
pub struct FlowWriter {
    instance: Arc<Instance>,
    raw: mxl_sys::FlowWriter,
    buffer_length: u64,
    channels: usize,
    block: u64,
    commit_hint: u32,
    sync_hint: u32,
    last_commit: Option<u64>,
    held_blocks: u64,
}

// SAFETY: the writer handle stays valid until Drop; one thread at a time
// owns the wrapper, and open and commit touch only the writer and its
// shared memory.
unsafe impl Send for FlowWriter {}

impl FlowWriter {
    pub fn buffer_length(&self) -> u64 {
        self.buffer_length
    }

    pub fn channels(&self) -> usize {
        self.channels
    }

    /// The effective (`maxCommitBatchSizeHint`, `maxSyncBatchSizeHint`), as
    /// libmxl reported them at creation.
    pub fn hints(&self) -> (u32, u32) {
        (self.commit_hint, self.sync_hint)
    }

    /// The exclusive end of the last commit.
    pub fn last_commit(&self) -> Option<u64> {
        self.last_commit
    }

    pub fn held_blocks(&self) -> u64 {
        self.held_blocks
    }

    /// RT. Writes block `k`, `[kB, (k + 1)B)`, from `data`, planar with B
    /// samples per channel: `mxlFlowWriterOpenSamples((k + 1)B, B)`, both
    /// fragments filled, `mxlFlowWriterCommitSamples`. A block starting
    /// below the last commit is held and counted; libmxl zero-fills a
    /// forward gap on commit.
    pub fn write_block(&mut self, k: u64, data: &[f32]) -> Write {
        let b = self.block;
        let Some((start, end)) = k
            .checked_mul(b)
            .and_then(|start| Some((start, start.checked_add(b)?)))
        else {
            return Write::Failed(mxl_sys::MXL_ERR_INVALID_ARG);
        };
        if self.last_commit.is_some_and(|last| start < last) {
            self.held_blocks += 1;
            return Write::Held;
        }
        let n = b as usize;
        if data.len() < self.channels * n {
            return Write::Failed(mxl_sys::MXL_ERR_INVALID_ARG);
        }
        let api = self.instance.api();
        let mut slice = mxl_sys::MutableWrappedMultiBufferSlice::default();
        // SAFETY: live writer, writable slice.
        let status = unsafe { api.flow_writer_open_samples(self.raw, end, n, &mut slice) };
        if status != mxl_sys::MXL_STATUS_OK {
            return Write::Failed(status);
        }
        let [f0, f1] = slice.base.fragments;
        let width = size_of::<f32>();
        for (ch, src) in data.chunks_exact(n).take(slice.count).enumerate() {
            let (a, rest) = src.split_at((f0.size / width).min(n));
            let second = (f1.size / width).min(rest.len());
            // SAFETY: libmxl returned two writable fragments of `f0.size`
            // and `f1.size` bytes inside channel 0 of the mapped ring, and
            // channel `ch < slice.count` lies `ch * stride` bytes on. At most
            // those sizes are copied, from `src`, which holds `n` samples.
            // Bytes are copied, so no alignment is assumed.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    a.as_ptr().cast::<u8>(),
                    f0.pointer.cast::<u8>().add(ch * slice.stride),
                    size_of_val(a),
                );
                std::ptr::copy_nonoverlapping(
                    rest.as_ptr().cast::<u8>(),
                    f1.pointer.cast::<u8>().add(ch * slice.stride),
                    second * width,
                );
            }
        }
        // SAFETY: live writer with samples open.
        let status = unsafe { api.flow_writer_commit_samples(self.raw) };
        if status != mxl_sys::MXL_STATUS_OK {
            return Write::Failed(status);
        }
        self.last_commit = Some(end);
        Write::Committed
    }
}

impl Drop for FlowWriter {
    fn drop(&mut self) {
        // SAFETY: `raw` came from mxlCreateFlowWriter on this instance and is
        // released exactly once, here.
        unsafe {
            self.instance
                .api()
                .release_flow_writer(self.instance.raw(), self.raw)
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "5fbec3b1-1b0f-417d-9059-8b94a47197ed";

    fn config() -> OutputConfig<'static> {
        OutputConfig {
            flow_id: ID,
            label: "Mix A",
            group: "foldz-1",
            role: "Audio 1",
            channels: 6,
            block: 48,
        }
    }

    #[test]
    fn flow_definition_has_the_fields_libmxl_reads() {
        let v: serde_json::Value =
            serde_json::from_str(&flow_definition(&config()).unwrap()).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "id": ID,
                "label": "Mix A",
                "format": "urn:x-nmos:format:audio",
                "media_type": "audio/float32",
                "sample_rate": { "numerator": 48000 },
                "channel_count": 6,
                "bit_depth": 32,
                "tags": { "urn:x-nmos:tag:grouphint/v1.0": ["foldz-1:Audio 1"] },
            })
        );
    }

    /// `create_writer` on a flow another writer holds: libmxl opens it with
    /// `was_created == false`, and the writer is released, not adopted.
    /// Against the real libmxl, which the builder image carries.
    #[test]
    fn an_opened_existing_flow_is_released_as_raced() {
        let api = crate::load(std::path::Path::new("/opt/mxl/lib/libmxl.so")).unwrap();
        let domain = std::env::temp_dir().join(format!("samplez-mxl-raced-{}", std::process::id()));
        std::fs::create_dir_all(&domain).unwrap();
        let owner = Instance::new(&api, &domain).unwrap();
        let Creation::Created { writer: mut w, .. } = create(&owner, &config()).unwrap() else {
            panic!("flow not created");
        };
        assert_eq!(w.write_block(1_000, &[0.5; 6 * 48]), Write::Committed);

        let other = Instance::new(&api, &domain).unwrap();
        let def = flow_definition(&config()).unwrap();
        assert!(matches!(
            create_writer(&other, &config(), def, false).unwrap(),
            Creation::Raced
        ));
        // The owner's flow is intact and still active.
        assert_eq!(other.is_flow_active(ID).unwrap(), Some(true));
        assert_eq!(w.write_block(1_001, &[0.5; 6 * 48]), Write::Committed);
        drop(w);
        drop(other);
        drop(owner);
        std::fs::remove_dir_all(&domain).unwrap();
    }

    #[test]
    fn flow_definition_refuses_what_libmxl_would() {
        for c in [
            OutputConfig {
                label: "",
                ..config()
            },
            OutputConfig {
                group: "",
                ..config()
            },
            OutputConfig {
                role: "a:b",
                ..config()
            },
            OutputConfig {
                group: "a:b",
                ..config()
            },
            OutputConfig {
                channels: 0,
                ..config()
            },
            OutputConfig {
                flow_id: "not-a-uuid",
                ..config()
            },
        ] {
            assert!(flow_definition(&c).is_err());
        }
    }
}

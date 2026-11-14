//! Reading MXL audio flows: input generations built on the supervisor
//! side, the RT block read, failed-input identification and the poller for
//! a pruned input.
//!
//! None of the types here may be dropped on the RT thread: dropping a
//! reader or group releases it in libmxl (munmap, instance mutex), and
//! dropping a generation can destroy its instance.

use std::sync::Arc;

use mxl_sys::{
    MXL_DATA_FORMAT_AUDIO, MXL_ERR_FLOW_INVALID, MXL_ERR_OUT_OF_RANGE_TOO_EARLY,
    MXL_ERR_OUT_OF_RANGE_TOO_LATE, MXL_STATUS_OK, WrappedMultiBufferSlice,
};

use crate::time::{RATE, timestamp_to_index};
use crate::{Error, Instance, MxlApi, Result, Status, check, id_cstring};

/// Consecutive late blocks with a frozen head after which an input is
/// probed for validity.
pub const FROZEN_PROBE_BLOCKS: u32 = 10;

/// How long a polled head must stay on time before the input is added back.
pub const ON_TIME_NS: u64 = 1_000_000_000;

/// The block timeline shared by every input.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    /// Block size B in samples.
    pub block: u64,
    /// Read delay `readerMarginSamples`.
    pub margin: u64,
    pub max_catchup_blocks: u64,
}

/// One input slot's configuration.
#[derive(Clone, Copy, Debug)]
pub struct InputConfig<'a> {
    pub flow_id: &'a str,
    pub channels: u32,
    /// `offsetSamples`.
    pub offset: i64,
}

/// One reference to libmxl's reader for a flow on an instance. libmxl
/// shares one reader object per flow id within an instance and counts
/// references to it; dropping this releases one reference, the last one
/// unmaps the flow and removes the reader from every group of the
/// instance. Must not be dropped on the RT thread.
pub struct FlowReader {
    instance: Arc<Instance>,
    raw: mxl_sys::FlowReader,
}

// SAFETY: the reader handle stays valid until Drop; one thread at a time
// owns the wrapper, and libmxl's read calls touch only the reader and its
// shared memory.
unsafe impl Send for FlowReader {}

impl FlowReader {
    /// `mxlCreateFlowReader`.
    pub fn open(instance: &Arc<Instance>, flow_id: &str) -> Result<FlowReader> {
        let id = id_cstring(flow_id)?;
        let mut raw = std::ptr::null_mut();
        // SAFETY: valid instance, NUL-terminated id, a null options pointer
        // and a writable out pointer.
        check("mxlCreateFlowReader", unsafe {
            instance.api().create_flow_reader(
                instance.raw(),
                id.as_ptr(),
                std::ptr::null(),
                &mut raw,
            )
        })?;
        Ok(FlowReader {
            instance: Arc::clone(instance),
            raw,
        })
    }

    pub fn raw(&self) -> mxl_sys::FlowReader {
        self.raw
    }

    /// `mxlFlowReaderGetConfigInfo`.
    pub fn config(&self) -> Result<mxl_sys::FlowConfigInfo> {
        let mut info = mxl_sys::FlowConfigInfo::default();
        // SAFETY: live reader, writable info.
        check("mxlFlowReaderGetConfigInfo", unsafe {
            self.instance
                .api()
                .flow_reader_get_config_info(self.raw, &mut info)
        })?;
        Ok(info)
    }

    /// RT. The head index from `mxlFlowReaderGetRuntimeInfo`.
    pub fn head(&self) -> std::result::Result<u64, Status> {
        let mut info = mxl_sys::FlowRuntimeInfo::default();
        // SAFETY: live reader, writable info.
        let status = unsafe {
            self.instance
                .api()
                .flow_reader_get_runtime_info(self.raw, &mut info)
        };
        if status == MXL_STATUS_OK {
            Ok(info.headIndex)
        } else {
            Err(status)
        }
    }

    /// RT. `mxlFlowReaderGetSamplesNonBlocking(end, count)`. The slices
    /// point into the flow's ring and are not to be held.
    fn samples(&self, end: u64, count: usize, slice: &mut WrappedMultiBufferSlice) -> Status {
        // SAFETY: live reader, writable slice.
        unsafe {
            self.instance
                .api()
                .flow_reader_get_samples_non_blocking(self.raw, end, count, slice)
        }
    }

    /// RT, failure path. `mxlFlowReaderGetSamplesNonBlocking(head + 1, 1)`:
    /// a range beyond the head, for which libmxl runs its flow validity
    /// check (one `stat()` and path allocations).
    fn probe(&self, head: u64) -> Status {
        let mut slice = WrappedMultiBufferSlice::default();
        self.samples(head + 1, 1, &mut slice)
    }
}

impl Drop for FlowReader {
    fn drop(&mut self) {
        // SAFETY: `raw` came from mxlCreateFlowReader on this instance and
        // this reference is released exactly once, here.
        unsafe {
            self.instance
                .api()
                .release_flow_reader(self.instance.raw(), self.raw)
        };
    }
}

/// A libmxl flow synchronization group. Must not be dropped on the RT
/// thread.
pub struct SyncGroup {
    instance: Arc<Instance>,
    raw: mxl_sys::FlowSynchronizationGroup,
}

// SAFETY: the group handle stays valid until Drop and one thread at a time
// owns the wrapper; libmxl's group has no lock of its own.
unsafe impl Send for SyncGroup {}

impl SyncGroup {
    /// `mxlCreateFlowSynchronizationGroup`.
    pub fn new(instance: &Arc<Instance>) -> Result<SyncGroup> {
        let mut raw = std::ptr::null_mut();
        // SAFETY: valid instance, writable out pointer.
        check("mxlCreateFlowSynchronizationGroup", unsafe {
            instance
                .api()
                .create_flow_synchronization_group(instance.raw(), &mut raw)
        })?;
        Ok(SyncGroup {
            instance: Arc::clone(instance),
            raw,
        })
    }

    /// `mxlFlowSynchronizationGroupAddReader`. The reader must belong to the
    /// group's instance: libmxl removes a released reader only from the
    /// groups of its own instance.
    pub fn add(&mut self, reader: &FlowReader) -> Result<()> {
        if !Arc::ptr_eq(&self.instance, &reader.instance) {
            return Err(Error::Format(
                "reader and group belong to different instances".to_owned(),
            ));
        }
        // SAFETY: live group and reader on the same instance.
        check("mxlFlowSynchronizationGroupAddReader", unsafe {
            self.instance
                .api()
                .flow_synchronization_group_add_reader(self.raw, reader.raw)
        })
    }

    /// RT. `mxlFlowSynchronizationGroupWaitForDataAt`: OK once every
    /// reader's head reached the index of `timestamp`;
    /// `MXL_ERR_OUT_OF_RANGE_TOO_EARLY` when `timeout_ns` passed first;
    /// `MXL_ERR_FLOW_INVALID` for the first reader found on a replaced flow.
    /// Blocks on a futex while a reader is behind.
    pub fn wait(&self, timestamp: u64, timeout_ns: u64) -> Status {
        // SAFETY: live group; its readers are live while this wrapper's
        // generation holds them.
        unsafe {
            self.instance
                .api()
                .flow_synchronization_group_wait_for_data_at(self.raw, timestamp, timeout_ns)
        }
    }
}

impl Drop for SyncGroup {
    fn drop(&mut self) {
        // SAFETY: `raw` came from mxlCreateFlowSynchronizationGroup on this
        // instance and is released exactly once, here.
        unsafe {
            self.instance
                .api()
                .release_flow_synchronization_group(self.instance.raw(), self.raw)
        };
    }
}

/// One input generation: an instance of its own, a reader reference per
/// attached slot and one synchronization group holding every reader. Built
/// on the supervisor side and handed to the RT thread whole; no libmxl call
/// touches its instance while the RT thread uses it. Must not be dropped
/// on the RT thread.
pub struct Generation {
    group: SyncGroup,
    inputs: Vec<Option<Input>>,
    max_offset: i64,
    instance: Arc<Instance>,
}

/// An attached input slot.
pub struct Input {
    reader: FlowReader,
    block: u64,
    offset: i64,
}

impl Generation {
    /// A generation with `slots` empty input slots, on a new instance.
    pub fn new(api: &Arc<MxlApi>, domain: &std::path::Path, slots: usize) -> Result<Generation> {
        let instance = Instance::new(api, domain)?;
        Ok(Generation {
            group: SyncGroup::new(&instance)?,
            inputs: (0..slots).map(|_| None).collect(),
            max_offset: 0,
            instance,
        })
    }

    pub fn instance(&self) -> &Arc<Instance> {
        &self.instance
    }

    /// Opens a reader reference for `slot`, checks the flow and adds the
    /// reader to the group. Refuses a flow whose configuration or
    /// definition is not 48 kHz float32 audio with `cfg.channels` channels,
    /// or whose readable history is shorter than the read window
    /// `(maxCatchupBlocks + 2)B + margin + |offset|`. Returns the margin the
    /// producer needs when `timing.margin` is below
    /// `offset + maxCommitBatchSizeHint + B`.
    pub fn attach(
        &mut self,
        slot: usize,
        cfg: &InputConfig,
        timing: &Timing,
    ) -> Result<Option<u64>> {
        if slot >= self.inputs.len() {
            return Err(Error::Format(format!("no input slot {slot}")));
        }
        let reader = FlowReader::open(&self.instance, cfg.flow_id)?;
        let info = reader.config()?;
        // SAFETY: both union members are structs of integers, so any bytes
        // are a valid value; libmxl fills `continuous` for audio, and the
        // values are used only once the format is checked to be audio.
        let cont = unsafe { info.__bindgen_anon_1.continuous };
        let rate = info.common.grainRate;
        if info.common.format != MXL_DATA_FORMAT_AUDIO
            || rate.denominator == 0
            || rate.numerator * RATE.denominator != RATE.numerator * rate.denominator
            || cont.channelCount != cfg.channels
        {
            return Err(Error::Format(format!(
                "flow {}: format {}, rate {}/{}, {} channels; expected audio, 48000/1, {}",
                cfg.flow_id,
                info.common.format,
                rate.numerator,
                rate.denominator,
                cont.channelCount,
                cfg.channels
            )));
        }
        check_flow_def(&self.instance.flow_def(cfg.flow_id)?, cfg)?;

        let buffer_length = u64::from(cont.bufferLength);
        let required = (timing.max_catchup_blocks + 2) * timing.block
            + timing.margin
            + cfg.offset.unsigned_abs();
        if required > buffer_length / 2 {
            return Err(Error::Window {
                required,
                buffer_length,
            });
        }
        let needed = i128::from(cfg.offset)
            + i128::from(info.common.maxCommitBatchSizeHint)
            + i128::from(timing.block);
        let short = (i128::from(timing.margin) < needed).then_some(needed as u64);

        self.group.add(&reader)?;
        self.inputs[slot] = Some(Input {
            reader,
            block: timing.block,
            offset: cfg.offset,
        });
        self.max_offset = self
            .inputs
            .iter()
            .flatten()
            .map(|i| i.offset)
            .max()
            .unwrap_or(0);
        Ok(short)
    }

    pub fn input(&self, slot: usize) -> Option<&Input> {
        self.inputs.get(slot)?.as_ref()
    }

    pub fn has_inputs(&self) -> bool {
        self.inputs.iter().any(Option::is_some)
    }

    /// The largest `offsetSamples` of the attached inputs, 0 without any.
    pub fn max_offset(&self) -> i64 {
        self.max_offset
    }

    /// RT. The group wait, see [`SyncGroup::wait`].
    pub fn wait(&self, timestamp: u64, timeout_ns: u64) -> Status {
        self.group.wait(timestamp, timeout_ns)
    }
}

fn check_flow_def(def: &str, cfg: &InputConfig) -> Result<()> {
    let v: serde_json::Value = serde_json::from_str(def)
        .map_err(|e| Error::Format(format!("flow {}: definition: {e}", cfg.flow_id)))?;
    let rate = &v["sample_rate"];
    // libmxl's flow parser defaults an absent denominator and an absent
    // channel_count to 1.
    let ok = v["format"] == "urn:x-nmos:format:audio"
        && v["media_type"] == "audio/float32"
        && rate["numerator"] == RATE.numerator
        && rate.get("denominator").is_none_or(|d| *d == 1)
        && v["bit_depth"] == 32
        && v.get("channel_count")
            .map_or(1, |c| c.as_u64().unwrap_or(0))
            == u64::from(cfg.channels);
    if ok {
        Ok(())
    } else {
        Err(Error::Format(format!(
            "flow {}: definition is not 48 kHz float32 audio with {} channels: {def}",
            cfg.flow_id, cfg.channels
        )))
    }
}

/// The outcome of one [`Input::read`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The range was copied out.
    Copy,
    /// The range starts below the end of the last range read; not read.
    BelowLastRead,
    /// The head is below the range end; not read.
    Late,
    /// `MXL_ERR_OUT_OF_RANGE_TOO_LATE`: the range left the readable
    /// history, the producer is ahead of the clock or on another epoch.
    TooLate,
    /// `MXL_ERR_OUT_OF_RANGE_TOO_EARLY`: the head moved backwards between
    /// the check and the read.
    TooEarly,
    /// `MXL_ERR_FLOW_INVALID`: the flow was replaced or removed.
    Invalid,
    /// Any other status, such as `MXL_ERR_UNKNOWN` for an exception caught
    /// inside libmxl.
    Other(Status),
}

/// Outcome counts of one input slot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    pub copied: u64,
    pub below_last_read: u64,
    pub late: u64,
    pub too_late: u64,
    pub too_early: u64,
    pub invalid: u64,
    pub other: u64,
    /// Validity probes made by [`Input::identify`].
    pub probes: u64,
}

/// The state of one input slot. It belongs to the slot, not to a reader,
/// and is carried across generation swaps.
#[derive(Clone, Debug, Default)]
pub struct SlotState {
    /// The exclusive end of the last range read from this slot.
    pub last_end: Option<u64>,
    /// The gain of the fade the engine applies to this input.
    pub fade: f32,
    /// The head seen by the last head check.
    pub head: u64,
    /// Consecutive late blocks with `head` unchanged.
    pub frozen_late_blocks: u32,
    /// Set by an [`Outcome::Invalid`] read or by [`Input::identify`];
    /// cleared by the supervisor.
    pub invalid: bool,
    pub counters: Counters,
}

impl Input {
    pub fn reader(&self) -> &FlowReader {
        &self.reader
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// RT. Reads block `k`, the range `[kB + offset, (k + 1)B + offset)`,
    /// into `dst`, planar with B samples per channel:
    /// 1. a range starting below `state.last_end` is not read;
    /// 2. unless `group` is `MXL_STATUS_OK`, the head from
    ///    `mxlFlowReaderGetRuntimeInfo` is checked and a range ending above
    ///    it is late and not read. `group` is the status of the group wait
    ///    that preceded this block; a block without a wait (catch-up) passes
    ///    any other status;
    /// 3. `mxlFlowReaderGetSamplesNonBlocking((k + 1)B + offset, B)`, both
    ///    fragments copied out at once.
    ///
    /// `dst` is zeroed for every outcome but [`Outcome::Copy`].
    pub fn read(&self, state: &mut SlotState, k: u64, group: Status, dst: &mut [f32]) -> Outcome {
        let outcome = self.read_range(state, k, group, dst);
        let c = &mut state.counters;
        match outcome {
            Outcome::Copy => c.copied += 1,
            Outcome::BelowLastRead => c.below_last_read += 1,
            Outcome::Late => c.late += 1,
            Outcome::TooLate => c.too_late += 1,
            Outcome::TooEarly => c.too_early += 1,
            Outcome::Invalid => {
                c.invalid += 1;
                state.invalid = true;
            }
            Outcome::Other(_) => c.other += 1,
        }
        if outcome != Outcome::Copy {
            dst.fill(0.0);
        }
        outcome
    }

    fn read_range(&self, state: &mut SlotState, k: u64, group: Status, dst: &mut [f32]) -> Outcome {
        let b = self.block;
        let Some(end) = ((k + 1) * b).checked_add_signed(self.offset) else {
            return Outcome::TooLate;
        };
        let Some(start) = end.checked_sub(b) else {
            return Outcome::TooLate;
        };
        if state.last_end.is_some_and(|last| start < last) {
            return Outcome::BelowLastRead;
        }
        if group == MXL_STATUS_OK {
            state.frozen_late_blocks = 0;
        } else {
            let head = match self.reader.head() {
                Ok(head) => head,
                Err(status) => return Outcome::Other(status),
            };
            if end > head {
                if head == state.head {
                    state.frozen_late_blocks = state.frozen_late_blocks.saturating_add(1);
                } else {
                    state.frozen_late_blocks = 1;
                }
                state.head = head;
                return Outcome::Late;
            }
            state.head = head;
            state.frozen_late_blocks = 0;
        }

        let n = b as usize;
        let mut slice = WrappedMultiBufferSlice::default();
        match self.reader.samples(end, n, &mut slice) {
            MXL_STATUS_OK => {}
            MXL_ERR_OUT_OF_RANGE_TOO_LATE => return Outcome::TooLate,
            MXL_ERR_OUT_OF_RANGE_TOO_EARLY => return Outcome::TooEarly,
            MXL_ERR_FLOW_INVALID => return Outcome::Invalid,
            status => return Outcome::Other(status),
        }
        let [f0, f1] = slice.base.fragments;
        let width = size_of::<f32>();
        for (ch, out) in dst.chunks_exact_mut(n).take(slice.count).enumerate() {
            let (a, rest) = out.split_at_mut((f0.size / width).min(n));
            let second = (f1.size / width).min(rest.len());
            // SAFETY: libmxl returned two fragments of `f0.size` and
            // `f1.size` bytes inside channel 0 of the mapped ring, and
            // channel `ch < slice.count` lies `ch * stride` bytes on. At most
            // those sizes are copied, into `out`, which holds `n` samples.
            // Bytes are copied, so no alignment is assumed.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    f0.pointer.cast::<u8>().add(ch * slice.stride),
                    a.as_mut_ptr().cast::<u8>(),
                    size_of_val(a),
                );
                std::ptr::copy_nonoverlapping(
                    f1.pointer.cast::<u8>().add(ch * slice.stride),
                    rest.as_mut_ptr().cast::<u8>(),
                    second * width,
                );
            }
        }
        state.last_end = Some(end);
        Outcome::Copy
    }

    /// RT, failure path. Identifies a failed input after a late block: on
    /// a group status of `MXL_ERR_FLOW_INVALID`, which names no reader, or
    /// when the head has been frozen for [`FROZEN_PROBE_BLOCKS`] late blocks,
    /// probes the flow once with `mxlFlowReaderGetSamplesNonBlocking(head +
    /// 1, 1)`, which runs libmxl's validity check, and marks the slot
    /// invalid on `MXL_ERR_FLOW_INVALID`. A slot already marked is not
    /// probed. Returns true when it marked the slot.
    pub fn identify(&self, state: &mut SlotState, group: Status, outcome: Outcome) -> bool {
        if state.invalid
            || outcome != Outcome::Late
            || (group != MXL_ERR_FLOW_INVALID && state.frozen_late_blocks != FROZEN_PROBE_BLOCKS)
        {
            return false;
        }
        state.counters.probes += 1;
        state.invalid = self.reader.probe(state.head) == MXL_ERR_FLOW_INVALID;
        state.invalid
    }
}

/// The result of one [`PolledFlow::poll`].
#[derive(Debug)]
pub enum Poll {
    /// No reader could be opened.
    Absent(Error),
    /// libmxl reported the flow invalid: every reader reference was
    /// released and opening new ones was attempted.
    Invalid,
    /// The flow's head index.
    Head(u64),
}

/// Supervisor side. The poll reader of one flow that one or more pruned
/// slots read, on the supervisor's poll instance: one libmxl reader
/// reference per holding slot, all to the reader libmxl shares per flow
/// id.
pub struct PolledFlow {
    instance: Arc<Instance>,
    flow_id: String,
    holders: usize,
    /// Empty, or one reference per holder.
    readers: Vec<FlowReader>,
}

impl PolledFlow {
    /// A poll reader with one holder. A flow that does not exist yet is
    /// opened by a later [`poll`](Self::poll).
    pub fn new(instance: &Arc<Instance>, flow_id: &str) -> Result<PolledFlow> {
        crate::validate_id(flow_id)?;
        let mut p = PolledFlow {
            instance: Arc::clone(instance),
            flow_id: flow_id.to_owned(),
            holders: 1,
            readers: Vec::new(),
        };
        // An absent flow is retried by poll.
        let _ = p.open();
        Ok(p)
    }

    pub fn flow_id(&self) -> &str {
        &self.flow_id
    }

    /// Adds a holder.
    pub fn hold(&mut self) {
        self.holders += 1;
        if !self.readers.is_empty() {
            match FlowReader::open(&self.instance, &self.flow_id) {
                Ok(r) => self.readers.push(r),
                // Keeps the invariant; poll opens all references again.
                Err(_) => self.readers.clear(),
            }
        }
    }

    /// Removes a holder and releases its reference. Returns the holders
    /// left; at 0 the poll reader is to be dropped.
    pub fn release(&mut self) -> usize {
        self.holders = self.holders.saturating_sub(1);
        self.readers.truncate(self.holders);
        self.holders
    }

    /// Opens one reference per holder.
    fn open(&mut self) -> Result<()> {
        while self.readers.len() < self.holders {
            match FlowReader::open(&self.instance, &self.flow_id) {
                Ok(r) => self.readers.push(r),
                Err(e) => {
                    self.readers.clear();
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Reads the head and calls `mxlFlowReaderGetSamplesNonBlocking(head +
    /// 1, 1)`, which runs libmxl's validity check. On
    /// `MXL_ERR_FLOW_INVALID` every reference is released, so that libmxl
    /// drops its reader object, and new ones are opened, so that a flow
    /// recreated under the same id is found (upstream mxl-gst sink,
    /// `handleInvalidFlow`).
    pub fn poll(&mut self) -> Poll {
        if self.readers.is_empty()
            && let Err(e) = self.open()
        {
            return Poll::Absent(e);
        }
        let reader = &self.readers[0];
        let head = match reader.head() {
            Ok(head) => head,
            Err(status) => {
                return Poll::Absent(Error::Mxl {
                    call: "mxlFlowReaderGetRuntimeInfo",
                    status,
                });
            }
        };
        if reader.probe(head) == MXL_ERR_FLOW_INVALID {
            self.readers.clear();
            let _ = self.open();
            return Poll::Invalid;
        }
        Poll::Head(head)
    }
}

/// Supervisor side. Whether a pruned slot's polled head is back on time.
#[derive(Clone, Copy, Debug, Default)]
pub struct Recovery {
    on_time_since: Option<u64>,
}

impl Recovery {
    /// True once `head` has been at or above
    /// `mxlTimestampToIndex(now) - margin + B + offset` for [`ON_TIME_NS`].
    /// A head below it, or none, starts the period again.
    pub fn update(
        &mut self,
        api: &MxlApi,
        head: Option<u64>,
        now: u64,
        timing: &Timing,
        offset: i64,
    ) -> bool {
        let threshold = i128::from(timestamp_to_index(api, now)) - i128::from(timing.margin)
            + i128::from(timing.block)
            + i128::from(offset);
        if head.is_none_or(|h| i128::from(h) < threshold) {
            self.on_time_since = None;
            return false;
        }
        let since = *self.on_time_since.get_or_insert(now);
        now.saturating_sub(since) >= ON_TIME_NS
    }
}
